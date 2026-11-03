"""Run a deployed pipeline: record the run, dispatch it to a worker, store the result.

Manual runs (API/CLI), scheduled runs and DAG runs all go through ``run_pipeline`` so
they get the same retries, worker dispatch, run records and webhooks.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from tau.core import database
from tau.daemon.executor import ExecutionResult, execute_pipeline
from tau.models.pipeline import Pipeline
from tau.models.run import PipelineRun, RunStatus
from tau.repositories.pipeline_repo import PipelineRepository
from tau.repositories.run_repo import RunRepository

logger = logging.getLogger("tau.runner")


async def _dispatch(pipeline: Pipeline, run_id: str, params: dict | None, last_success) -> ExecutionResult:
    from tau.api.workers import get_pool

    kwargs = dict(
        pipeline_name=pipeline.name,
        code=pipeline.code,
        run_id=run_id,
        params=params,
        timeout_seconds=pipeline.timeout_seconds,
        last_successful_run=last_success,
        retry_count=pipeline.retry_count or 0,
    )
    pool = get_pool()
    if pool is None:  # app created without its lifespan (e.g. embedded in tests)
        return await execute_pipeline(**kwargs)
    return await pool.dispatch(**kwargs)


async def run_pipeline(
    session,
    pipeline: Pipeline,
    params: dict | None = None,
    trigger: str = "manual",
) -> PipelineRun:
    """Execute ``pipeline`` and return its stored run record."""
    pipe_repo = PipelineRepository(session)
    run_repo = RunRepository(session)

    last = await run_repo.get_last_success(pipeline.name)
    last_success = last.finished_at.replace(tzinfo=timezone.utc) if last and last.finished_at else None

    run_id = str(uuid.uuid4())
    run = await run_repo.create(
        id=run_id,
        pipeline_id=pipeline.id,
        pipeline_name=pipeline.name,
        trigger=trigger,
        params=params,
        status=RunStatus.RUNNING.value,
        started_at=datetime.now(tz=timezone.utc),
    )

    try:
        result = await _dispatch(pipeline, run_id, params, last_success)
    except Exception as e:  # never leave a run stuck in "running"
        logger.exception(f"Run {run_id} of {pipeline.name} crashed in the dispatcher")
        result = ExecutionResult.failure(f"Tau internal error: {type(e).__name__}: {e}")

    run = await run_repo.update(
        run,
        status=result.status,
        started_at=result.started_at,
        finished_at=result.finished_at,
        duration_ms=result.duration_ms,
        result=result.result,
        error=result.error,
        trace=result.trace,
        logs=result.logs,
    )
    await pipe_repo.update(
        pipeline,
        last_run_at=result.finished_at,
        last_run_status=result.status,
    )

    from tau.daemon.webhooks import notify_run_complete

    await notify_run_complete(pipeline.name, run_id, result.status, result.error)
    return run


async def run_pipeline_by_name(
    name: str, params: dict | None = None, trigger: str = "manual"
) -> dict:
    """Run a pipeline in its own database session (safe to call concurrently)."""
    async with database.async_session_factory() as session:
        pipeline = await PipelineRepository(session).get_by_name(name)
        if pipeline is None:
            return {"status": "failed", "error": f"Pipeline '{name}' is not deployed"}
        run = await run_pipeline(session, pipeline, params=params, trigger=trigger)
        return {
            "run_id": run.id,
            "status": run.status,
            "duration_ms": run.duration_ms,
            "worker_id": (run.trace or {}).get("worker_id"),
            "error": run.error,
        }


async def run_dag(root: str | None = None, trigger: str = "dag", skip_root_result: dict | None = None):
    """Run the dependency graph (or the part downstream of ``root``) in dependency order.

    If ``skip_root_result`` is given, ``root`` is not run again and that result is used
    instead (a scheduled run of ``root`` has just finished).
    """
    from tau.api.dag import get_dag
    from tau.dag.runner import DAGRunner

    async def run_fn(name: str) -> dict:
        if skip_root_result is not None and name == root:
            return skip_root_result
        return await run_pipeline_by_name(name, trigger=trigger)

    from tau.api.workers import get_pool

    pool = get_pool()
    max_parallel = max(1, pool.total_capacity) if pool else 4
    return await DAGRunner(get_dag(), run_fn, max_parallel=max_parallel).run(root=root)
