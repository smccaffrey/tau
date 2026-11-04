"""Run a deployed pipeline: record the run, dispatch it to a worker, store the result.

Manual runs (API/CLI), scheduled runs and DAG runs all go through ``run_pipeline`` so
they get the same retries, worker dispatch, run records and webhooks.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone

from tau.core import database
from tau.core.config import instance_id
from tau.daemon import executor
from tau.daemon.executor import ExecutionResult, execute_pipeline
from tau.models.pipeline import Pipeline
from tau.models.run import PipelineRun, RunStatus
from tau.repositories.pipeline_repo import PipelineRepository
from tau.repositories.run_repo import RunRepository

logger = logging.getLogger("tau.runner")


async def _dispatch(
    pipeline: Pipeline, run_id: str, params: dict | None, last_success, on_assigned=None
) -> ExecutionResult:
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
    return await pool.dispatch(**kwargs, on_assigned=on_assigned)


async def create_run(
    session, pipeline: Pipeline, params: dict | None = None, trigger: str = "manual"
) -> PipelineRun:
    """Record a new run as ``running`` and make it cancellable. Execute it with ``execute_run``."""
    run_id = str(uuid.uuid4())
    executor.register_run(run_id)
    return await RunRepository(session).create(
        id=run_id,
        pipeline_id=pipeline.id,
        pipeline_name=pipeline.name,
        trigger=trigger,
        params=params,
        status=RunStatus.RUNNING.value,
        started_at=datetime.now(tz=timezone.utc),
        instance_id=instance_id(),
    )


async def execute_run(session, pipeline: Pipeline, run: PipelineRun) -> PipelineRun:
    """Dispatch a run created by ``create_run`` and store its result."""
    pipe_repo = PipelineRepository(session)
    run_repo = RunRepository(session)
    run_id = run.id

    try:
        last = await run_repo.get_last_success(pipeline.name)
        last_success = last.finished_at.replace(tzinfo=timezone.utc) if last and last.finished_at else None

        async def on_assigned(worker_id: str) -> None:
            await run_repo.update(run, worker_id=worker_id)

        result = await _dispatch(pipeline, run_id, run.params, last_success, on_assigned)
    except Exception as e:  # never leave a run stuck in "running"
        logger.exception(f"Run {run_id} of {pipeline.name} crashed in the dispatcher")
        result = ExecutionResult.failure(f"Tau internal error: {type(e).__name__}: {e}")
    finally:
        executor.forget_run(run_id)

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
        worker_id=result.worker_id,
    )
    await pipe_repo.update(
        pipeline,
        last_run_at=result.finished_at,
        last_run_status=result.status,
    )

    from tau.daemon.webhooks import notify_run_complete

    await notify_run_complete(pipeline.name, run_id, result.status, result.error)
    return run


async def run_pipeline(
    session,
    pipeline: Pipeline,
    params: dict | None = None,
    trigger: str = "manual",
) -> PipelineRun:
    """Execute ``pipeline`` and return its stored run record."""
    run = await create_run(session, pipeline, params=params, trigger=trigger)
    return await execute_run(session, pipeline, run)


_background: set[asyncio.Task] = set()


async def start_background_run(
    session, pipeline: Pipeline, params: dict | None = None, trigger: str = "manual"
) -> PipelineRun:
    """Record a run and execute it in the background. Returns the ``running`` record."""
    run = await create_run(session, pipeline, params=params, trigger=trigger)
    run_id, pipeline_name = run.id, pipeline.name

    async def _go():
        try:
            async with database.async_session_factory() as s:
                p = await PipelineRepository(s).get_by_name(pipeline_name)
                r = await RunRepository(s).get_by_id(run_id)
                if p is None or r is None:
                    return
                await execute_run(s, p, r)
        except Exception:
            logger.exception(f"Background run {run_id} of {pipeline_name} failed to record its result")
        finally:
            executor.forget_run(run_id)

    task = asyncio.create_task(_go(), name=f"tau-run-{run_id[:8]}")
    _background.add(task)
    task.add_done_callback(_background.discard)
    return run


async def cancel_run(session, run: PipelineRun) -> str:
    """Cancel a run. Returns ``cancelling`` (it will stop shortly) or ``cancelled``.

    The request is stored on the run, so the daemon that owns it picks it up on its next
    heartbeat even if this request reached a different daemon.
    """
    from tau.api.workers import get_pool

    run_repo = RunRepository(session)
    await run_repo.update(run, cancel_requested=True)

    pool = get_pool()
    if await (pool.cancel(run.id) if pool else _cancel_local(run.id)):
        return "cancelling"
    if run.instance_id in (None, instance_id()):
        # Owned by this daemon but not executing here: a stale record. Close it now.
        await run_repo.update(
            run,
            status=RunStatus.CANCELLED.value,
            finished_at=datetime.now(tz=timezone.utc),
            error="Run cancelled (it was not executing)",
        )
        return "cancelled"
    return "cancelling"


async def _cancel_local(run_id: str) -> bool:
    return executor.cancel_run(run_id)


async def process_cancel_requests() -> int:
    """Cancel this daemon's runs whose cancellation was requested through another daemon."""
    from tau.api.workers import get_pool

    pool = get_pool()
    async with database.async_session_factory() as session:
        runs = await RunRepository(session).list_cancel_requested(instance_id())
    count = 0
    for run in runs:
        if executor.cancel_requested(run.id):
            continue  # already being cancelled
        if await (pool.cancel(run.id) if pool else _cancel_local(run.id)):
            count += 1
    return count


async def recover_interrupted_runs() -> int:
    """Mark runs this daemon left in ``running`` (it stopped mid-run) as failed."""
    async with database.async_session_factory() as session:
        return await RunRepository(session).fail_interrupted(
            instance_id(),
            "Interrupted: the Tau daemon stopped while this run was in progress",
        )


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
