"""Scheduled pipeline runner — called by APScheduler."""

import logging

from tau.core import database
from tau.daemon.runner import run_dag, run_pipeline
from tau.models.run import RunStatus
from tau.repositories.pipeline_repo import PipelineRepository

logger = logging.getLogger("tau.scheduler")


async def scheduled_run(pipeline_name: str):
    """Execute a scheduled pipeline run, then its downstream dependents if it succeeded.

    Only the scheduler leader runs it (see ``tau.daemon.leader``).
    """
    from tau.daemon import leader

    if not leader.is_leader():
        logger.debug(f"Skipping scheduled run of {pipeline_name}: another daemon is the scheduler leader")
        return
    logger.info(f"Scheduled run triggered: {pipeline_name}")

    async with database.async_session_factory() as session:
        pipeline = await PipelineRepository(session).get_by_name(pipeline_name)
        if not pipeline:
            logger.error(f"Pipeline '{pipeline_name}' not found for scheduled run")
            return
        run = await run_pipeline(session, pipeline, trigger="scheduled")

    if run.status == RunStatus.SUCCESS.value:
        logger.info(f"Scheduled run succeeded: {pipeline_name} ({run.duration_ms}ms)")
    else:
        logger.error(f"Scheduled run failed: {pipeline_name} — {run.error}")
        return

    from tau.api.dag import get_dag

    if get_dag().get_downstream(pipeline_name):
        result = await run_dag(
            root=pipeline_name,
            trigger="upstream",
            skip_root_result={"status": run.status, "run_id": run.id},
        )
        logger.info(
            f"Downstream of {pipeline_name}: {result.status} "
            f"(failed={result.failed}, skipped={result.skipped})"
        )
