"""Scheduled pipeline runner — called by APScheduler."""

import asyncio
import logging
import uuid
from datetime import datetime

from tau.core.database import async_session_factory
from tau.repositories.pipeline_repo import PipelineRepository
from tau.repositories.run_repo import RunRepository
from tau.daemon.executor import execute_pipeline
from tau.models.run import RunStatus

logger = logging.getLogger("tau.scheduler")


async def scheduled_run(pipeline_name: str):
    """Execute a scheduled pipeline run."""
    logger.info(f"Scheduled run triggered: {pipeline_name}")

    async with async_session_factory() as session:
        pipe_repo = PipelineRepository(session)
        run_repo = RunRepository(session)

        pipeline = await pipe_repo.get_by_name(pipeline_name)
        if not pipeline:
            logger.error(f"Pipeline '{pipeline_name}' not found for scheduled run")
            return

        # Get last successful run
        last_run = await run_repo.get_last_run(pipeline_name)
        last_success = (
            last_run.finished_at
            if last_run and last_run.status == RunStatus.SUCCESS.value
            else None
        )

        run_id = str(uuid.uuid4())

        # Create run record
        run = await run_repo.create(
            id=run_id,
            pipeline_id=pipeline.id,
            pipeline_name=pipeline_name,
            trigger="scheduled",
        )

        # Execute
        result = await execute_pipeline(
            pipeline_name=pipeline_name,
            code=pipeline.code,
            run_id=run_id,
            timeout_seconds=pipeline.timeout_seconds,
            last_successful_run=last_success,
        )

        # Update run record
        await run_repo.update(
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

        # Update pipeline
        await pipe_repo.update(
            pipeline,
            last_run_at=result.finished_at,
            last_run_status=result.status,
        )

        # Fire webhooks
        from tau.daemon.webhooks import notify_run_complete
        await notify_run_complete(pipeline_name, run_id, result.status, result.error)

        if result.status == RunStatus.SUCCESS.value:
            logger.info(f"Scheduled run succeeded: {pipeline_name} ({result.duration_ms}ms)")
        else:
            logger.error(f"Scheduled run failed: {pipeline_name} — {result.error}")
