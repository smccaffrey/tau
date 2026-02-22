"""Pipeline API endpoints."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from tau.core.database import get_session
from tau.core.auth import verify_api_key
from tau.repositories.pipeline_repo import PipelineRepository
from tau.repositories.run_repo import RunRepository
from tau.schemas.pipeline import (
    PipelineCreate, PipelineUpdate, PipelineResponse,
    PipelineListResponse, PipelineCodeResponse,
)
from tau.daemon.executor import execute_pipeline, ExecutionResult
from tau.daemon.scheduler import add_cron_job, add_interval_job, remove_job
from tau.models.run import RunStatus
from tau.schemas.run import RunResponse

import uuid
from datetime import datetime

router = APIRouter(prefix="/pipelines", tags=["pipelines"])


@router.post("", response_model=PipelineResponse)
async def deploy_pipeline(
    data: PipelineCreate,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """Deploy (create or update) a pipeline."""
    repo = PipelineRepository(session)
    existing = await repo.get_by_name(data.name)

    if existing:
        pipeline = await repo.update(
            existing,
            code=data.code,
            description=data.description,
            tags=data.tags,
            schedule_cron=data.schedule_cron,
            schedule_interval=data.schedule_interval,
            retry_count=data.retry_count,
            timeout_seconds=data.timeout_seconds,
        )
    else:
        pipeline = await repo.create(
            name=data.name,
            code=data.code,
            description=data.description,
            filename=data.filename,
            tags=data.tags,
            schedule_cron=data.schedule_cron,
            schedule_interval=data.schedule_interval,
            retry_count=data.retry_count,
            timeout_seconds=data.timeout_seconds,
        )

    # Update scheduler if schedule is set
    _sync_schedule(pipeline)

    return pipeline


@router.get("", response_model=PipelineListResponse)
async def list_pipelines(
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """List all pipelines."""
    repo = PipelineRepository(session)
    pipelines = await repo.list_all()
    return PipelineListResponse(pipelines=pipelines, total=len(pipelines))


@router.get("/{name}", response_model=PipelineResponse)
async def get_pipeline(
    name: str,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """Get pipeline details."""
    repo = PipelineRepository(session)
    pipeline = await repo.get_by_name(name)
    if not pipeline:
        raise HTTPException(404, f"Pipeline '{name}' not found")
    return pipeline


@router.get("/{name}/code", response_model=PipelineCodeResponse)
async def get_pipeline_code(
    name: str,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """Get pipeline source code."""
    repo = PipelineRepository(session)
    pipeline = await repo.get_by_name(name)
    if not pipeline:
        raise HTTPException(404, f"Pipeline '{name}' not found")
    return PipelineCodeResponse(name=pipeline.name, code=pipeline.code, filename=pipeline.filename)


@router.delete("/{name}")
async def undeploy_pipeline(
    name: str,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """Remove a pipeline."""
    repo = PipelineRepository(session)
    pipeline = await repo.get_by_name(name)
    if not pipeline:
        raise HTTPException(404, f"Pipeline '{name}' not found")
    remove_job(f"pipeline:{name}")
    await repo.delete(pipeline)
    return {"status": "removed", "name": name}


@router.post("/{name}/run", response_model=RunResponse)
async def run_pipeline(
    name: str,
    params: dict | None = None,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """Trigger a pipeline run."""
    pipe_repo = PipelineRepository(session)
    run_repo = RunRepository(session)

    pipeline = await pipe_repo.get_by_name(name)
    if not pipeline:
        raise HTTPException(404, f"Pipeline '{name}' not found")

    # Get last successful run
    last_run = await run_repo.get_last_run(name)
    last_success = last_run.finished_at if last_run and last_run.status == RunStatus.SUCCESS.value else None

    run_id = str(uuid.uuid4())

    # Create run record
    run = await run_repo.create(
        id=run_id,
        pipeline_id=pipeline.id,
        pipeline_name=name,
        trigger="manual",
        params=params,
    )

    # Execute
    result = await execute_pipeline(
        pipeline_name=name,
        code=pipeline.code,
        run_id=run_id,
        params=params,
        timeout_seconds=pipeline.timeout_seconds,
        last_successful_run=last_success,
    )

    # Update run record
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

    # Update pipeline last run info
    await pipe_repo.update(
        pipeline,
        last_run_at=result.finished_at,
        last_run_status=result.status,
    )

    return run


@router.post("/{name}/schedule")
async def set_schedule(
    name: str,
    cron: str | None = None,
    interval_seconds: int | None = None,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """Set or update pipeline schedule."""
    repo = PipelineRepository(session)
    pipeline = await repo.get_by_name(name)
    if not pipeline:
        raise HTTPException(404, f"Pipeline '{name}' not found")

    await repo.update(
        pipeline,
        schedule_cron=cron,
        schedule_interval=interval_seconds,
    )

    _sync_schedule(pipeline)

    return {
        "status": "scheduled",
        "name": name,
        "cron": cron,
        "interval_seconds": interval_seconds,
    }


def _sync_schedule(pipeline):
    """Sync pipeline schedule with the scheduler."""
    from tau.daemon.scheduled_runner import scheduled_run

    job_id = f"pipeline:{pipeline.name}"

    if pipeline.schedule_cron:
        add_cron_job(
            job_id=job_id,
            func=scheduled_run,
            cron_expression=pipeline.schedule_cron,
            kwargs={"pipeline_name": pipeline.name},
        )
    elif pipeline.schedule_interval:
        add_interval_job(
            job_id=job_id,
            func=scheduled_run,
            seconds=pipeline.schedule_interval,
            kwargs={"pipeline_name": pipeline.name},
        )
    else:
        remove_job(job_id)
