"""Run API endpoints."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from tau.core.database import get_session
from tau.core.auth import verify_api_key
from tau.repositories.run_repo import RunRepository
from tau.schemas.run import RunResponse, RunListResponse, RunLogsResponse

router = APIRouter(prefix="/runs", tags=["runs"])


@router.get("", response_model=RunListResponse)
async def list_all_runs(
    limit: int = 20,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """List recent runs across all pipelines."""
    repo = RunRepository(session)
    runs = await repo.list_recent(limit=limit)
    return RunListResponse(runs=runs, total=len(runs))


@router.get("/errors", response_model=RunListResponse)
async def list_errors(
    limit: int = 20,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """List recent failed runs across all pipelines."""
    repo = RunRepository(session)
    runs = await repo.list_errors(limit=limit)
    return RunListResponse(runs=runs, total=len(runs))


@router.get("/{pipeline_name}", response_model=RunListResponse)
async def list_runs(
    pipeline_name: str,
    limit: int = 10,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """List recent runs for a pipeline."""
    repo = RunRepository(session)
    runs = await repo.list_by_pipeline(pipeline_name, limit=limit)
    return RunListResponse(runs=runs, total=len(runs))


@router.get("/{pipeline_name}/last", response_model=RunResponse)
async def get_last_run(
    pipeline_name: str,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """Get the most recent run for a pipeline."""
    repo = RunRepository(session)
    run = await repo.get_last_run(pipeline_name)
    if not run:
        raise HTTPException(404, f"No runs found for pipeline '{pipeline_name}'")
    return run


@router.get("/{pipeline_name}/{run_id}/logs", response_model=RunLogsResponse)
async def get_run_logs(
    pipeline_name: str,
    run_id: str,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """Get logs for a specific run."""
    repo = RunRepository(session)
    run = await repo.get_by_id(run_id)
    if not run:
        raise HTTPException(404, f"Run '{run_id}' not found")
    return RunLogsResponse(id=run.id, pipeline_name=run.pipeline_name, logs=run.logs)
