"""Pipeline API endpoints."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from tau.core.database import get_session
from tau.core.auth import verify_api_key
from tau.dag.resolver import CycleError
from tau.pipeline.loader import find_pipeline_metadata
from tau.repositories.dependency_repo import DependencyRepository
from tau.repositories.pipeline_repo import PipelineRepository
from tau.repositories.run_repo import RunRepository
from tau.schemas.pipeline import (
    PipelineCreate, PipelineResponse,
    PipelineListResponse, PipelineCodeResponse,
)
from tau.daemon.runner import run_pipeline as _run_pipeline, start_background_run
from tau.daemon.scheduler import add_cron_job, add_interval_job, remove_job, validate_cron
from tau.schemas.run import RunResponse

router = APIRouter(prefix="/pipelines", tags=["pipelines"])

DEFAULT_RETRY_COUNT = 3
DEFAULT_TIMEOUT_SECONDS = 1800


def _check_schedule(cron: str | None, interval_seconds: int | None) -> None:
    if cron:
        try:
            validate_cron(cron)
        except ValueError as e:
            raise HTTPException(400, f"Invalid cron expression '{cron}': {e}")
    if interval_seconds is not None and interval_seconds <= 0:
        raise HTTPException(400, "interval_seconds must be positive")


@router.post("", response_model=PipelineResponse)
async def deploy_pipeline(
    data: PipelineCreate,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """Deploy (create or update) a pipeline.

    Fields missing from the request are taken from the code's ``@pipeline(...)`` arguments
    (description, schedule, tags, retry, timeout, depends_on), so ``tau deploy file.py``
    honours the decorator.
    """
    meta = find_pipeline_metadata(data.code, data.name)
    fields = dict(
        code=data.code,
        description=data.description if data.description is not None else meta.get("description"),
        tags=data.tags if data.tags is not None else meta.get("tags"),
        schedule_cron=data.schedule_cron,
        schedule_interval=data.schedule_interval,
        retry_count=data.retry_count if data.retry_count is not None else meta.get("retry"),
        timeout_seconds=data.timeout_seconds if data.timeout_seconds is not None else meta.get("timeout_seconds"),
    )
    if data.schedule_cron is None and data.schedule_interval is None:
        fields["schedule_cron"] = meta.get("schedule")
    depends_on = data.depends_on if data.depends_on is not None else meta.get("depends_on")
    _check_schedule(fields["schedule_cron"], fields["schedule_interval"])

    if depends_on is not None:
        from tau.api.dag import get_dag

        try:
            get_dag().set_upstream(data.name, list(depends_on))
        except CycleError as e:
            raise HTTPException(400, str(e))

    repo = PipelineRepository(session)
    existing = await repo.get_by_name(data.name)

    if existing:
        pipeline = await repo.update(existing, **fields)
        if data.schedule_cron is not None or data.schedule_interval is not None:
            # An explicit schedule replaces the other kind (cron vs interval).
            pipeline = await repo.set_schedule(pipeline, data.schedule_cron, data.schedule_interval)
    else:
        fields["retry_count"] = fields["retry_count"] if fields["retry_count"] is not None else DEFAULT_RETRY_COUNT
        if fields["timeout_seconds"] is None:
            fields["timeout_seconds"] = DEFAULT_TIMEOUT_SECONDS
        pipeline = await repo.create(name=data.name, filename=data.filename, **fields)

    deps = DependencyRepository(session)
    if depends_on is not None:
        await deps.set_upstream(data.name, list(depends_on))

    _sync_schedule(pipeline)
    pipeline.depends_on = await deps.upstream_of(pipeline.name)
    return pipeline


@router.get("", response_model=PipelineListResponse)
async def list_pipelines(
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """List all pipelines."""
    repo = PipelineRepository(session)
    pipelines = await repo.list_all()
    upstream: dict[str, list[str]] = {}
    for up, down in await DependencyRepository(session).list_edges():
        upstream.setdefault(down, []).append(up)
    for p in pipelines:
        p.depends_on = sorted(upstream.get(p.name, []))
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
    pipeline.depends_on = await DependencyRepository(session).upstream_of(name)
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
    """Remove a pipeline, its run history and its dependency edges."""
    from tau.api.dag import get_dag

    repo = PipelineRepository(session)
    pipeline = await repo.get_by_name(name)
    if not pipeline:
        raise HTTPException(404, f"Pipeline '{name}' not found")
    remove_job(f"pipeline:{name}")
    # Runs reference the pipeline by foreign key, which PostgreSQL enforces.
    await RunRepository(session).delete_for_pipeline(pipeline.id)
    await repo.delete(pipeline)
    await DependencyRepository(session).remove_pipeline(name)
    get_dag().remove_pipeline(name)
    return {"status": "removed", "name": name}


@router.post("/{name}/run", response_model=RunResponse)
async def run_pipeline(
    name: str,
    params: dict | None = None,
    wait: bool = True,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """Run a pipeline and wait for the result (``?wait=false`` returns the running run at once).

    The run goes to the least-loaded worker and is retried ``retry_count`` times on failure.
    Follow a background run with ``GET /runs/{name}/{run_id}``; stop it with
    ``POST /runs/{run_id}/cancel``.
    """
    pipeline = await PipelineRepository(session).get_by_name(name)
    if not pipeline:
        raise HTTPException(404, f"Pipeline '{name}' not found")
    if not wait:
        return await start_background_run(session, pipeline, params=params, trigger="manual")
    return await _run_pipeline(session, pipeline, params=params, trigger="manual")


@router.post("/{name}/schedule")
async def set_schedule(
    name: str,
    cron: str | None = None,
    interval_seconds: int | None = None,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """Set, replace or (with neither argument) clear a pipeline's schedule."""
    repo = PipelineRepository(session)
    pipeline = await repo.get_by_name(name)
    if not pipeline:
        raise HTTPException(404, f"Pipeline '{name}' not found")
    if cron and interval_seconds:
        raise HTTPException(400, "Give either cron or interval_seconds, not both")
    _check_schedule(cron, interval_seconds)

    pipeline = await repo.set_schedule(pipeline, cron, interval_seconds)
    _sync_schedule(pipeline)

    return {
        "status": "scheduled" if (cron or interval_seconds) else "unscheduled",
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
