"""Schedule API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from tau.core.auth import require_api_key
from tau.core.database import get_session
from tau.schemas.schedule import ScheduleCreate, ScheduleListResponse, ScheduleResponse
from tau.services.pipeline_service import PipelineService
from tau.services.schedule_service import ScheduleService

router = APIRouter(prefix="/schedules", tags=["schedules"])


@router.post("", response_model=ScheduleResponse, status_code=status.HTTP_201_CREATED)
async def create_schedule(
    body: ScheduleCreate,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(require_api_key),
):
    pipeline_service = PipelineService(session)
    pipeline = await pipeline_service.get_by_name(body.pipeline_name)
    if not pipeline:
        raise HTTPException(status_code=404, detail=f"Pipeline '{body.pipeline_name}' not found")

    schedule_service = ScheduleService(session)
    schedule = await schedule_service.create_schedule(
        pipeline_id=pipeline.id,
        cron_expression=body.cron_expression,
    )

    # Register with the scheduler
    from tau.daemon.scheduler import get_scheduler
    scheduler = get_scheduler()
    if scheduler:
        scheduler.add_pipeline_schedule(pipeline.name, schedule.cron_expression, schedule.id)

    return ScheduleResponse(
        id=schedule.id,
        pipeline_id=schedule.pipeline_id,
        cron_expression=schedule.cron_expression,
        is_active=schedule.is_active,
        created_at=schedule.created_at,
        pipeline_name=pipeline.name,
    )


@router.get("", response_model=ScheduleListResponse)
async def list_schedules(
    session: AsyncSession = Depends(get_session),
    _: str = Depends(require_api_key),
):
    schedule_service = ScheduleService(session)
    schedules = await schedule_service.list_all()

    # Enrich with pipeline names
    pipeline_service = PipelineService(session)
    result = []
    for s in schedules:
        pipeline = await pipeline_service.repo.get_by_id(s.pipeline_id)
        result.append(ScheduleResponse(
            id=s.id,
            pipeline_id=s.pipeline_id,
            cron_expression=s.cron_expression,
            is_active=s.is_active,
            created_at=s.created_at,
            pipeline_name=pipeline.name if pipeline else None,
        ))

    return ScheduleListResponse(schedules=result, total=len(result))


@router.delete("/{schedule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_schedule(
    schedule_id: str,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(require_api_key),
):
    schedule_service = ScheduleService(session)
    deleted = await schedule_service.delete(schedule_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Schedule not found")

    from tau.daemon.scheduler import get_scheduler
    scheduler = get_scheduler()
    if scheduler:
        scheduler.remove_schedule(schedule_id)
