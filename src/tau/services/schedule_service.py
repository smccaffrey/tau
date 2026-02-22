"""Schedule service — business logic for schedule operations."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from tau.models.schedule import Schedule
from tau.repositories.schedule_repo import ScheduleRepository


class ScheduleService:
    def __init__(self, session: AsyncSession):
        self.repo = ScheduleRepository(session)

    async def create_schedule(self, pipeline_id: str, cron_expression: str) -> Schedule:
        return await self.repo.create(
            pipeline_id=pipeline_id,
            cron_expression=cron_expression,
        )

    async def list_by_pipeline(self, pipeline_id: str) -> list[Schedule]:
        return await self.repo.list_by_pipeline(pipeline_id)

    async def list_all(self) -> list[Schedule]:
        return await self.repo.list_all()

    async def delete(self, schedule_id: str) -> bool:
        schedule = await self.repo.get_by_id(schedule_id)
        if not schedule:
            return False
        await self.repo.delete(schedule)
        return True
