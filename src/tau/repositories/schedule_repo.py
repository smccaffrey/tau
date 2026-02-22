"""Schedule repository — database operations for schedules."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from tau.models.schedule import Schedule


class ScheduleRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, **kwargs) -> Schedule:
        schedule = Schedule(**kwargs)
        self.session.add(schedule)
        await self.session.commit()
        await self.session.refresh(schedule)
        return schedule

    async def get_by_id(self, schedule_id: str) -> Schedule | None:
        result = await self.session.execute(
            select(Schedule).where(Schedule.id == schedule_id)
        )
        return result.scalar_one_or_none()

    async def list_by_pipeline(self, pipeline_id: str) -> list[Schedule]:
        result = await self.session.execute(
            select(Schedule)
            .where(Schedule.pipeline_id == pipeline_id)
            .order_by(Schedule.created_at.desc())
        )
        return list(result.scalars().all())

    async def list_all(self) -> list[Schedule]:
        result = await self.session.execute(
            select(Schedule).order_by(Schedule.created_at.desc())
        )
        return list(result.scalars().all())

    async def delete(self, schedule: Schedule) -> None:
        await self.session.delete(schedule)
        await self.session.commit()
