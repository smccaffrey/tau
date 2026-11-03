"""Pipeline repository — data access layer."""

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from tau.core.database import to_db_time
from tau.models.pipeline import Pipeline


class PipelineRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, **kwargs) -> Pipeline:
        pipeline = Pipeline(**{k: to_db_time(v) for k, v in kwargs.items()})
        self.session.add(pipeline)
        await self.session.commit()
        await self.session.refresh(pipeline)
        return pipeline

    async def get_by_name(self, name: str) -> Pipeline | None:
        result = await self.session.execute(select(Pipeline).where(Pipeline.name == name))
        return result.scalar_one_or_none()

    async def get_by_id(self, id: str) -> Pipeline | None:
        result = await self.session.execute(select(Pipeline).where(Pipeline.id == id))
        return result.scalar_one_or_none()

    async def list_all(self) -> list[Pipeline]:
        result = await self.session.execute(select(Pipeline).order_by(Pipeline.created_at.desc()))
        return list(result.scalars().all())

    async def update(self, pipeline: Pipeline, **kwargs) -> Pipeline:
        for key, value in kwargs.items():
            if value is not None:
                setattr(pipeline, key, to_db_time(value))
        await self.session.commit()
        await self.session.refresh(pipeline)
        return pipeline

    async def set_schedule(
        self, pipeline: Pipeline, cron: str | None, interval_seconds: int | None
    ) -> Pipeline:
        """Replace the schedule. Unlike ``update``, ``None`` clears a field."""
        pipeline.schedule_cron = cron
        pipeline.schedule_interval = interval_seconds
        await self.session.commit()
        await self.session.refresh(pipeline)
        return pipeline

    async def delete(self, pipeline: Pipeline) -> None:
        await self.session.delete(pipeline)
        await self.session.commit()

    async def count(self) -> int:
        result = await self.session.execute(select(func.count(Pipeline.id)))
        return result.scalar_one()
