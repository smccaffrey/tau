"""Pipeline repository — data access layer."""

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from tau.models.pipeline import Pipeline


class PipelineRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, **kwargs) -> Pipeline:
        pipeline = Pipeline(**kwargs)
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
                setattr(pipeline, key, value)
        await self.session.commit()
        await self.session.refresh(pipeline)
        return pipeline

    async def delete(self, pipeline: Pipeline) -> None:
        await self.session.delete(pipeline)
        await self.session.commit()

    async def count(self) -> int:
        result = await self.session.execute(select(func.count(Pipeline.id)))
        return result.scalar_one()
