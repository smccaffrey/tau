"""Pipeline run repository."""

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from tau.models.run import PipelineRun


class RunRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, **kwargs) -> PipelineRun:
        run = PipelineRun(**kwargs)
        self.session.add(run)
        await self.session.commit()
        await self.session.refresh(run)
        return run

    async def get_by_id(self, id: str) -> PipelineRun | None:
        result = await self.session.execute(select(PipelineRun).where(PipelineRun.id == id))
        return result.scalar_one_or_none()

    async def get_last_run(self, pipeline_name: str) -> PipelineRun | None:
        result = await self.session.execute(
            select(PipelineRun)
            .where(PipelineRun.pipeline_name == pipeline_name)
            .order_by(PipelineRun.created_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def list_by_pipeline(self, pipeline_name: str, limit: int = 10) -> list[PipelineRun]:
        result = await self.session.execute(
            select(PipelineRun)
            .where(PipelineRun.pipeline_name == pipeline_name)
            .order_by(PipelineRun.created_at.desc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def list_errors(self, limit: int = 20) -> list[PipelineRun]:
        result = await self.session.execute(
            select(PipelineRun)
            .where(PipelineRun.status == "failed")
            .order_by(PipelineRun.created_at.desc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def update(self, run: PipelineRun, **kwargs) -> PipelineRun:
        for key, value in kwargs.items():
            if value is not None:
                setattr(run, key, value)
        await self.session.commit()
        await self.session.refresh(run)
        return run
