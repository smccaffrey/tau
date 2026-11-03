"""Pipeline dependency (DAG edge) repository."""

from sqlalchemy import delete, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from tau.models.dag import PipelineDependency


class DependencyRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def list_edges(self) -> list[tuple[str, str]]:
        result = await self.session.execute(select(PipelineDependency))
        return [(d.upstream_name, d.downstream_name) for d in result.scalars().all()]

    async def upstream_of(self, pipeline: str) -> list[str]:
        result = await self.session.execute(
            select(PipelineDependency.upstream_name).where(
                PipelineDependency.downstream_name == pipeline
            )
        )
        return sorted(result.scalars().all())

    async def set_upstream(self, pipeline: str, depends_on: list[str]) -> None:
        """Replace every upstream edge of ``pipeline``."""
        await self.session.execute(
            delete(PipelineDependency).where(PipelineDependency.downstream_name == pipeline)
        )
        for up in dict.fromkeys(depends_on):
            self.session.add(PipelineDependency(upstream_name=up, downstream_name=pipeline))
        await self.session.commit()

    async def add_edge(self, upstream: str, downstream: str) -> None:
        existing = await self.session.execute(
            select(PipelineDependency).where(
                PipelineDependency.upstream_name == upstream,
                PipelineDependency.downstream_name == downstream,
            )
        )
        if existing.scalar_one_or_none() is None:
            self.session.add(PipelineDependency(upstream_name=upstream, downstream_name=downstream))
            await self.session.commit()

    async def remove_pipeline(self, pipeline: str) -> None:
        """Drop every edge that touches ``pipeline``."""
        await self.session.execute(
            delete(PipelineDependency).where(
                or_(
                    PipelineDependency.upstream_name == pipeline,
                    PipelineDependency.downstream_name == pipeline,
                )
            )
        )
        await self.session.commit()
