"""Run service — business logic for pipeline run operations."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from tau.models.run import PipelineRun
from tau.repositories.run_repo import RunRepository


class RunService:
    def __init__(self, session: AsyncSession):
        self.repo = RunRepository(session)

    async def create_run(self, pipeline_id: str) -> PipelineRun:
        return await self.repo.create(pipeline_id=pipeline_id, status="pending")

    async def get_run(self, run_id: str) -> PipelineRun | None:
        return await self.repo.get_by_id(run_id)

    async def list_runs(self, pipeline_id: str, limit: int = 20) -> list[PipelineRun]:
        return await self.repo.list_by_pipeline(pipeline_id, limit=limit)

    async def get_last_successful(self, pipeline_id: str) -> PipelineRun | None:
        return await self.repo.get_last_successful(pipeline_id)

    async def list_recent_errors(self, limit: int = 20) -> list[PipelineRun]:
        return await self.repo.list_recent_errors(limit=limit)

    async def update_run(self, run: PipelineRun, **kwargs) -> PipelineRun:
        return await self.repo.update(run, **kwargs)
