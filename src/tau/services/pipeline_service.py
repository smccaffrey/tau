"""Pipeline service — business logic for pipeline operations."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from tau.core.config import settings
from tau.models.pipeline import Pipeline
from tau.pipeline.loader import extract_pipeline_metadata
from tau.repositories.pipeline_repo import PipelineRepository


class PipelineService:
    def __init__(self, session: AsyncSession):
        self.repo = PipelineRepository(session)

    async def deploy(self, name: str, source_code: str, description: str | None = None, tags: list[str] | None = None) -> Pipeline:
        """Deploy a pipeline by saving its source code and registering it."""
        pipelines_dir = settings.get_pipelines_dir()
        file_path = pipelines_dir / f"{name}.py"
        file_path.write_text(source_code)

        # Extract metadata from the source code
        try:
            metas = extract_pipeline_metadata(file_path)
            if metas:
                meta = metas[0]
                description = description or meta.get("description")
                tags = tags or meta.get("tags")
        except Exception:
            pass  # Use provided metadata if extraction fails

        existing = await self.repo.get_by_name(name)
        if existing:
            return await self.repo.update(
                existing,
                description=description,
                tags=tags,
                source_file=str(file_path),
            )

        return await self.repo.create(
            name=name,
            description=description,
            tags=tags,
            source_file=str(file_path),
        )

    async def get_by_name(self, name: str) -> Pipeline | None:
        return await self.repo.get_by_name(name)

    async def list_all(self) -> list[Pipeline]:
        return await self.repo.list_all()

    async def delete(self, name: str) -> bool:
        pipeline = await self.repo.get_by_name(name)
        if not pipeline:
            return False
        # Remove source file
        source_file = Path(pipeline.source_file)
        if source_file.exists():
            source_file.unlink()
        await self.repo.delete(pipeline)
        return True
