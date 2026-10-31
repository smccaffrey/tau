"""Daemon startup tasks: restore schedules and auto-deploy pipeline files."""

from __future__ import annotations

import logging
from pathlib import Path

from tau.core.database import get_session
from tau.pipeline.loader import load_pipeline_from_file
from tau.repositories.pipeline_repo import PipelineRepository

logger = logging.getLogger("tau.bootstrap")


async def restore_schedules() -> int:
    """Re-register cron/interval jobs for every deployed pipeline.

    The scheduler lives in memory, so without this a daemon restart would silently
    stop all scheduled pipelines.
    """
    from tau.api.pipelines import _sync_schedule

    restored = 0
    async for session in get_session():
        for pipeline in await PipelineRepository(session).list_all():
            if not (pipeline.schedule_cron or pipeline.schedule_interval):
                continue
            try:
                _sync_schedule(pipeline)
                restored += 1
            except Exception as e:  # a bad cron string shouldn't stop the daemon
                logger.warning(f"Could not restore schedule for {pipeline.name}: {e}")
    return restored


async def autodeploy(directory: str | Path) -> list[str]:
    """Deploy (create or update) every ``*.py`` pipeline file in ``directory``.

    Metadata (name, description, schedule, tags, retries, timeout) comes from each
    file's ``@pipeline`` decorator. Use this for GitOps-style setups and containers:
    mount a folder of pipelines and set ``TAU_AUTODEPLOY_DIR``.
    """
    from tau.api.pipelines import _sync_schedule

    path = Path(directory)
    if not path.is_dir():
        logger.warning(f"TAU_AUTODEPLOY_DIR {path} does not exist; skipping auto-deploy")
        return []

    deployed: list[str] = []
    async for session in get_session():
        repo = PipelineRepository(session)
        for file in sorted(path.glob("*.py")):
            if file.name.startswith("_"):
                continue
            try:
                metas = load_pipeline_from_file(file)
            except Exception as e:
                logger.error(f"Skipping {file.name}: failed to load ({e})")
                continue

            code = file.read_text()
            for meta in metas:
                fields = dict(
                    code=code,
                    description=meta.get("description") or None,
                    tags=meta.get("tags") or None,
                    schedule_cron=meta.get("schedule"),
                    retry_count=meta.get("retry", 3),
                    timeout_seconds=meta.get("timeout_seconds", 1800),
                )
                existing = await repo.get_by_name(meta["name"])
                if existing:
                    pipeline = await repo.update(existing, **fields)
                else:
                    pipeline = await repo.create(name=meta["name"], filename=file.name, **fields)
                _sync_schedule(pipeline)
                deployed.append(meta["name"])
    return deployed
