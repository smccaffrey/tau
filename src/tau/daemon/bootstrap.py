"""Daemon startup tasks: restore schedules and auto-deploy pipeline files."""

from __future__ import annotations

import logging
from pathlib import Path

from tau.core.database import get_session
from tau.pipeline.loader import load_pipeline_from_file
from tau.repositories.dependency_repo import DependencyRepository
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

    from tau.api.dag import get_dag
    from tau.dag.resolver import CycleError

    deployed: list[str] = []
    async for session in get_session():
        repo = PipelineRepository(session)
        deps = DependencyRepository(session)
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
                if meta.get("depends_on"):
                    try:
                        get_dag().set_upstream(meta["name"], list(meta["depends_on"]))
                        await deps.set_upstream(meta["name"], list(meta["depends_on"]))
                    except CycleError as e:
                        logger.error(f"Ignoring depends_on of {meta['name']}: {e}")
                deployed.append(meta["name"])
    return deployed


def _schedule_signature(pipeline) -> tuple | None:
    if pipeline.schedule_cron:
        return ("cron", pipeline.schedule_cron, "UTC")
    if pipeline.schedule_interval:
        return ("interval", pipeline.schedule_interval)
    return None


async def sync_state() -> dict:
    """Bring this daemon's in-memory state in line with the database.

    Run on every heartbeat so coordinators that share a database agree on schedules,
    the DAG and the worker pool, whichever one received the change.
    """
    from tau.api.dag import get_dag, load_dag
    from tau.api.pipelines import _sync_schedule
    from tau.api.workers import get_pool
    from tau.core.config import get_settings
    from tau.daemon.scheduler import job_signature, remove_job, scheduled_job_ids
    from tau.repositories.worker_repo import WorkerRepository

    changed = {"schedules": 0, "dag": False, "workers": 0}
    async for session in get_session():
        # Schedules
        wanted: set[str] = set()
        for pipeline in await PipelineRepository(session).list_all():
            job_id = f"pipeline:{pipeline.name}"
            sig = _schedule_signature(pipeline)
            if sig is not None:
                wanted.add(job_id)
            if job_signature(job_id) != sig:
                try:
                    _sync_schedule(pipeline)
                    changed["schedules"] += 1
                except Exception as e:
                    logger.warning(f"Could not sync schedule for {pipeline.name}: {e}")
        for job_id in scheduled_job_ids():
            if job_id.startswith("pipeline:") and job_id not in wanted:
                remove_job(job_id)
                changed["schedules"] += 1

        # DAG
        stored = sorted(await DependencyRepository(session).list_edges())
        current = sorted(
            (up, name) for name, node in get_dag().nodes.items() for up in node.upstream
        )
        if stored != current:
            await load_dag(session)
            changed["dag"] = True

        # Workers registered with any coordinator
        pool = get_pool()
        if pool is not None:
            settings = get_settings()
            repo = WorkerRepository(session)
            await repo.prune(max(300, 20 * settings.heartbeat_interval))
            before = {w.worker_id for w in pool._remote_workers}
            await pool.sync_remotes(await repo.list_all())
            changed["workers"] = len(before ^ {w.worker_id for w in pool._remote_workers})
    return changed
