"""Tau daemon — FastAPI app with built-in scheduler, worker pool, and dashboard."""

import asyncio
import logging
import socket

import httpx
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI

from tau import __version__
from tau.core.config import get_settings
from tau.core.database import init_engine, create_tables
from tau.api.router import api_router
from tau.dashboard.app import router as dashboard_router
from tau.daemon.scheduler import start_scheduler, stop_scheduler, list_jobs
from tau.workers.pool import WorkerPool
from tau.api.workers import set_pool

logger = logging.getLogger("tau")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown."""
    settings = get_settings()

    # Init database
    init_engine(settings.database_url)
    await create_tables()
    logger.info(f"Database initialized: {_redact(settings.database_url)}")

    from tau.core.config import instance_id
    from tau.daemon import leader
    from tau.daemon.runner import recover_interrupted_runs

    interrupted = await recover_interrupted_runs()
    if interrupted:
        logger.warning(f"Marked {interrupted} run(s) interrupted by the last shutdown as failed")
    logger.info(f"Instance id: {instance_id(settings)} (execution mode: {settings.execution_mode})")

    # Init worker pool
    pool = WorkerPool()
    if settings.local_execution:
        pool.add_local(max_concurrent=settings.max_concurrent)
        logger.info(f"Worker pool initialized (local, max_concurrent={settings.max_concurrent})")
    else:
        logger.info("Worker pool initialized without a local worker (TAU_LOCAL_EXECUTION=false)")
    set_pool(pool)

    from tau.api.dag import load_dag
    from tau.core import database

    async with database.async_session_factory() as session:
        edges = await load_dag(session)
    if edges:
        logger.info(f"Loaded {edges} pipeline dependenc{'y' if edges == 1 else 'ies'}")

    # Start scheduler, then bring back schedules and auto-deploy pipeline files
    start_scheduler()
    logger.info("Scheduler started")

    from tau.daemon.bootstrap import autodeploy, restore_schedules

    restored = await restore_schedules()
    if restored:
        logger.info(f"Restored {restored} pipeline schedule(s)")
    if settings.autodeploy_dir:
        deployed = await autodeploy(settings.autodeploy_dir)
        logger.info(f"Auto-deployed {len(deployed)} pipeline(s) from {settings.autodeploy_dir}: {', '.join(deployed)}")

    from tau.daemon.bootstrap import sync_state

    await sync_state()  # workers registered before a restart, or with another coordinator
    if await leader.refresh():
        logger.info("Scheduler leader: this daemon fires scheduled runs")
    else:
        logger.info("Scheduler standby: another daemon holds the scheduler lock")

    tasks = [asyncio.create_task(_heartbeat_loop(pool, settings.heartbeat_interval))]
    if settings.coordinator_url:
        tasks.append(asyncio.create_task(_register_loop(settings)))

    yield

    # Shutdown
    for task in tasks:
        task.cancel()
    await pool.disconnect_remotes()
    stop_scheduler()
    await leader.release()
    logger.info("Tau daemon stopped")


async def _heartbeat_loop(pool: WorkerPool, interval: int) -> None:
    """Periodic housekeeping.

    - health-check remote workers so offline ones come back when they recover
    - keep or take over the scheduler lock
    - pick up schedules, DAG edges and workers changed through another coordinator
    - cancel runs whose cancellation was requested through another coordinator
    """
    from tau.daemon import leader
    from tau.daemon.bootstrap import sync_state
    from tau.daemon.runner import process_cancel_requests

    while True:
        await asyncio.sleep(max(1, interval))
        for step in (pool.broadcast_heartbeat, leader.refresh, sync_state, process_cancel_requests):
            try:
                await step()
            except Exception as e:  # pragma: no cover - keep the loop alive
                logger.warning(f"Heartbeat step {step.__name__} failed: {e}")


async def _register_loop(settings) -> None:
    """Worker mode: register with the coordinator now and on every heartbeat interval.

    Re-registering is idempotent on the coordinator, and it re-adds this worker after a
    coordinator restart (the coordinator keeps its worker list in memory).
    """
    worker_id = settings.worker_id or socket.gethostname()
    advertise = settings.advertise_url or f"http://{socket.gethostname()}:{settings.port}"
    payload = {
        "worker_id": worker_id,
        "host": advertise,
        "api_key": settings.api_key,
        "max_concurrent": settings.max_concurrent,
    }
    headers = {"Authorization": f"Bearer {settings.coordinator_api_key or settings.api_key}"}
    url = settings.coordinator_url.rstrip("/") + "/api/v1/workers/register"
    registered = False
    async with httpx.AsyncClient(timeout=10) as client:
        while True:
            try:
                resp = await client.post(url, json=payload, headers=headers)
                resp.raise_for_status()
                status = resp.json().get("status")
                if not registered or status != "registered":
                    logger.info(f"Registered with coordinator {settings.coordinator_url} as {worker_id} ({status})")
                registered = status == "registered"
            except Exception as e:
                registered = False
                logger.warning(f"Could not register with coordinator {settings.coordinator_url}: {e}")
            await asyncio.sleep(max(1, settings.heartbeat_interval))


def _redact(url: str) -> str:
    """Hide the password in a database URL before logging it."""
    from sqlalchemy.engine import make_url

    try:
        return make_url(url).render_as_string(hide_password=True)
    except Exception:
        return url


def create_app() -> FastAPI:
    app = FastAPI(
        title="Tau Pipelines",
        description="AI-native data pipeline orchestration daemon",
        version=__version__,
        lifespan=lifespan,
    )

    app.include_router(api_router)
    app.include_router(dashboard_router)

    @app.get("/health")
    async def health():
        from tau.api.workers import get_pool
        pool = get_pool()
        from tau.core.config import instance_id
        from tau.daemon import leader

        return {
            "status": "ok",
            "version": __version__,
            "instance_id": instance_id(),
            "scheduler_leader": leader.is_leader(),
            "scheduler_jobs": list_jobs(),
            "workers": pool.info() if pool else None,
        }

    return app


def main():
    """Entry point for `taud` command."""
    import sys

    settings = get_settings()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper()),
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("alembic").setLevel(logging.WARNING)

    host = settings.host
    port = settings.port

    # Parse CLI args (simple, no dep on typer for daemon)
    args = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg == "--port" and i + 1 < len(args):
            port = int(args[i + 1])
        if arg == "--host" and i + 1 < len(args):
            host = args[i + 1]

    if settings.api_key == "tau_dev_key" and host not in ("127.0.0.1", "localhost", "::1"):
        logger.warning(
            "Tau is listening on %s with the default API key. Anyone who can reach this port can "
            "run code on this machine. Set TAU_API_KEY to a long random value.",
            host,
        )

    logger.info(f"Starting Tau daemon v{__version__} on {host}:{port}")
    logger.info(f"Dashboard: http://{host}:{port}/")

    app = create_app()
    uvicorn.run(app, host=host, port=port, log_level=settings.log_level)


if __name__ == "__main__":
    main()
