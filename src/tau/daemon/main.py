"""Tau daemon — FastAPI app with built-in scheduler, worker pool, and dashboard."""

import logging
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
    logger.info(f"Database initialized: {settings.database_url}")

    # Init worker pool
    pool = WorkerPool()
    pool.add_local(max_concurrent=settings.max_concurrent)
    set_pool(pool)
    logger.info(f"Worker pool initialized (local, max_concurrent={settings.max_concurrent})")

    # Start scheduler
    start_scheduler()
    logger.info("Scheduler started")

    yield

    # Shutdown
    await pool.disconnect_remotes()
    stop_scheduler()
    logger.info("Tau daemon stopped")


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
        return {
            "status": "ok",
            "version": __version__,
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

    host = settings.host
    port = settings.port

    # Parse CLI args (simple, no dep on typer for daemon)
    args = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg == "--port" and i + 1 < len(args):
            port = int(args[i + 1])
        if arg == "--host" and i + 1 < len(args):
            host = args[i + 1]

    logger.info(f"Starting Tau daemon v{__version__} on {host}:{port}")
    logger.info(f"Dashboard: http://{host}:{port}/")

    app = create_app()
    uvicorn.run(app, host=host, port=port, log_level=settings.log_level)


if __name__ == "__main__":
    main()
