"""Built-in scheduler — manages cron/interval triggers for pipelines."""

from __future__ import annotations
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger("tau.scheduler")

_scheduler: AsyncIOScheduler | None = None


def get_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler()
    return _scheduler


def start_scheduler():
    scheduler = get_scheduler()
    if not scheduler.running:
        scheduler.start()
        logger.info("Scheduler started")


def stop_scheduler():
    scheduler = get_scheduler()
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")


def add_cron_job(
    job_id: str,
    func,
    cron_expression: str,
    kwargs: dict | None = None,
    timezone: str = "UTC",
):
    """Add a cron-based scheduled job."""
    scheduler = get_scheduler()

    # Remove existing job if present
    try:
        scheduler.remove_job(job_id)
    except Exception:
        pass

    # Parse cron expression (5 fields: min hour day month dow)
    parts = cron_expression.strip().split()
    if len(parts) != 5:
        raise ValueError(f"Invalid cron expression: {cron_expression} (need 5 fields)")

    trigger = CronTrigger(
        minute=parts[0],
        hour=parts[1],
        day=parts[2],
        month=parts[3],
        day_of_week=parts[4],
        timezone=timezone,
    )

    scheduler.add_job(
        func,
        trigger=trigger,
        id=job_id,
        kwargs=kwargs or {},
        replace_existing=True,
        misfire_grace_time=60,
    )
    logger.info(f"Scheduled job '{job_id}' with cron: {cron_expression}")


def add_interval_job(
    job_id: str,
    func,
    seconds: int,
    kwargs: dict | None = None,
):
    """Add an interval-based scheduled job."""
    scheduler = get_scheduler()

    try:
        scheduler.remove_job(job_id)
    except Exception:
        pass

    scheduler.add_job(
        func,
        trigger=IntervalTrigger(seconds=seconds),
        id=job_id,
        kwargs=kwargs or {},
        replace_existing=True,
        misfire_grace_time=60,
    )
    logger.info(f"Scheduled job '{job_id}' every {seconds}s")


def remove_job(job_id: str):
    scheduler = get_scheduler()
    try:
        scheduler.remove_job(job_id)
        logger.info(f"Removed job '{job_id}'")
    except Exception:
        pass


def list_jobs() -> list[dict]:
    scheduler = get_scheduler()
    jobs = []
    for job in scheduler.get_jobs():
        next_run = job.next_run_time
        jobs.append({
            "id": job.id,
            "next_run": next_run.isoformat() if next_run else None,
            "trigger": str(job.trigger),
        })
    return jobs
