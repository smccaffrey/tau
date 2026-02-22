"""Tests for the scheduler module."""

import pytest
from tau.daemon.scheduler import (
    get_scheduler, start_scheduler, stop_scheduler,
    add_cron_job, add_interval_job, remove_job, list_jobs,
    _scheduler,
)
import tau.daemon.scheduler as sched_module


@pytest.fixture(autouse=True)
def reset_scheduler():
    """Reset the global scheduler between tests."""
    sched_module._scheduler = None
    yield
    if sched_module._scheduler and sched_module._scheduler.running:
        sched_module._scheduler.shutdown(wait=False)
    sched_module._scheduler = None


class TestScheduler:
    @pytest.mark.asyncio
    async def test_start_stop(self):
        start_scheduler()
        scheduler = get_scheduler()
        assert scheduler.running
        # stop_scheduler calls shutdown — just verify it doesn't raise
        stop_scheduler()

    @pytest.mark.asyncio
    async def test_add_cron_job(self):
        start_scheduler()

        async def dummy(**kwargs):
            pass

        add_cron_job("test_cron", dummy, "0 6 * * *")
        jobs = list_jobs()
        assert len(jobs) == 1
        assert jobs[0]["id"] == "test_cron"
        stop_scheduler()

    @pytest.mark.asyncio
    async def test_add_interval_job(self):
        start_scheduler()

        async def dummy(**kwargs):
            pass

        add_interval_job("test_interval", dummy, seconds=60)
        jobs = list_jobs()
        assert len(jobs) == 1
        assert jobs[0]["id"] == "test_interval"
        stop_scheduler()

    @pytest.mark.asyncio
    async def test_remove_job(self):
        start_scheduler()

        async def dummy(**kwargs):
            pass

        add_cron_job("removable", dummy, "0 * * * *")
        assert len(list_jobs()) == 1
        remove_job("removable")
        assert len(list_jobs()) == 0
        stop_scheduler()

    @pytest.mark.asyncio
    async def test_remove_nonexistent_job(self):
        start_scheduler()
        remove_job("nonexistent")  # Should not raise
        stop_scheduler()

    @pytest.mark.asyncio
    async def test_replace_existing(self):
        start_scheduler()

        async def dummy(**kwargs):
            pass

        add_cron_job("replace_test", dummy, "0 6 * * *")
        add_cron_job("replace_test", dummy, "0 12 * * *")
        jobs = list_jobs()
        assert len(jobs) == 1
        stop_scheduler()

    @pytest.mark.asyncio
    async def test_invalid_cron(self):
        start_scheduler()

        async def dummy(**kwargs):
            pass

        with pytest.raises(ValueError, match="Invalid cron"):
            add_cron_job("bad_cron", dummy, "invalid")
        stop_scheduler()

    @pytest.mark.asyncio
    async def test_list_multiple_jobs(self):
        start_scheduler()

        async def dummy(**kwargs):
            pass

        add_cron_job("job_a", dummy, "0 6 * * *")
        add_cron_job("job_b", dummy, "0 12 * * *")
        add_interval_job("job_c", dummy, seconds=300)

        jobs = list_jobs()
        assert len(jobs) == 3
        ids = [j["id"] for j in jobs]
        assert "job_a" in ids
        assert "job_b" in ids
        assert "job_c" in ids
        stop_scheduler()
