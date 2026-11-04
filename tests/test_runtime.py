"""Runtime behaviour: off-loop execution, cancellation, background runs, restart recovery,
migrations, and state shared through the database by several coordinators."""

import asyncio
import socket
import time

import pytest
from sqlalchemy import create_engine

from tau.api import dag as dag_api
from tau.api.workers import set_pool
from tau.core import database
from tau.daemon import executor, leader
from tau.daemon.executor import execute_pipeline
from tau.daemon.scheduler import job_signature, remove_job
from tau.models.run import PipelineRun
from tau.repositories.pipeline_repo import PipelineRepository
from tau.repositories.run_repo import RunRepository
from tau.repositories.worker_repo import WorkerRepository
from tau.workers.local import LocalWorker
from tau.workers.pool import WorkerPool

BLOCKING_CODE = '''
import time
from tau import pipeline

@pipeline(name="blocking")
async def blocking(ctx):
    time.sleep({seconds})  # blocks without awaiting
    return {{"slept": {seconds}}}
'''

SLEEPY_CODE = '''
import asyncio
from tau import pipeline

@pipeline(name="sleepy", retry=0)
async def sleepy(ctx):
    ctx.log("sleeping")
    await asyncio.sleep(30)
    return {"done": True}
'''

QUICK_CODE = '''
from tau import pipeline

@pipeline(name="quick", retry=0)
async def quick(ctx):
    return {"ok": 1}
'''


@pytest.fixture(autouse=True)
def _clean():
    dag_api.get_dag().nodes.clear()
    yield
    dag_api.get_dag().nodes.clear()
    set_pool(None)
    remove_job("pipeline:quick")


@pytest.fixture
async def file_db(app, tmp_path):
    """A file database: the in-memory test database shares one connection between
    sessions, so concurrent sessions (a background run and an API call) can roll back
    each other's writes there."""
    database.init_engine(f"sqlite+aiosqlite:///{tmp_path / 'runs.db'}")
    await database.create_tables()
    yield
    await database.engine.dispose()


async def _wait_until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return False


# ─── Execution off the daemon loop ───


class TestThreadExecution:
    @pytest.mark.asyncio
    async def test_blocking_pipeline_does_not_block_the_loop(self):
        ticks = 0

        async def ticker():
            nonlocal ticks
            while True:
                await asyncio.sleep(0.02)
                ticks += 1

        t = asyncio.create_task(ticker())
        result = await execute_pipeline("blocking", BLOCKING_CODE.format(seconds=0.5), "blk-1")
        t.cancel()
        assert result.status == "success"
        assert ticks >= 10  # the loop kept running while the pipeline slept

    @pytest.mark.asyncio
    async def test_inline_mode_still_works(self, monkeypatch):
        monkeypatch.setenv("TAU_EXECUTION_MODE", "inline")
        result = await execute_pipeline("quick", QUICK_CODE, "inl-1")
        assert result.status == "success" and result.result == {"ok": 1}

    @pytest.mark.asyncio
    async def test_blocking_past_timeout_is_a_timeout(self):
        result = await execute_pipeline(
            "blocking", BLOCKING_CODE.format(seconds=1.5), "blk-3", timeout_seconds=1
        )
        assert result.status == "timeout"
        assert "past its 1s timeout" in result.error
        assert result.result == {"slept": 1.5}

    @pytest.mark.asyncio
    async def test_blocked_thread_hits_outer_timeout(self, monkeypatch):
        monkeypatch.setenv("TAU_TIMEOUT_GRACE", "1")
        started = time.monotonic()
        result = await execute_pipeline(
            "blocking", BLOCKING_CODE.format(seconds=4), "blk-2", timeout_seconds=1
        )
        assert result.status == "timeout"
        assert "did not stop" in result.error
        assert time.monotonic() - started < 3.5


# ─── Cancellation ───


class TestCancel:
    @pytest.mark.asyncio
    async def test_cancel_running_pipeline(self):
        task = asyncio.create_task(execute_pipeline("sleepy", SLEEPY_CODE, "c-1"))
        assert await _wait_until(lambda: executor._runs.get("c-1") and executor._runs["c-1"].task)
        assert executor.cancel_run("c-1")
        result = await asyncio.wait_for(task, 5)
        assert result.status == "cancelled"
        assert "CANCELLED" in result.logs
        assert not executor.is_active("c-1")

    @pytest.mark.asyncio
    async def test_cancel_unknown_run(self):
        assert executor.cancel_run("nope") is False

    @pytest.mark.asyncio
    async def test_cancel_while_queued(self):
        worker = LocalWorker(max_concurrent=1)
        first = asyncio.create_task(worker.execute("sleepy", SLEEPY_CODE, "q-1"))
        assert await _wait_until(lambda: worker.current_load == 1 and "q-1" in worker._active_runs)
        second = asyncio.create_task(worker.execute("quick", QUICK_CODE, "q-2"))
        assert await _wait_until(lambda: worker.queued == 1)
        assert await worker.cancel("q-2")  # cancellable while waiting for a slot
        assert await worker.cancel("q-1")
        assert (await asyncio.wait_for(first, 5)).status == "cancelled"
        assert (await asyncio.wait_for(second, 5)).status == "cancelled"

    @pytest.mark.asyncio
    async def test_api_background_run_and_cancel(self, file_db, client):
        await client.post("/api/v1/pipelines", json={"name": "sleepy", "code": SLEEPY_CODE})
        resp = await client.post("/api/v1/pipelines/sleepy/run?wait=false")
        assert resp.status_code == 200
        run = resp.json()
        assert run["status"] == "running"
        assert run["instance_id"]

        assert await _wait_until(lambda: executor._runs.get(run["id"]) and executor._runs[run["id"]].task)
        resp = await client.post(f"/api/v1/runs/{run['id']}/cancel")
        assert resp.json() == {"run_id": run["id"], "status": "cancelling"}

        async def finished():
            r = (await client.get(f"/api/v1/runs/sleepy/{run['id']}")).json()
            return r if r["status"] != "running" else None

        deadline = time.monotonic() + 5
        while (final := await finished()) is None and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        assert final["status"] == "cancelled"
        assert final["cancel_requested"] is True

        resp = await client.post(f"/api/v1/runs/{run['id']}/cancel")
        assert resp.status_code == 409

    @pytest.mark.asyncio
    async def test_background_run_succeeds(self, file_db, client):
        await client.post("/api/v1/pipelines", json={"name": "quick", "code": QUICK_CODE})
        run = (await client.post("/api/v1/pipelines/quick/run?wait=false")).json()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            r = (await client.get(f"/api/v1/runs/quick/{run['id']}")).json()
            if r["status"] != "running":
                break
            await asyncio.sleep(0.05)
        assert r["status"] == "success" and r["result"] == {"ok": 1}

    @pytest.mark.asyncio
    async def test_cancel_stale_run_closes_it(self, client, db_session):
        await client.post("/api/v1/pipelines", json={"name": "quick", "code": QUICK_CODE})
        pipeline = await PipelineRepository(db_session).get_by_name("quick")
        run = await RunRepository(db_session).create(
            id="stale-1", pipeline_id=pipeline.id, pipeline_name="quick",
            status="running", instance_id=socket.gethostname(),
        )
        resp = await client.post(f"/api/v1/runs/{run.id}/cancel")
        assert resp.json()["status"] == "cancelled"

    @pytest.mark.asyncio
    async def test_cancel_requested_elsewhere_is_picked_up(self, file_db, client, db_session):
        from tau.daemon.runner import process_cancel_requests

        await client.post("/api/v1/pipelines", json={"name": "sleepy", "code": SLEEPY_CODE})
        run = (await client.post("/api/v1/pipelines/sleepy/run?wait=false")).json()
        assert await _wait_until(lambda: executor._runs.get(run["id"]) and executor._runs[run["id"]].task)

        # Another coordinator stored the request; this one notices on its heartbeat.
        stored = await RunRepository(db_session).get_by_id(run["id"])
        await RunRepository(db_session).update(stored, cancel_requested=True)
        assert await process_cancel_requests() == 1
        assert await _wait_until(lambda: not executor.is_active(run["id"]))

    @pytest.mark.asyncio
    async def test_worker_cancel_endpoint(self, client):
        resp = await client.post("/api/v1/workers/runs/unknown/cancel")
        assert resp.json() == {"run_id": "unknown", "cancelled": False}


# ─── Restart recovery ───


class TestRecovery:
    @pytest.mark.asyncio
    async def test_only_this_instances_runs_are_failed(self, client, db_session):
        from tau.daemon.runner import recover_interrupted_runs

        await client.post("/api/v1/pipelines", json={"name": "quick", "code": QUICK_CODE})
        pipeline = await PipelineRepository(db_session).get_by_name("quick")
        repo = RunRepository(db_session)
        for run_id, inst in [("mine", socket.gethostname()), ("legacy", None), ("other", "other-host")]:
            await repo.create(id=run_id, pipeline_id=pipeline.id, pipeline_name="quick",
                              status="running", instance_id=inst)

        assert await recover_interrupted_runs() == 2
        db_session.expire_all()
        assert (await repo.get_by_id("mine")).status == "failed"
        assert "Interrupted" in (await repo.get_by_id("legacy")).error
        assert (await repo.get_by_id("other")).status == "running"


# ─── Migrations ───


def _diff(path):
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext

    with create_engine(f"sqlite:///{path}").connect() as conn:
        version = conn.exec_driver_sql("SELECT version_num FROM alembic_version").scalar()
        return version, compare_metadata(MigrationContext.configure(conn), database.Base.metadata)


class TestMigrations:
    @pytest.mark.asyncio
    async def test_fresh_database_matches_models(self, tmp_path):
        path = tmp_path / "fresh.db"
        database.init_engine(f"sqlite+aiosqlite:///{path}")
        await database.create_tables()
        await database.create_tables()  # idempotent
        await database.engine.dispose()
        version, diff = _diff(path)
        assert version == "0002" and diff == []

    @pytest.mark.asyncio
    async def test_legacy_database_is_stamped_and_upgraded(self, tmp_path):
        from sqlalchemy import Column, DateTime, ForeignKey, Integer, JSON, MetaData, String, Table, Text

        # The schema create_all produced before migrations existed.
        path = tmp_path / "legacy.db"
        md = MetaData()
        Table("pipelines", md,
              Column("id", String(36), primary_key=True), Column("name", String(255), nullable=False, unique=True),
              Column("description", Text), Column("code", Text, nullable=False), Column("filename", String(255)),
              Column("tags", JSON), Column("status", String(20), nullable=False),
              Column("schedule_cron", String(100)), Column("schedule_interval", Integer),
              Column("retry_count", Integer, nullable=False), Column("timeout_seconds", Integer, nullable=False),
              Column("created_at", DateTime, nullable=False), Column("updated_at", DateTime, nullable=False),
              Column("last_run_at", DateTime), Column("last_run_status", String(20)))
        Table("pipeline_runs", md,
              Column("id", String(36), primary_key=True),
              Column("pipeline_id", String(36), ForeignKey("pipelines.id"), nullable=False),
              Column("pipeline_name", String(255), nullable=False), Column("status", String(20), nullable=False),
              Column("trigger", String(50), nullable=False), Column("started_at", DateTime),
              Column("finished_at", DateTime), Column("duration_ms", Integer), Column("params", JSON),
              Column("result", JSON), Column("error", Text), Column("trace", JSON), Column("logs", Text),
              Column("created_at", DateTime, nullable=False))
        sync = create_engine(f"sqlite:///{path}")
        md.create_all(sync)
        with sync.begin() as conn:
            conn.exec_driver_sql(
                "INSERT INTO pipelines (id, name, code, status, retry_count, timeout_seconds, created_at, updated_at)"
                " VALUES ('p1', 'old', 'x', 'active', 0, 60, '2026-01-01', '2026-01-01')"
            )
            conn.exec_driver_sql(
                "INSERT INTO pipeline_runs (id, pipeline_id, pipeline_name, status, trigger, created_at)"
                " VALUES ('r1', 'p1', 'old', 'success', 'manual', '2026-01-01')"
            )
        sync.dispose()

        database.init_engine(f"sqlite+aiosqlite:///{path}")
        await database.create_tables()
        async with database.async_session_factory() as s:
            run = await s.get(PipelineRun, "r1")
            assert run.pipeline_name == "old" and run.cancel_requested is False
        await database.engine.dispose()
        version, _ = _diff(path)
        assert version == "0002"


# ─── State shared by coordinators ───


class TestSharedState:
    @pytest.mark.asyncio
    async def test_webhooks_are_persisted(self, client):
        await client.post("/api/v1/webhooks", json={"url": "http://hook.test/a", "events": ["*"]})
        await client.post("/api/v1/webhooks", json={"url": "http://hook.test/a", "events": ["run.failed"]})
        hooks = (await client.get("/api/v1/webhooks")).json()["webhooks"]
        assert hooks == [{"url": "http://hook.test/a", "events": ["run.failed"]}]
        assert (await client.delete("/api/v1/webhooks", params={"url": "http://hook.test/a"})).status_code == 200
        assert (await client.delete("/api/v1/webhooks", params={"url": "http://hook.test/a"})).status_code == 404

    @pytest.mark.asyncio
    async def test_registered_workers_reach_other_coordinators(self, client, db_session):
        from tau.daemon.bootstrap import sync_state

        set_pool(WorkerPool())
        resp = await client.post("/api/v1/workers/register", json={
            "worker_id": "w-db", "host": "http://127.0.0.1:1", "api_key": "k", "max_concurrent": 2,
        })
        assert resp.json()["status"] == "registered_offline"
        assert [w.worker_id for w in await WorkerRepository(db_session).list_all()] == ["w-db"]

        other = WorkerPool()  # a second coordinator sharing the database
        set_pool(other)
        await sync_state()
        assert other.get_remote("w-db") is not None
        assert other.get_remote("w-db").max_concurrent == 2

        await client.delete("/api/v1/workers/w-db")
        await sync_state()
        assert other.get_remote("w-db") is None

    @pytest.mark.asyncio
    async def test_schedules_and_dag_sync_from_database(self, client, db_session):
        from tau.daemon.bootstrap import sync_state
        from tau.repositories.dependency_repo import DependencyRepository

        await client.post("/api/v1/pipelines", json={"name": "quick", "code": QUICK_CODE})
        # Another coordinator scheduled it and added an edge.
        pipeline = await PipelineRepository(db_session).get_by_name("quick")
        await PipelineRepository(db_session).set_schedule(pipeline, None, 3600)
        await DependencyRepository(db_session).set_upstream("quick", ["upstream_y"])

        changed = await sync_state()
        assert changed["schedules"] == 1 and changed["dag"] is True
        assert job_signature("pipeline:quick") == ("interval", 3600)
        assert "upstream_y" in dag_api.get_dag().nodes["quick"].upstream

        assert await sync_state() == {"schedules": 0, "dag": False, "workers": 0}

        await PipelineRepository(db_session).set_schedule(pipeline, None, None)
        await sync_state()
        assert job_signature("pipeline:quick") is None

    @pytest.mark.asyncio
    async def test_sqlite_daemon_is_always_leader(self, app):
        assert await leader.refresh() is True
        assert leader.is_leader()


def test_instance_id_defaults_to_hostname(monkeypatch):
    from tau.core.config import instance_id

    monkeypatch.delenv("TAU_INSTANCE_ID", raising=False)
    assert instance_id() == socket.gethostname()
    monkeypatch.setenv("TAU_INSTANCE_ID", "coord-a")
    assert instance_id() == "coord-a"

