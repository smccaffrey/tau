"""Regression tests for behaviour found broken in end-to-end QA.

Each test covers something the README promises that previously did not happen:
retries, decorator metadata on deploy, persisted dependencies, schedule clearing,
worker dispatch and failover, and PostgreSQL-safe timestamps.
"""

import asyncio
from datetime import datetime, timezone

import pytest

from tau.api import dag as dag_api
from tau.api.workers import set_pool
from tau.core import database
from tau.core.database import to_db_time
from tau.daemon.executor import ExecutionResult, execute_pipeline
from tau.daemon.scheduler import get_scheduler
from tau.pipeline.loader import find_pipeline_metadata, parse_pipeline_metadata
from tau.workers.local import LocalWorker
from tau.workers.pool import WorkerPool
from tau.workers.remote import RemoteWorker, WorkerUnavailable

FLAKY_CODE = '''
from tau import pipeline

ATTEMPTS = {"n": 0}

@pipeline(name="flaky", retry=2)
async def flaky(ctx):
    ATTEMPTS["n"] += 1
    ctx.log(f"attempt {ATTEMPTS['n']}")
    if ATTEMPTS["n"] < 2:
        raise RuntimeError("transient")
    return {"attempt": ATTEMPTS["n"]}
'''

# Each exec() gets a fresh module, so count attempts in a file instead.
COUNTING_CODE = '''
from pathlib import Path
from tau import pipeline

@pipeline(name="counting", retry={retry}, timeout="10s", schedule="0 6 * * *",
          tags=["qa"], depends_on=["upstream_x"], description="counts attempts")
async def counting(ctx):
    p = Path({path!r})
    n = int(p.read_text()) + 1 if p.exists() else 1
    p.write_text(str(n))
    ctx.log(f"attempt {{n}}")
    if n < {succeed_on}:
        raise RuntimeError("transient")
    return {{"attempt": n}}
'''


def counting_code(tmp_path, retry=2, succeed_on=2):
    return COUNTING_CODE.format(retry=retry, path=str(tmp_path / "count"), succeed_on=succeed_on)


@pytest.fixture(autouse=True)
def _reset_dag():
    dag_api.get_dag().nodes.clear()
    yield
    dag_api.get_dag().nodes.clear()
    set_pool(None)


# ─── Executor retries ───


class TestRetries:
    @pytest.mark.asyncio
    async def test_retries_until_success_and_keeps_logs(self, tmp_path):
        result = await execute_pipeline("counting", counting_code(tmp_path), "r1", retry_count=2)
        assert result.status == "success"
        assert result.trace["attempts"] == 2
        assert "attempt 1" in result.logs and "attempt 2" in result.logs
        assert "retrying" in result.logs

    @pytest.mark.asyncio
    async def test_gives_up_after_retry_count(self, tmp_path):
        code = counting_code(tmp_path, succeed_on=99)
        result = await execute_pipeline("counting", code, "r1", retry_count=2)
        assert result.status == "failed"
        assert result.trace["attempts"] == 3
        assert (tmp_path / "count").read_text() == "3"

    @pytest.mark.asyncio
    async def test_no_retry_by_default(self, tmp_path):
        code = counting_code(tmp_path, succeed_on=99)
        result = await execute_pipeline("counting", code, "r1")
        assert result.trace["attempts"] == 1


# ─── Decorator metadata ───


class TestDecoratorMetadata:
    def test_parse_reads_literal_arguments(self, tmp_path):
        meta = find_pipeline_metadata(counting_code(tmp_path, retry=5))
        assert meta["name"] == "counting"
        assert meta["retry"] == 5
        assert meta["timeout_seconds"] == 10
        assert meta["schedule"] == "0 6 * * *"
        assert meta["tags"] == ["qa"]
        assert meta["depends_on"] == ["upstream_x"]
        assert meta["description"] == "counts attempts"

    def test_parse_does_not_execute_code(self):
        code = 'raise SystemExit("ran")\nfrom tau import pipeline\n@pipeline(name="x")\nasync def x(ctx): ...\n'
        assert parse_pipeline_metadata(code)[0]["name"] == "x"

    def test_parse_picks_named_pipeline(self):
        code = (
            "from tau import pipeline\n"
            "@pipeline(name='a', retry=1)\nasync def a(ctx): ...\n"
            "@pipeline(name='b', retry=7)\nasync def b(ctx): ...\n"
        )
        assert find_pipeline_metadata(code, "b")["retry"] == 7

    def test_syntax_error_gives_empty_metadata(self):
        assert find_pipeline_metadata("def (") == {}

    @pytest.mark.asyncio
    async def test_deploy_uses_decorator(self, client, tmp_path):
        resp = await client.post("/api/v1/pipelines", json={"name": "counting", "code": counting_code(tmp_path, retry=5)})
        assert resp.status_code == 200
        data = resp.json()
        assert data["retry_count"] == 5
        assert data["timeout_seconds"] == 10
        assert data["schedule_cron"] == "0 6 * * *"
        assert data["tags"] == ["qa"]
        assert data["depends_on"] == ["upstream_x"]
        assert get_scheduler().get_job("pipeline:counting") is not None
        get_scheduler().remove_job("pipeline:counting")

    @pytest.mark.asyncio
    async def test_request_fields_override_decorator(self, client, tmp_path):
        resp = await client.post(
            "/api/v1/pipelines",
            json={"name": "counting", "code": counting_code(tmp_path), "retry_count": 0, "schedule_interval": 60},
        )
        data = resp.json()
        assert data["retry_count"] == 0
        assert data["schedule_cron"] is None
        assert data["schedule_interval"] == 60
        get_scheduler().remove_job("pipeline:counting")

    @pytest.mark.asyncio
    async def test_invalid_cron_is_rejected(self, client):
        code = "from tau import pipeline\n@pipeline(name='bad')\nasync def bad(ctx): ...\n"
        resp = await client.post("/api/v1/pipelines", json={"name": "bad", "code": code, "schedule_cron": "every day"})
        assert resp.status_code == 400
        assert (await client.get("/api/v1/pipelines/bad")).status_code == 404


# ─── Runs through the API ───


class TestRunApi:
    @pytest.mark.asyncio
    async def test_manual_run_retries(self, client, tmp_path):
        await client.post("/api/v1/pipelines", json={"name": "counting", "code": counting_code(tmp_path), "schedule_cron": None})
        run = (await client.post("/api/v1/pipelines/counting/run")).json()
        assert run["status"] == "success"
        assert run["trace"]["attempts"] == 2

    @pytest.mark.asyncio
    async def test_run_goes_through_pool(self, client, tmp_path):
        pool = WorkerPool()
        pool.add_local("local-qa", max_concurrent=1)
        set_pool(pool)
        await client.post("/api/v1/pipelines", json={"name": "counting", "code": counting_code(tmp_path, retry=0, succeed_on=1)})
        run = (await client.post("/api/v1/pipelines/counting/run")).json()
        assert run["status"] == "success"
        assert run["trace"]["worker_id"] == "local-qa"

    @pytest.mark.asyncio
    async def test_last_successful_run_skips_failures(self, client, tmp_path):
        code = '''
from tau import pipeline
@pipeline(name="lsr", retry=0)
async def lsr(ctx):
    if ctx.params.get("fail"):
        raise RuntimeError("no")
    return {"last": ctx.last_successful_run.isoformat() if ctx.last_successful_run else None}
'''
        await client.post("/api/v1/pipelines", json={"name": "lsr", "code": code})
        first = (await client.post("/api/v1/pipelines/lsr/run")).json()
        await client.post("/api/v1/pipelines/lsr/run", json={"fail": True})
        third = (await client.post("/api/v1/pipelines/lsr/run")).json()
        assert third["result"]["last"] is not None
        assert third["result"]["last"].startswith(first["finished_at"][:19])

    @pytest.mark.asyncio
    async def test_errors_include_timeouts(self, client):
        code = '''
import asyncio
from tau import pipeline
@pipeline(name="sleepy", timeout="1s", retry=0)
async def sleepy(ctx):
    await asyncio.sleep(5)
'''
        await client.post("/api/v1/pipelines", json={"name": "sleepy", "code": code})
        run = (await client.post("/api/v1/pipelines/sleepy/run")).json()
        assert run["status"] == "timeout"
        errors = (await client.get("/api/v1/runs/errors")).json()
        assert any(r["pipeline_name"] == "sleepy" for r in errors["runs"])

    @pytest.mark.asyncio
    async def test_undeploy_removes_runs_and_edges(self, client, db_session):
        code = "from tau import pipeline\n@pipeline(name='gone', depends_on=['other'], retry=0)\nasync def gone(ctx): return {}\n"
        await client.post("/api/v1/pipelines", json={"name": "gone", "code": code})
        await client.post("/api/v1/pipelines/gone/run")
        assert (await client.delete("/api/v1/pipelines/gone")).status_code == 200
        assert (await client.get("/api/v1/runs/gone")).json()["total"] == 0
        assert "gone" not in dag_api.get_dag().nodes


# ─── Schedules ───


class TestScheduleChanges:
    @pytest.mark.asyncio
    async def test_disable_clears_schedule(self, client):
        code = "from tau import pipeline\n@pipeline(name='sched')\nasync def sched(ctx): ...\n"
        await client.post("/api/v1/pipelines", json={"name": "sched", "code": code, "schedule_cron": "0 6 * * *"})
        resp = await client.post("/api/v1/pipelines/sched/schedule")
        assert resp.json()["status"] == "unscheduled"
        data = (await client.get("/api/v1/pipelines/sched")).json()
        assert data["schedule_cron"] is None
        assert get_scheduler().get_job("pipeline:sched") is None

    @pytest.mark.asyncio
    async def test_switch_cron_to_interval(self, client):
        code = "from tau import pipeline\n@pipeline(name='sched2')\nasync def sched2(ctx): ...\n"
        await client.post("/api/v1/pipelines", json={"name": "sched2", "code": code, "schedule_cron": "0 6 * * *"})
        await client.post("/api/v1/pipelines/sched2/schedule", params={"interval_seconds": 300})
        data = (await client.get("/api/v1/pipelines/sched2")).json()
        assert data["schedule_cron"] is None and data["schedule_interval"] == 300
        assert "interval" in str(get_scheduler().get_job("pipeline:sched2").trigger)
        get_scheduler().remove_job("pipeline:sched2")


# ─── DAG persistence and runs ───


DAG_CODE = '''
from tau import pipeline
@pipeline(name="{name}", retry=0{deps})
async def {name}(ctx):
    {body}
    return {{"name": "{name}"}}
'''


def dag_code(name, deps=None, fail=False):
    return DAG_CODE.format(
        name=name,
        deps=f", depends_on={deps!r}" if deps else "",
        body='raise RuntimeError("boom")' if fail else "pass",
    )


class TestDagPersistence:
    @pytest.mark.asyncio
    async def test_depends_on_survives_reload(self, client, db_session):
        await client.post("/api/v1/pipelines", json={"name": "a", "code": dag_code("a")})
        await client.post("/api/v1/pipelines", json={"name": "b", "code": dag_code("b", ["a"])})
        await client.post("/api/v1/dag/dependencies", json={"pipeline": "c", "depends_on": ["b"]})

        dag_api.get_dag().nodes.clear()  # simulate a restart
        assert await dag_api.load_dag(db_session) == 2
        assert dag_api.get_dag().parallel_groups() == [["a"], ["b"], ["c"]]

    @pytest.mark.asyncio
    async def test_cycle_rejected_and_graph_unchanged(self, client):
        await client.post("/api/v1/dag/dependencies", json={"pipeline": "b", "depends_on": ["a"]})
        resp = await client.post("/api/v1/dag/dependencies", json={"pipeline": "a", "depends_on": ["b"]})
        assert resp.status_code == 400
        assert dag_api.get_dag().parallel_groups() == [["a"], ["b"]]

    @pytest.mark.asyncio
    async def test_dag_run_skips_downstream_of_failure(self, client):
        for name, deps, fail in [("a", None, False), ("bad", None, True), ("b", ["a"], False), ("c", ["bad"], False)]:
            await client.post("/api/v1/pipelines", json={"name": name, "code": dag_code(name, deps, fail)})
        result = (await client.post("/api/v1/dag/run")).json()
        assert result["status"] == "partial"
        assert result["failed"] == ["bad"]
        assert result["skipped"] == ["c"]
        assert result["results"]["b"]["status"] == "success"

    @pytest.mark.asyncio
    async def test_dag_run_from_root(self, client):
        for name, deps in [("a", None), ("b", ["a"]), ("x", None)]:
            await client.post("/api/v1/pipelines", json={"name": name, "code": dag_code(name, deps)})
        result = (await client.post("/api/v1/dag/run", params={"root": "a"})).json()
        assert result["status"] == "success"
        assert set(result["results"]) == {"a", "b"}

    @pytest.mark.asyncio
    async def test_scheduled_run_triggers_downstream(self, client):
        from tau.daemon.scheduled_runner import scheduled_run

        await client.post("/api/v1/pipelines", json={"name": "a", "code": dag_code("a")})
        await client.post("/api/v1/pipelines", json={"name": "b", "code": dag_code("b", ["a"])})
        await scheduled_run("a")
        runs = (await client.get("/api/v1/runs/b")).json()["runs"]
        assert [r["trigger"] for r in runs] == ["upstream"]


# ─── Worker pool ───


class FakeRemote(RemoteWorker):
    """A remote worker that runs code in-process (or pretends to be down)."""

    def __init__(self, worker_id, down=False, max_concurrent=2):
        super().__init__(worker_id, f"http://{worker_id}:8400", "key", max_concurrent=max_concurrent)
        self.down = down
        self.calls = 0

    async def execute(self, **kwargs):
        self.calls += 1
        if self.down:
            self._status = "offline"
            raise WorkerUnavailable(f"{self.worker_id} down")
        self._current_load += 1
        try:
            return await execute_pipeline(**kwargs)
        finally:
            self._current_load -= 1


SLEEP_CODE = '''
import asyncio
from tau import pipeline
@pipeline(name="s")
async def s(ctx):
    await asyncio.sleep(0.05)
    return {}
'''


class TestPoolDispatch:
    @pytest.mark.asyncio
    async def test_spreads_runs_over_idle_workers(self):
        pool = WorkerPool()
        remotes = [FakeRemote("w1"), FakeRemote("w2")]
        pool._remote_workers.extend(remotes)
        results = [await pool.dispatch("s", SLEEP_CODE, f"r{i}") for i in range(4)]
        assert [r.worker_id for r in results] == ["w1", "w2", "w1", "w2"]

    @pytest.mark.asyncio
    async def test_concurrent_runs_use_least_loaded(self):
        pool = WorkerPool()
        pool.add_local("l0", max_concurrent=1)
        pool._remote_workers.extend([FakeRemote("w1", max_concurrent=1), FakeRemote("w2", max_concurrent=1)])
        results = await asyncio.gather(*[pool.dispatch("s", SLEEP_CODE, f"r{i}") for i in range(3)])
        assert sorted(r.worker_id for r in results) == ["l0", "w1", "w2"]

    @pytest.mark.asyncio
    async def test_fails_over_when_worker_unreachable(self):
        pool = WorkerPool()
        down, up = FakeRemote("w1", down=True), FakeRemote("w2")
        pool._remote_workers.extend([down, up])
        result = await pool.dispatch("s", SLEEP_CODE, "r1")
        assert result.status == "success" and result.worker_id == "w2"
        assert down.status == "offline"
        # The offline worker is skipped until a heartbeat brings it back.
        await pool.dispatch("s", SLEEP_CODE, "r2")
        assert down.calls == 1

    @pytest.mark.asyncio
    async def test_all_workers_unreachable(self):
        pool = WorkerPool()
        pool._remote_workers.append(FakeRemote("w1", down=True))
        result = await pool.dispatch("s", SLEEP_CODE, "r1")
        assert result.status == "failed"
        assert "unreachable: w1" in result.error

    @pytest.mark.asyncio
    async def test_full_pool_queues_instead_of_failing(self):
        pool = WorkerPool()
        pool.add_local("l0", max_concurrent=1)
        results = await asyncio.gather(*[pool.dispatch("s", SLEEP_CODE, f"r{i}") for i in range(3)])
        assert all(r.status == "success" for r in results)

    def test_reregistering_replaces_worker(self):
        pool = WorkerPool()
        pool.add_remote("w1", "http://a:8400", "k")
        pool.add_remote("w1", "http://b:8400", "k")
        assert [w.host for w in pool._remote_workers] == ["http://b:8400"]

    @pytest.mark.asyncio
    async def test_execute_endpoint_runs_without_storing(self, client):
        resp = await client.post(
            "/api/v1/workers/execute",
            json={"pipeline_name": "s", "code": SLEEP_CODE, "run_id": "remote-1", "retry_count": 0},
        )
        assert resp.status_code == 200
        result = ExecutionResult.from_dict(resp.json())
        assert result.status == "success"
        assert (await client.get("/api/v1/pipelines")).json()["total"] == 0

    @pytest.mark.asyncio
    async def test_remote_worker_round_trip(self, app):
        """RemoteWorker talks to a real daemon app over HTTP (in-process transport)."""
        import httpx

        worker = RemoteWorker("w1", "http://worker", "test_key")
        worker._client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://worker",
            headers={"Authorization": "Bearer test_key"},
        )
        result = await worker.execute(pipeline_name="s", code=SLEEP_CODE, run_id="rt-1")
        await worker.disconnect()
        assert result.status == "success"
        assert result.trace["status"] == "success"

    @pytest.mark.asyncio
    async def test_unreachable_remote_raises_worker_unavailable(self):
        worker = RemoteWorker("w1", "http://127.0.0.1:9", "key")
        import httpx

        worker._client = httpx.AsyncClient(base_url=worker.host)
        with pytest.raises(WorkerUnavailable):
            await worker.execute(pipeline_name="s", code=SLEEP_CODE, run_id="x")
        await worker.disconnect()
        assert worker.status == "offline"


class TestLocalWorkerLoad:
    @pytest.mark.asyncio
    async def test_queued_runs_count_as_load(self):
        w = LocalWorker("l0", max_concurrent=1)
        tasks = [asyncio.create_task(w.execute(pipeline_name="s", code=SLEEP_CODE, run_id=f"r{i}")) for i in range(2)]
        await asyncio.sleep(0.01)
        assert w.current_load == 2 and w.queued == 1 and w.capacity == 0
        await asyncio.gather(*tasks)
        assert w.current_load == 0


# ─── Timestamps ───


def test_to_db_time_strips_timezone():
    aware = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert to_db_time(aware) == datetime(2026, 1, 1, 12, 0)
    assert to_db_time(aware).tzinfo is None
    assert to_db_time("x") == "x"


def test_tables_registered():
    assert "pipeline_dependencies" in database.Base.metadata.tables
