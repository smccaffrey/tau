"""Tests for worker pool, local workers, and remote workers."""

import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime

from tau.workers.local import LocalWorker
from tau.workers.remote import RemoteWorker
from tau.workers.pool import WorkerPool
from tau.daemon.executor import ExecutionResult


# ─── Local Worker ───

class TestLocalWorker:
    @pytest.mark.asyncio
    async def test_init(self):
        w = LocalWorker(worker_id="test-0", max_concurrent=2)
        assert w.worker_id == "test-0"
        assert w.max_concurrent == 2
        assert w.status == "idle"
        assert w.current_load == 0

    @pytest.mark.asyncio
    async def test_execute_simple(self):
        w = LocalWorker(max_concurrent=2)
        code = '''
from tau import pipeline, PipelineContext
@pipeline(name="test")
async def test(ctx: PipelineContext):
    ctx.log("hello")
    return {"ok": True}
'''
        result = await w.execute("test", code, "run-1")
        assert result.status == "success"
        assert result.result == {"ok": True}

    @pytest.mark.asyncio
    async def test_execute_failure(self):
        w = LocalWorker()
        code = '''
from tau import pipeline, PipelineContext
@pipeline(name="fail")
async def fail(ctx: PipelineContext):
    raise ValueError("boom")
'''
        result = await w.execute("fail", code, "run-2")
        assert result.status == "failed"
        assert "boom" in result.error

    @pytest.mark.asyncio
    async def test_concurrency(self):
        w = LocalWorker(max_concurrent=2)
        max_concurrent = 0
        current = 0

        code = '''
from tau import pipeline, PipelineContext
import asyncio
@pipeline(name="slow")
async def slow(ctx: PipelineContext):
    await asyncio.sleep(0.05)
    return {"ok": True}
'''
        # Run 4 tasks with max_concurrent=2
        tasks = [
            w.execute("slow", code, f"run-{i}")
            for i in range(4)
        ]
        results = await asyncio.gather(*tasks)
        assert all(r.status == "success" for r in results)

    @pytest.mark.asyncio
    async def test_info(self):
        w = LocalWorker(worker_id="w0", max_concurrent=4)
        info = w.info()
        assert info["worker_id"] == "w0"
        assert info["type"] == "local"
        assert info["max_concurrent"] == 4
        assert info["current_load"] == 0

    @pytest.mark.asyncio
    async def test_capacity(self):
        w = LocalWorker(max_concurrent=4)
        assert w.capacity == 4


# ─── Remote Worker ───

class TestRemoteWorker:
    def test_init(self):
        w = RemoteWorker(worker_id="r1", host="http://w1:8400", api_key="key")
        assert w.worker_id == "r1"
        assert w.host == "http://w1:8400"
        assert w.status == "idle"

    def test_info(self):
        w = RemoteWorker(worker_id="r1", host="http://w1:8400", api_key="key", max_concurrent=8)
        info = w.info()
        assert info["type"] == "remote"
        assert info["host"] == "http://w1:8400"
        assert info["max_concurrent"] == 8

    def test_capacity(self):
        w = RemoteWorker(worker_id="r1", host="http://w1:8400", api_key="key", max_concurrent=4)
        assert w.capacity == 4

    @pytest.mark.asyncio
    async def test_connect_failure(self):
        w = RemoteWorker(worker_id="r1", host="http://nonexistent:9999", api_key="key", timeout=1.0)
        with pytest.raises(ConnectionError):
            await w.connect()

    @pytest.mark.asyncio
    async def test_execute_without_connect_raises(self):
        w = RemoteWorker(worker_id="r1", host="http://w1:8400", api_key="key")
        with pytest.raises(RuntimeError, match="not connected"):
            await w.execute("test", "code", "run-1")

    @pytest.mark.asyncio
    async def test_heartbeat_offline(self):
        w = RemoteWorker(worker_id="r1", host="http://w1:8400", api_key="key")
        result = await w.heartbeat()
        assert result["status"] == "offline"


# ─── Worker Pool ───

class TestWorkerPool:
    def test_empty_pool(self):
        pool = WorkerPool()
        assert pool.total_capacity == 0
        assert pool.total_load == 0
        assert pool.all_workers == []

    def test_add_local(self):
        pool = WorkerPool()
        w = pool.add_local(worker_id="l0", max_concurrent=4)
        assert len(pool._local_workers) == 1
        assert pool.total_capacity == 4

    def test_add_remote(self):
        pool = WorkerPool()
        w = pool.add_remote("r1", "http://w1:8400", "key", max_concurrent=8)
        assert len(pool._remote_workers) == 1
        # Remote starts as idle so has capacity
        assert w.capacity == 8

    def test_multiple_workers(self):
        pool = WorkerPool()
        pool.add_local("l0", max_concurrent=2)
        pool.add_local("l1", max_concurrent=2)
        pool.add_remote("r0", "http://w1:8400", "key", max_concurrent=4)
        assert len(pool.all_workers) == 3
        assert pool.total_capacity == 8

    @pytest.mark.asyncio
    async def test_dispatch_to_local(self):
        pool = WorkerPool()
        pool.add_local(max_concurrent=4)

        code = '''
from tau import pipeline, PipelineContext
@pipeline(name="test")
async def test(ctx: PipelineContext):
    return {"ok": True}
'''
        result = await pool.dispatch("test", code, "run-1")
        assert result.status == "success"

    @pytest.mark.asyncio
    async def test_dispatch_no_workers(self):
        pool = WorkerPool()
        result = await pool.dispatch("test", "code", "run-1")
        assert result.status == "failed"
        assert "No workers available" in result.error

    @pytest.mark.asyncio
    async def test_dispatch_prefers_local(self):
        pool = WorkerPool()
        local = pool.add_local("l0", max_concurrent=2)
        remote = pool.add_remote("r0", "http://w1:8400", "key", max_concurrent=4)

        # _select_worker should prefer local
        selected = pool._select_worker(prefer_local=True)
        assert isinstance(selected, LocalWorker)

    def test_info(self):
        pool = WorkerPool()
        pool.add_local("l0", max_concurrent=4)
        info = pool.info()
        assert info["local_count"] == 1
        assert info["remote_count"] == 0
        assert info["total_capacity"] == 4
        assert len(info["workers"]) == 1

    @pytest.mark.asyncio
    async def test_broadcast_heartbeat(self):
        pool = WorkerPool()
        pool.add_local("l0")
        results = await pool.broadcast_heartbeat()
        assert len(results) == 1
        assert results[0]["worker_id"] == "l0"

    @pytest.mark.asyncio
    async def test_dispatch_multiple_concurrent(self):
        pool = WorkerPool()
        pool.add_local(max_concurrent=4)

        code = '''
from tau import pipeline, PipelineContext
import asyncio
@pipeline(name="test")
async def test(ctx: PipelineContext):
    await asyncio.sleep(0.02)
    return {"ok": True}
'''
        tasks = [pool.dispatch("test", code, f"run-{i}") for i in range(4)]
        results = await asyncio.gather(*tasks)
        assert all(r.status == "success" for r in results)
