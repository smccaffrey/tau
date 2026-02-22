"""Worker pool — manages local and remote workers, dispatches pipeline runs."""

from __future__ import annotations
import asyncio
import logging
from datetime import datetime
from typing import Any

from tau.workers.local import LocalWorker
from tau.workers.remote import RemoteWorker
from tau.daemon.executor import ExecutionResult

logger = logging.getLogger("tau.workers.pool")


class WorkerPool:
    """Manages a pool of local and remote workers.

    Single-machine mode:
        pool = WorkerPool()
        pool.add_local(max_concurrent=4)
        # All pipelines run locally with 4-way concurrency

    Distributed mode:
        pool = WorkerPool()
        pool.add_local(max_concurrent=2)  # Coordinator also does work
        pool.add_remote("worker-1", "http://w1:8400", api_key="...")
        pool.add_remote("worker-2", "http://w2:8400", api_key="...")
        # Pipelines dispatched to least-loaded worker
    """

    def __init__(self):
        self._local_workers: list[LocalWorker] = []
        self._remote_workers: list[RemoteWorker] = []
        self._dispatch_lock = asyncio.Lock()

    def add_local(self, worker_id: str = "local-0", max_concurrent: int = 4) -> LocalWorker:
        """Add a local worker."""
        worker = LocalWorker(worker_id=worker_id, max_concurrent=max_concurrent)
        self._local_workers.append(worker)
        logger.info(f"Added local worker: {worker_id} (max={max_concurrent})")
        return worker

    def add_remote(
        self,
        worker_id: str,
        host: str,
        api_key: str,
        max_concurrent: int = 4,
    ) -> RemoteWorker:
        """Add a remote worker."""
        worker = RemoteWorker(
            worker_id=worker_id,
            host=host,
            api_key=api_key,
            max_concurrent=max_concurrent,
        )
        self._remote_workers.append(worker)
        logger.info(f"Added remote worker: {worker_id} at {host} (max={max_concurrent})")
        return worker

    @property
    def all_workers(self) -> list[LocalWorker | RemoteWorker]:
        return self._local_workers + self._remote_workers

    @property
    def total_capacity(self) -> int:
        return sum(w.capacity for w in self.all_workers if w.status != "offline")

    @property
    def total_load(self) -> int:
        return sum(w.current_load for w in self.all_workers)

    def _select_worker(self, prefer_local: bool = True) -> LocalWorker | RemoteWorker | None:
        """Select the best worker for a run (least-loaded with capacity).

        With prefer_local=True: tries local workers first, falls back to remote
        if no local capacity. With prefer_local=False: picks globally least-loaded.
        """
        local_candidates = [
            (w, w.current_load) for w in self._local_workers if w.capacity > 0
        ]
        remote_candidates = [
            (w, w.current_load) for w in self._remote_workers
            if w.status != "offline" and w.capacity > 0
        ]

        if prefer_local and local_candidates:
            # Use local if any have capacity
            local_candidates.sort(key=lambda x: x[1])
            return local_candidates[0][0]

        # Otherwise pick from all available (least loaded)
        all_candidates = local_candidates + remote_candidates
        if not all_candidates:
            return None

        all_candidates.sort(key=lambda x: x[1])
        return all_candidates[0][0]

    async def dispatch(
        self,
        pipeline_name: str,
        code: str,
        run_id: str,
        params: dict | None = None,
        timeout_seconds: int = 1800,
        last_successful_run: datetime | None = None,
        prefer_local: bool = True,
    ) -> ExecutionResult:
        """Dispatch a pipeline run to the best available worker."""
        async with self._dispatch_lock:
            worker = self._select_worker(prefer_local=prefer_local)

        if worker is None:
            # No capacity — queue or reject
            result = ExecutionResult()
            result.status = "failed"
            result.error = "No workers available — all at capacity"
            logger.warning(f"No capacity for {pipeline_name} (run={run_id})")
            return result

        worker_type = "local" if isinstance(worker, LocalWorker) else "remote"
        logger.info(
            f"Dispatching {pipeline_name} to {worker.worker_id} ({worker_type})"
        )

        return await worker.execute(
            pipeline_name=pipeline_name,
            code=code,
            run_id=run_id,
            params=params,
            timeout_seconds=timeout_seconds,
            last_successful_run=last_successful_run,
        )

    async def broadcast_heartbeat(self) -> list[dict]:
        """Check health of all remote workers."""
        results = []
        for w in self._remote_workers:
            result = await w.heartbeat()
            results.append(result)
        for w in self._local_workers:
            results.append(w.info())
        return results

    async def connect_remotes(self) -> None:
        """Connect all remote workers."""
        for w in self._remote_workers:
            try:
                await w.connect()
            except Exception as e:
                logger.error(f"Failed to connect to {w.worker_id}: {e}")

    async def disconnect_remotes(self) -> None:
        """Disconnect all remote workers."""
        for w in self._remote_workers:
            await w.disconnect()

    def info(self) -> dict:
        return {
            "workers": [w.info() for w in self.all_workers],
            "total_capacity": self.total_capacity,
            "total_load": self.total_load,
            "local_count": len(self._local_workers),
            "remote_count": len(self._remote_workers),
        }
