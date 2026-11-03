"""Worker pool — manages local and remote workers, dispatches pipeline runs."""

from __future__ import annotations
import asyncio
import itertools
import logging
from datetime import datetime

from tau.workers.local import LocalWorker
from tau.workers.remote import RemoteWorker, WorkerUnavailable
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
        pool.add_local(max_concurrent=2)  # Coordinator also does work (optional)
        pool.add_remote("worker-1", "http://w1:8400", api_key="...")
        pool.add_remote("worker-2", "http://w2:8400", api_key="...")
        # Each run goes to the least-loaded online worker; ties rotate between workers.
        # When every worker is full, the run queues on the least-loaded one.
        # If a remote worker cannot be reached, the run fails over to another worker.
    """

    def __init__(self):
        self._local_workers: list[LocalWorker] = []
        self._remote_workers: list[RemoteWorker] = []
        self._dispatch_lock = asyncio.Lock()
        self._last_used: dict[str, int] = {}
        self._ticket = itertools.count(1)

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
        """Add a remote worker. Registering an existing ``worker_id`` replaces it."""
        self._remote_workers = [w for w in self._remote_workers if w.worker_id != worker_id]
        worker = RemoteWorker(
            worker_id=worker_id,
            host=host,
            api_key=api_key,
            max_concurrent=max_concurrent,
        )
        self._remote_workers.append(worker)
        logger.info(f"Added remote worker: {worker_id} at {host} (max={max_concurrent})")
        return worker

    def get_remote(self, worker_id: str) -> RemoteWorker | None:
        return next((w for w in self._remote_workers if w.worker_id == worker_id), None)

    async def remove_remote(self, worker_id: str) -> bool:
        worker = self.get_remote(worker_id)
        if worker is None:
            return False
        await worker.disconnect()
        self._remote_workers.remove(worker)
        return True

    @property
    def local_worker(self) -> LocalWorker | None:
        return self._local_workers[0] if self._local_workers else None

    @property
    def all_workers(self) -> list[LocalWorker | RemoteWorker]:
        return self._local_workers + self._remote_workers

    @property
    def total_capacity(self) -> int:
        return sum(w.capacity for w in self.all_workers if w.status != "offline")

    @property
    def total_load(self) -> int:
        return sum(w.current_load for w in self.all_workers)

    def _online(self, exclude: set[str]) -> list[LocalWorker | RemoteWorker]:
        return [
            w for w in self.all_workers
            if w.status != "offline" and w.max_concurrent > 0 and w.worker_id not in exclude
        ]

    def _select_worker(
        self, prefer_local: bool = False, exclude: set[str] | None = None
    ) -> LocalWorker | RemoteWorker | None:
        """Select a worker for a run.

        Picks the online worker with the lowest load ratio that still has capacity; ties go
        to whichever was used least recently. With ``prefer_local=True`` a local worker with
        capacity always wins. If every worker is full, the least-loaded one is returned and
        the run waits there for a free slot. Returns None when no worker is online.
        """
        online = self._online(exclude or set())
        if not online:
            return None

        if prefer_local:
            local = [w for w in online if isinstance(w, LocalWorker) and w.capacity > 0]
            if local:
                online = local

        with_capacity = [w for w in online if w.capacity > 0] or online
        return min(
            with_capacity,
            key=lambda w: (w.current_load / w.max_concurrent, self._last_used.get(w.worker_id, 0)),
        )

    async def dispatch(
        self,
        pipeline_name: str,
        code: str,
        run_id: str,
        params: dict | None = None,
        timeout_seconds: int = 1800,
        last_successful_run: datetime | None = None,
        retry_count: int = 0,
        prefer_local: bool = False,
    ) -> ExecutionResult:
        """Dispatch a pipeline run to the best available worker."""
        tried: set[str] = set()
        while True:
            async with self._dispatch_lock:
                worker = self._select_worker(prefer_local=prefer_local, exclude=tried)
                if worker is not None:
                    self._last_used[worker.worker_id] = next(self._ticket)

            if worker is None:
                msg = "No workers available"
                if tried:
                    msg += f" (unreachable: {', '.join(sorted(tried))})"
                logger.warning(f"{msg} for {pipeline_name} (run={run_id})")
                return ExecutionResult.failure(msg)

            worker_type = "local" if isinstance(worker, LocalWorker) else "remote"
            logger.info(f"Dispatching {pipeline_name} (run={run_id}) to {worker.worker_id} ({worker_type})")

            try:
                result = await worker.execute(
                    pipeline_name=pipeline_name,
                    code=code,
                    run_id=run_id,
                    params=params,
                    timeout_seconds=timeout_seconds,
                    last_successful_run=last_successful_run,
                    retry_count=retry_count,
                )
            except WorkerUnavailable as e:
                # The run never reached the worker, so it is safe to send it elsewhere.
                logger.warning(f"{e}; failing over {pipeline_name} (run={run_id})")
                tried.add(worker.worker_id)
                continue

            result.worker_id = worker.worker_id
            if result.trace is not None:
                result.trace["worker_id"] = worker.worker_id
            return result

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
