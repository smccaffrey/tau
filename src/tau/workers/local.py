"""Local worker — executes pipelines in the daemon process or as local subprocesses."""

from __future__ import annotations
import asyncio
import logging
from datetime import datetime

from tau.daemon import executor
from tau.daemon.executor import execute_pipeline, ExecutionResult

logger = logging.getLogger("tau.workers.local")


class LocalWorker:
    """Runs pipelines in the daemon process.

    - Each attempt runs on its own thread and event loop (``TAU_EXECUTION_MODE=thread``)
    - Concurrency controlled via semaphore; waiting runs count as load
    - No network overhead — direct in-process execution
    """

    def __init__(self, worker_id: str = "local-0", max_concurrent: int = 4):
        self.worker_id = worker_id
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._active_runs: dict[str, asyncio.Task] = {}  # run_id → task
        self._status = "idle"
        self._queued = 0

    @property
    def status(self) -> str:
        if self.current_load:
            return "busy"
        return self._status

    @property
    def current_load(self) -> int:
        """Running plus waiting runs."""
        return len(self._active_runs) + self._queued

    @property
    def queued(self) -> int:
        return self._queued

    @property
    def capacity(self) -> int:
        return max(0, self.max_concurrent - self.current_load)

    async def execute(
        self,
        pipeline_name: str,
        code: str,
        run_id: str,
        params: dict | None = None,
        timeout_seconds: int = 1800,
        last_successful_run: datetime | None = None,
        retry_count: int = 0,
    ) -> ExecutionResult:
        """Execute a pipeline with concurrency control (waits for a free slot)."""
        executor.register_run(run_id)  # cancellable while queued
        self._queued += 1
        try:
            await self._semaphore.acquire()
        except BaseException:
            executor.forget_run(run_id)
            raise
        finally:
            self._queued -= 1
        self._active_runs[run_id] = asyncio.current_task()
        try:
            logger.info(f"[{self.worker_id}] Executing {pipeline_name} (run={run_id})")
            result = await execute_pipeline(
                pipeline_name=pipeline_name,
                code=code,
                run_id=run_id,
                params=params,
                timeout_seconds=timeout_seconds,
                last_successful_run=last_successful_run,
                retry_count=retry_count,
            )
            logger.info(f"[{self.worker_id}] Finished {pipeline_name}: {result.status}")
            return result
        finally:
            self._active_runs.pop(run_id, None)
            self._semaphore.release()

    async def cancel(self, run_id: str) -> bool:
        """Cancel a queued or running pipeline. The run finishes with status ``cancelled``."""
        return executor.cancel_run(run_id)

    def info(self) -> dict:
        return {
            "worker_id": self.worker_id,
            "type": "local",
            "status": self.status,
            "max_concurrent": self.max_concurrent,
            "current_load": self.current_load,
            "queued": self._queued,
            "active_runs": list(self._active_runs.keys()),
        }
