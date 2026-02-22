"""Local worker — executes pipelines in the daemon process or as local subprocesses."""

from __future__ import annotations
import asyncio
import logging
from datetime import datetime
from typing import Any

from tau.daemon.executor import execute_pipeline, ExecutionResult

logger = logging.getLogger("tau.workers.local")


class LocalWorker:
    """Runs pipelines locally in the daemon's event loop.

    For single-machine deployments:
    - Pipelines run as async tasks in the daemon process
    - Concurrency controlled via semaphore
    - No network overhead — direct in-process execution
    """

    def __init__(self, worker_id: str = "local-0", max_concurrent: int = 4):
        self.worker_id = worker_id
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._active_runs: dict[str, asyncio.Task] = {}  # run_id → task
        self._status = "idle"

    @property
    def status(self) -> str:
        if self._active_runs:
            return "busy"
        return self._status

    @property
    def current_load(self) -> int:
        return len(self._active_runs)

    @property
    def capacity(self) -> int:
        return self.max_concurrent - self.current_load

    async def execute(
        self,
        pipeline_name: str,
        code: str,
        run_id: str,
        params: dict | None = None,
        timeout_seconds: int = 1800,
        last_successful_run: datetime | None = None,
    ) -> ExecutionResult:
        """Execute a pipeline with concurrency control."""
        async with self._semaphore:
            task = asyncio.current_task()
            self._active_runs[run_id] = task
            try:
                logger.info(f"[{self.worker_id}] Executing {pipeline_name} (run={run_id})")
                result = await execute_pipeline(
                    pipeline_name=pipeline_name,
                    code=code,
                    run_id=run_id,
                    params=params,
                    timeout_seconds=timeout_seconds,
                    last_successful_run=last_successful_run,
                )
                logger.info(f"[{self.worker_id}] Finished {pipeline_name}: {result.status}")
                return result
            finally:
                self._active_runs.pop(run_id, None)

    async def cancel(self, run_id: str) -> bool:
        """Cancel a running pipeline."""
        task = self._active_runs.get(run_id)
        if task:
            task.cancel()
            self._active_runs.pop(run_id, None)
            return True
        return False

    def info(self) -> dict:
        return {
            "worker_id": self.worker_id,
            "type": "local",
            "status": self.status,
            "max_concurrent": self.max_concurrent,
            "current_load": self.current_load,
            "active_runs": list(self._active_runs.keys()),
        }
