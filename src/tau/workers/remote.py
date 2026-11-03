"""Remote worker — delegates pipeline execution to a remote Tau instance."""

from __future__ import annotations
import logging
from datetime import datetime, timezone

import httpx

from tau.daemon.executor import ExecutionResult

logger = logging.getLogger("tau.workers.remote")


class WorkerUnavailable(Exception):
    """The worker could not be reached, so the run never started there."""


class RemoteWorker:
    """Delegates pipeline execution to a remote Tau daemon.

    For distributed deployments:
    - Each remote worker is another `taud` instance
    - Communication via HTTP (same API the CLI uses)
    - The coordinator dispatches runs to workers based on load
    - Workers report status via heartbeat
    """

    def __init__(
        self,
        worker_id: str,
        host: str,
        api_key: str,
        max_concurrent: int = 4,
        timeout: float = 30.0,
    ):
        self.worker_id = worker_id
        self.host = host.rstrip("/")
        self.api_key = api_key
        self.max_concurrent = max_concurrent
        self._timeout = timeout
        self._status = "idle"
        self._current_load = 0
        self._last_heartbeat: datetime | None = None
        self._client: httpx.AsyncClient | None = None

    async def connect(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=self.host,
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=self._timeout,
        )
        # Verify connectivity
        try:
            resp = await self._client.get("/health")
            resp.raise_for_status()
            self._status = "idle"
            self._last_heartbeat = datetime.now(tz=timezone.utc)
            logger.info(f"[{self.worker_id}] Connected to {self.host}")
        except Exception as e:
            self._status = "offline"
            raise ConnectionError(f"Cannot reach worker at {self.host}: {e}")

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
            self._status = "offline"

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args):
        await self.disconnect()

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
        """Execute a pipeline on the remote worker.

        The worker runs the code through its own local worker (``POST /api/v1/workers/execute``)
        without storing the pipeline, its runs, or its schedule: the coordinator owns those.

        Raises WorkerUnavailable (and marks the worker offline) when the worker cannot be
        reached, so the pool can send the run somewhere else.
        """
        if not self._client:
            raise RuntimeError(f"Worker {self.worker_id} not connected")

        self._current_load += 1
        self._status = "busy"

        # Allow for every retry and its backoff before giving up on the HTTP call.
        attempts = 1 + max(0, retry_count or 0)
        http_timeout = timeout_seconds * attempts + 30 * attempts + 30

        try:
            resp = await self._client.post(
                "/api/v1/workers/execute",
                json={
                    "pipeline_name": pipeline_name,
                    "code": code,
                    "run_id": run_id,
                    "params": params,
                    "timeout_seconds": timeout_seconds,
                    "retry_count": retry_count,
                    "last_successful_run": (
                        last_successful_run.isoformat() if last_successful_run else None
                    ),
                },
                timeout=httpx.Timeout(http_timeout, connect=10.0),
            )
            resp.raise_for_status()
            result = ExecutionResult.from_dict(resp.json())
            logger.info(f"[{self.worker_id}] {pipeline_name}: {result.status}")
            return result

        except (httpx.ConnectError, httpx.ConnectTimeout) as e:
            self._status = "offline"
            raise WorkerUnavailable(f"Worker {self.worker_id} at {self.host} is unreachable: {e}")
        except httpx.HTTPStatusError as e:
            return ExecutionResult.failure(
                f"Remote worker {self.worker_id} error: {e.response.status_code} — {e.response.text}"
            )
        except Exception as e:
            # The request may have reached the worker, so the run is not retried elsewhere.
            return ExecutionResult.failure(
                f"Lost contact with remote worker {self.worker_id} during the run: {type(e).__name__}: {e}"
            )
        finally:
            self._current_load = max(0, self._current_load - 1)
            if self._status != "offline":
                self._status = "idle" if self._current_load == 0 else "busy"

    async def heartbeat(self) -> dict:
        """Check worker health and get current status."""
        if not self._client:
            return {"status": "offline", "worker_id": self.worker_id}

        try:
            resp = await self._client.get("/health")
            resp.raise_for_status()
            data = resp.json()
            self._last_heartbeat = datetime.now(tz=timezone.utc)
            self._status = "idle" if self._current_load == 0 else "busy"
            return {
                "status": self._status,
                "worker_id": self.worker_id,
                "host": self.host,
                "version": data.get("version"),
                "last_heartbeat": self._last_heartbeat.isoformat(),
            }
        except Exception:
            self._status = "offline"
            return {"status": "offline", "worker_id": self.worker_id}

    @property
    def status(self) -> str:
        return self._status

    @property
    def current_load(self) -> int:
        return self._current_load

    @property
    def capacity(self) -> int:
        return max(0, self.max_concurrent - self._current_load)

    def info(self) -> dict:
        return {
            "worker_id": self.worker_id,
            "type": "remote",
            "host": self.host,
            "status": self._status,
            "max_concurrent": self.max_concurrent,
            "current_load": self._current_load,
            "last_heartbeat": self._last_heartbeat.isoformat() if self._last_heartbeat else None,
        }
