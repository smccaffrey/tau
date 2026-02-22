"""Remote worker — delegates pipeline execution to a remote Tau instance."""

from __future__ import annotations
import logging
from datetime import datetime
from typing import Any

import httpx

from tau.daemon.executor import ExecutionResult

logger = logging.getLogger("tau.workers.remote")


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
            self._last_heartbeat = datetime.utcnow()
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
    ) -> ExecutionResult:
        """Execute a pipeline on the remote worker."""
        if not self._client:
            raise RuntimeError(f"Worker {self.worker_id} not connected")

        self._current_load += 1
        self._status = "busy"

        try:
            # First ensure the pipeline is deployed on the remote
            deploy_resp = await self._client.post(
                "/api/v1/pipelines",
                json={
                    "name": pipeline_name,
                    "code": code,
                    "timeout_seconds": timeout_seconds,
                },
            )
            deploy_resp.raise_for_status()

            # Then trigger the run
            run_resp = await self._client.post(
                f"/api/v1/pipelines/{pipeline_name}/run",
                json=params,
                timeout=timeout_seconds + 10,  # Extra buffer
            )
            run_resp.raise_for_status()
            data = run_resp.json()

            # Map to ExecutionResult
            result = ExecutionResult()
            result.status = data.get("status", "unknown")
            result.result = data.get("result")
            result.error = data.get("error")
            result.trace = data.get("trace")
            result.logs = data.get("logs")
            result.duration_ms = data.get("duration_ms")

            if data.get("started_at"):
                result.started_at = datetime.fromisoformat(data["started_at"])
            if data.get("finished_at"):
                result.finished_at = datetime.fromisoformat(data["finished_at"])

            logger.info(f"[{self.worker_id}] {pipeline_name}: {result.status}")
            return result

        except httpx.HTTPStatusError as e:
            result = ExecutionResult()
            result.status = "failed"
            result.error = f"Remote worker error: {e.response.status_code} — {e.response.text}"
            return result
        except Exception as e:
            result = ExecutionResult()
            result.status = "failed"
            result.error = f"Remote worker unreachable: {e}"
            return result
        finally:
            self._current_load -= 1
            if self._current_load <= 0:
                self._current_load = 0
                self._status = "idle"

    async def heartbeat(self) -> dict:
        """Check worker health and get current status."""
        if not self._client:
            return {"status": "offline", "worker_id": self.worker_id}

        try:
            resp = await self._client.get("/health")
            resp.raise_for_status()
            data = resp.json()
            self._last_heartbeat = datetime.utcnow()
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
