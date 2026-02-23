"""PipelineContext — the runtime context passed to every pipeline function."""

from __future__ import annotations
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Any, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from tau.connectors.base import Connector
    from tau.connections.registry import ConnectionRegistry


@dataclass
class StepTrace:
    name: str
    status: str = "pending"
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: int | None = None
    rows_in: int | None = None
    rows_out: int | None = None
    error: dict | None = None
    metadata: dict = field(default_factory=dict)


class PipelineContext:
    """Runtime context for pipeline execution."""

    def __init__(
        self,
        pipeline_name: str,
        run_id: str,
        params: dict | None = None,
        last_successful_run: datetime | None = None,
        connection_registry: "ConnectionRegistry | None" = None,
    ):
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        self.params = params or {}
        self.last_successful_run = last_successful_run
        self._logs: list[str] = []
        self._steps: list[StepTrace] = []
        self._current_step: StepTrace | None = None
        self._result: dict = {}
        self._connection_registry = connection_registry
        self._active_connections: Dict[str, "Connector"] = {}

    def log(self, message: str) -> None:
        """Log a message (captured in execution trace)."""
        ts = datetime.now(tz=timezone.utc).isoformat()
        self._logs.append(f"[{ts}] {message}")

    def step(self, name: str) -> "StepContext":
        """Start a named step for tracing."""
        return StepContext(self, name)

    async def extract(self, source: str = "", resource: str = "", **kwargs) -> list[dict]:
        """Extract data from a source. Stub for Phase 1."""
        self.log(f"Extract: source={source} resource={resource}")
        return kwargs.get("data", [])

    async def transform(self, data: list[dict], steps: list | None = None, **kwargs) -> list[dict]:
        """Transform data. Stub for Phase 1."""
        self.log(f"Transform: {len(data)} records, {len(steps or [])} steps")
        return data

    async def load(self, target: str = "", data: list[dict] | None = None, **kwargs) -> None:
        """Load data to a target. Stub for Phase 1."""
        self.log(f"Load: target={target}, {len(data or [])} records")

    async def sql(self, query: str, params: dict | None = None, **kwargs) -> list[dict]:
        """Execute SQL in a warehouse. Stub for Phase 1."""
        self.log(f"SQL: {query[:80]}...")
        return []

    async def materialize(self, config, connector=None, dialect: str = "postgres") -> dict:
        """Materialize a table using a strategy (full_refresh, incremental, scd2, etc.)."""
        from tau.materializations.engine import MaterializationEngine
        if connector is None:
            raise ValueError("A SQL-capable connector is required for materialization")
        engine = MaterializationEngine(executor=connector, dialect=dialect)
        result = await engine.materialize(config)
        self.log(f"Materialized {config.target_table}: strategy={config.strategy.value}, rows={result.get('rows', '?')}")
        return result

    async def check(self, *assertions) -> None:
        """Run assertions. Raises on failure."""
        for i, assertion in enumerate(assertions):
            if not assertion:
                raise AssertionError(f"Check #{i+1} failed")
        self.log(f"Checks passed: {len(assertions)} assertions")

    def secret(self, name: str) -> str:
        """Get a secret by name. Stub for Phase 1."""
        import os
        return os.environ.get(name, "")

    async def connection(self, name: str) -> "Connector":
        """Get a named connection from the registry.

        Connections are cached per pipeline run and automatically
        connected on first use. They'll be disconnected when the
        pipeline context is cleaned up.

        Args:
            name: Connection name from tau.toml

        Returns:
            Connected connector instance

        Raises:
            ValueError: If no connection registry available
            KeyError: If connection name not found
        """
        if self._connection_registry is None:
            raise ValueError(
                "No connection registry available. "
                "Make sure connections are configured in tau.toml"
            )

        # Return cached connection if already active
        if name in self._active_connections:
            return self._active_connections[name]

        # Get connector from registry and connect it
        connector = self._connection_registry.get_connection(name)
        await connector.connect()

        # Cache the active connection
        self._active_connections[name] = connector
        self.log(f"Connected to '{name}' ({type(connector).__name__})")

        return connector

    async def cleanup_connections(self) -> None:
        """Disconnect all active connections.

        This should be called at the end of pipeline execution
        to clean up resources.
        """
        for name, connector in self._active_connections.items():
            try:
                await connector.disconnect()
                self.log(f"Disconnected from '{name}'")
            except Exception as e:
                self.log(f"Error disconnecting from '{name}': {e}")

        self._active_connections.clear()

    def get_logs(self) -> str:
        return "\n".join(self._logs)

    def get_trace(self) -> dict:
        return {
            "run_id": self.run_id,
            "pipeline": self.pipeline_name,
            "steps": [
                {
                    "name": s.name,
                    "status": s.status,
                    "started_at": s.started_at.isoformat() if s.started_at else None,
                    "finished_at": s.finished_at.isoformat() if s.finished_at else None,
                    "duration_ms": s.duration_ms,
                    "rows_in": s.rows_in,
                    "rows_out": s.rows_out,
                    "error": s.error,
                    **s.metadata,
                }
                for s in self._steps
            ],
            "log_lines": len(self._logs),
        }


class StepContext:
    """Context manager for pipeline steps."""

    def __init__(self, ctx: PipelineContext, name: str):
        self.ctx = ctx
        self.trace = StepTrace(name=name)

    async def __aenter__(self):
        self.trace.started_at = datetime.now(tz=timezone.utc)
        self.trace.status = "running"
        self.ctx._steps.append(self.trace)
        self.ctx.log(f"Step started: {self.trace.name}")
        return self.trace

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.trace.finished_at = datetime.now(tz=timezone.utc)
        if self.trace.started_at:
            self.trace.duration_ms = int(
                (self.trace.finished_at - self.trace.started_at).total_seconds() * 1000
            )
        if exc_type:
            self.trace.status = "failed"
            self.trace.error = {"type": exc_type.__name__, "message": str(exc_val)}
            self.ctx.log(f"Step failed: {self.trace.name} — {exc_val}")
        else:
            self.trace.status = "success"
            self.ctx.log(f"Step completed: {self.trace.name}")
        return False  # don't suppress exceptions
