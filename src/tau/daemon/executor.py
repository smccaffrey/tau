"""Pipeline executor — runs pipeline code and captures traces."""

from __future__ import annotations
import asyncio
import importlib.util
import traceback
from datetime import datetime, timezone

from tau.pipeline.context import PipelineContext
from tau.models.run import RunStatus
from tau.connections.registry import ConnectionRegistry
from tau.core.config import get_settings


class ExecutionResult:
    def __init__(self):
        self.status: str = RunStatus.PENDING.value
        self.started_at: datetime | None = None
        self.finished_at: datetime | None = None
        self.duration_ms: int | None = None
        self.result: dict | None = None
        self.error: str | None = None
        self.trace: dict | None = None
        self.logs: str | None = None
        self.worker_id: str | None = None

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_ms": self.duration_ms,
            "result": self.result,
            "error": self.error,
            "trace": self.trace,
            "logs": self.logs,
            "worker_id": self.worker_id,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ExecutionResult":
        r = cls()
        r.status = data.get("status", "failed")
        r.result = data.get("result")
        r.error = data.get("error")
        r.trace = data.get("trace")
        r.logs = data.get("logs")
        r.duration_ms = data.get("duration_ms")
        r.worker_id = data.get("worker_id")
        for key in ("started_at", "finished_at"):
            if data.get(key):
                setattr(r, key, datetime.fromisoformat(data[key]))
        return r

    @classmethod
    def failure(cls, error: str) -> "ExecutionResult":
        r = cls()
        r.status = RunStatus.FAILED.value
        r.error = error
        r.started_at = r.finished_at = datetime.now(tz=timezone.utc)
        r.duration_ms = 0
        r.trace = {"status": r.status, "steps": []}
        r.logs = error
        return r


# Seconds to wait before retry N is RETRY_BACKOFF_BASE * 2**(N-1), capped at RETRY_BACKOFF_MAX.
RETRY_BACKOFF_BASE = 1.0
RETRY_BACKOFF_MAX = 30.0


def retry_backoff(attempt: int) -> float:
    return min(RETRY_BACKOFF_BASE * 2 ** (attempt - 1), RETRY_BACKOFF_MAX)


async def execute_pipeline(
    pipeline_name: str,
    code: str,
    run_id: str,
    params: dict | None = None,
    timeout_seconds: int = 1800,
    last_successful_run: datetime | None = None,
    retry_count: int = 0,
) -> ExecutionResult:
    """Execute pipeline code and return structured results.

    A failing pipeline is retried up to ``retry_count`` times with exponential backoff.
    Timeouts are not retried. Logs from every attempt are kept, and the trace records
    how many attempts were made.
    """
    started_at = datetime.now(tz=timezone.utc)
    previous_logs: list[str] = []
    attempts = 1 + max(0, retry_count or 0)

    for attempt in range(1, attempts + 1):
        result = await _execute_once(
            pipeline_name, code, run_id, params, timeout_seconds, last_successful_run
        )
        if attempt > 1:
            result.logs = "\n".join(previous_logs + [result.logs or ""])
        result.started_at = started_at
        result.duration_ms = int((result.finished_at - started_at).total_seconds() * 1000)
        result.trace["started_at"] = started_at.isoformat()
        result.trace["duration_ms"] = result.duration_ms
        result.trace["attempts"] = attempt

        if result.status != RunStatus.FAILED.value or attempt == attempts:
            return result

        delay = retry_backoff(attempt)
        previous_logs.append(result.logs or "")
        previous_logs.append(
            f"[{datetime.now(tz=timezone.utc).isoformat()}] Attempt {attempt} failed; "
            f"retrying in {delay:g}s ({attempts - attempt} retr{'y' if attempts - attempt == 1 else 'ies'} left)"
        )
        await asyncio.sleep(delay)

    return result  # pragma: no cover - the loop always returns


async def _execute_once(
    pipeline_name: str,
    code: str,
    run_id: str,
    params: dict | None,
    timeout_seconds: int,
    last_successful_run: datetime | None,
) -> ExecutionResult:
    result = ExecutionResult()
    result.started_at = datetime.now(tz=timezone.utc)
    result.status = RunStatus.RUNNING.value

    # Create connection registry from settings
    settings = get_settings()
    connection_registry = ConnectionRegistry(settings.connections)

    ctx = PipelineContext(
        pipeline_name=pipeline_name,
        run_id=run_id,
        params=params,
        last_successful_run=last_successful_run,
        connection_registry=connection_registry,
    )

    try:
        # Load the pipeline code as a module
        func = _load_pipeline_function(pipeline_name, code)
        if func is None:
            raise RuntimeError(f"No @pipeline decorated function found in code for '{pipeline_name}'")

        try:
            pipeline_result = await asyncio.wait_for(func(ctx), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            result.status = RunStatus.TIMEOUT.value
            result.error = f"Pipeline timed out after {timeout_seconds}s"
            ctx.log(f"TIMEOUT after {timeout_seconds}s")
            return result

        result.status = RunStatus.SUCCESS.value
        result.result = pipeline_result if isinstance(pipeline_result, dict) else {"output": str(pipeline_result)}

    except Exception as e:
        result.status = RunStatus.FAILED.value
        result.error = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        ctx.log(f"FAILED: {type(e).__name__}: {str(e)}")

    finally:
        await ctx.cleanup_connections()

        result.finished_at = datetime.now(tz=timezone.utc)
        result.duration_ms = int((result.finished_at - result.started_at).total_seconds() * 1000)
        result.trace = ctx.get_trace()
        result.trace["status"] = result.status
        result.trace["started_at"] = result.started_at.isoformat()
        result.trace["finished_at"] = result.finished_at.isoformat()
        result.trace["duration_ms"] = result.duration_ms
        result.logs = ctx.get_logs()

    return result


def _load_pipeline_function(pipeline_name: str, code: str):
    """Load pipeline code and find the decorated function."""
    module_name = f"tau_pipeline_{pipeline_name}"

    # Create a temporary module from code string
    spec = importlib.util.spec_from_loader(module_name, loader=None)
    module = importlib.util.module_from_spec(spec)

    # Inject tau imports into the module's namespace
    exec(code, module.__dict__)

    # Find the @pipeline decorated function
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if callable(attr) and hasattr(attr, "_tau_pipeline"):
            meta = attr._tau_pipeline
            if meta["name"] == pipeline_name or attr_name == pipeline_name:
                return attr

    # Fallback: look for any async function with _tau_pipeline
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if callable(attr) and hasattr(attr, "_tau_pipeline"):
            return attr

    return None
