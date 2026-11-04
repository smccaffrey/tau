"""Pipeline executor — runs pipeline code and captures traces."""

from __future__ import annotations
import asyncio
import importlib.util
import threading
import traceback
from dataclasses import dataclass, field
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


# ─── Cancellation ───
#
# Every attempt registers a handle while it runs. ``cancel_run`` asks the attempt's own
# event loop (which may be in another thread) to cancel the pipeline task. A cancel that
# arrives while the run is still queued or between retries is remembered and honoured
# when the next attempt would start.


@dataclass
class _RunHandle:
    loop: asyncio.AbstractEventLoop | None = None
    task: asyncio.Task | None = None
    cancel_requested: bool = False
    started: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))


_runs: dict[str, _RunHandle] = {}
_runs_lock = threading.Lock()


def _handle(run_id: str) -> _RunHandle:
    with _runs_lock:
        return _runs.setdefault(run_id, _RunHandle())


def register_run(run_id: str) -> None:
    """Make a run cancellable before it starts executing (while it is queued)."""
    _handle(run_id)


def forget_run(run_id: str) -> None:
    with _runs_lock:
        _runs.pop(run_id, None)


def cancel_requested(run_id: str) -> bool:
    with _runs_lock:
        handle = _runs.get(run_id)
        return bool(handle and handle.cancel_requested)


def active_run_ids() -> list[str]:
    with _runs_lock:
        return list(_runs)


def is_active(run_id: str) -> bool:
    with _runs_lock:
        return run_id in _runs


def cancel_run(run_id: str) -> bool:
    """Request cancellation of a run executing (or queued) in this process.

    Returns False if this process does not know the run. Cancellation is cooperative:
    the pipeline gets ``asyncio.CancelledError`` at its next ``await``. Code that blocks
    without awaiting (e.g. ``time.sleep``) is only stopped by the run timeout.
    """
    with _runs_lock:
        handle = _runs.get(run_id)
        if handle is None:
            return False
        handle.cancel_requested = True
        loop, task = handle.loop, handle.task
    if loop is not None and task is not None and not loop.is_closed():
        try:
            loop.call_soon_threadsafe(task.cancel)
        except RuntimeError:  # loop closed between the check and the call
            pass
    return True


def cancelled_result(started_at: datetime, logs: str = "") -> ExecutionResult:
    r = ExecutionResult()
    r.status = RunStatus.CANCELLED.value
    r.error = "Run cancelled"
    r.started_at = started_at
    r.finished_at = datetime.now(tz=timezone.utc)
    r.duration_ms = int((r.finished_at - started_at).total_seconds() * 1000)
    r.trace = {"status": r.status, "steps": []}
    r.logs = "\n".join(x for x in (logs, f"[{r.finished_at.isoformat()}] CANCELLED") if x)
    return r


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
    Timeouts and cancellations are not retried. Logs from every attempt are kept, and the
    trace records how many attempts were made.

    With ``TAU_EXECUTION_MODE=thread`` (the default) each attempt runs on its own thread
    with its own event loop, so pipeline code that blocks cannot stall the daemon.
    ``inline`` runs attempts on the daemon's event loop.
    """
    started_at = datetime.now(tz=timezone.utc)
    previous_logs: list[str] = []
    attempts = 1 + max(0, retry_count or 0)
    handle = _handle(run_id)

    try:
        for attempt in range(1, attempts + 1):
            if handle.cancel_requested:
                return cancelled_result(started_at, "\n".join(previous_logs))
            result = await _execute_attempt(
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
            await _sleep_unless_cancelled(handle, delay)

        return result  # pragma: no cover - the loop always returns
    finally:
        with _runs_lock:
            _runs.pop(run_id, None)


async def _sleep_unless_cancelled(handle: _RunHandle, delay: float) -> None:
    deadline = asyncio.get_running_loop().time() + delay
    while not handle.cancel_requested:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return
        await asyncio.sleep(min(remaining, 0.25))


async def _execute_attempt(
    pipeline_name: str,
    code: str,
    run_id: str,
    params: dict | None,
    timeout_seconds: int,
    last_successful_run: datetime | None,
) -> ExecutionResult:
    args = (pipeline_name, code, run_id, params, timeout_seconds, last_successful_run)
    settings = get_settings()
    if settings.execution_mode == "inline":
        return await _execute_once(*args)

    loop = asyncio.get_running_loop()
    done: asyncio.Future = loop.create_future()

    def deliver(value, error):
        if done.done():
            return
        if error is not None:
            done.set_exception(error)
        else:
            done.set_result(value)

    def target():
        try:
            value, error = asyncio.run(_execute_once(*args)), None
        except BaseException as e:  # pragma: no cover - _execute_once catches everything
            value, error = None, e
        try:
            loop.call_soon_threadsafe(deliver, value, error)
        except RuntimeError:  # the daemon loop has shut down
            pass

    thread = threading.Thread(target=target, name=f"tau-run-{run_id[:8]}", daemon=True)
    started_at = datetime.now(tz=timezone.utc)
    thread.start()

    # The pipeline enforces its own timeout. This guard covers code that blocks without
    # awaiting, which the in-thread timeout cannot interrupt. Python cannot kill a thread,
    # so such a thread is abandoned and the run is recorded as timed out.
    grace = max(1, settings.timeout_grace)
    try:
        return await asyncio.wait_for(asyncio.shield(done), timeout=timeout_seconds + grace)
    except asyncio.TimeoutError:
        cancel_run(run_id)
        r = ExecutionResult()
        r.status = RunStatus.TIMEOUT.value
        r.error = (
            f"Pipeline timed out after {timeout_seconds}s and did not stop within {grace}s "
            "(it is blocking without awaiting; its thread was abandoned)"
        )
        r.started_at = started_at
        r.finished_at = datetime.now(tz=timezone.utc)
        r.duration_ms = int((r.finished_at - started_at).total_seconds() * 1000)
        r.trace = {"status": r.status, "steps": []}
        r.logs = r.error
        return r


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
    handle = _handle(run_id)

    try:
        # Load the pipeline code as a module
        func = _load_pipeline_function(pipeline_name, code)
        if func is None:
            raise RuntimeError(f"No @pipeline decorated function found in code for '{pipeline_name}'")

        task = asyncio.ensure_future(func(ctx))
        with _runs_lock:
            handle.loop, handle.task = asyncio.get_running_loop(), task
            cancel_now = handle.cancel_requested
        if cancel_now:
            task.cancel()

        try:
            pipeline_result = await asyncio.wait_for(task, timeout=timeout_seconds)
        except asyncio.TimeoutError:
            result.status = RunStatus.TIMEOUT.value
            result.error = f"Pipeline timed out after {timeout_seconds}s"
            ctx.log(f"TIMEOUT after {timeout_seconds}s")
            return result
        except asyncio.CancelledError:
            if not handle.cancel_requested:
                raise  # the daemon itself is shutting down
            result.status = RunStatus.CANCELLED.value
            result.error = "Run cancelled"
            ctx.log("CANCELLED")
            return result

        result.result = pipeline_result if isinstance(pipeline_result, dict) else {"output": str(pipeline_result)}
        elapsed = (datetime.now(tz=timezone.utc) - result.started_at).total_seconds()
        if elapsed > timeout_seconds:
            # Blocking code kept the timeout from firing; the run still overran it.
            result.status = RunStatus.TIMEOUT.value
            result.error = (
                f"Pipeline finished after {elapsed:.0f}s, past its {timeout_seconds}s timeout "
                "(it blocked without awaiting, so it could not be stopped in time)"
            )
            ctx.log(f"TIMEOUT: finished after {elapsed:.0f}s (limit {timeout_seconds}s)")
        else:
            result.status = RunStatus.SUCCESS.value

    except Exception as e:
        result.status = RunStatus.FAILED.value
        result.error = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        ctx.log(f"FAILED: {type(e).__name__}: {str(e)}")

    finally:
        with _runs_lock:
            handle.loop = handle.task = None
        try:
            await ctx.cleanup_connections()
        except Exception as e:
            ctx.log(f"Warning: closing connections failed: {type(e).__name__}: {e}")

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
