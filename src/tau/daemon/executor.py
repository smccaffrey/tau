"""Pipeline executor — runs pipeline code and captures traces."""

from __future__ import annotations
import asyncio
import importlib.util
import sys
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from tau.pipeline.context import PipelineContext
from tau.models.run import RunStatus


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


async def execute_pipeline(
    pipeline_name: str,
    code: str,
    run_id: str,
    params: dict | None = None,
    timeout_seconds: int = 1800,
    last_successful_run: datetime | None = None,
) -> ExecutionResult:
    """Execute pipeline code and return structured results."""
    result = ExecutionResult()
    result.started_at = datetime.utcnow()
    result.status = RunStatus.RUNNING.value

    ctx = PipelineContext(
        pipeline_name=pipeline_name,
        run_id=run_id,
        params=params,
        last_successful_run=last_successful_run,
    )

    try:
        # Load the pipeline code as a module
        func = _load_pipeline_function(pipeline_name, code)
        if func is None:
            raise RuntimeError(f"No @pipeline decorated function found in code for '{pipeline_name}'")

        # Execute with timeout
        try:
            pipeline_result = await asyncio.wait_for(
                func(ctx),
                timeout=timeout_seconds,
            )
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
        result.finished_at = datetime.utcnow()
        if result.started_at:
            result.duration_ms = int(
                (result.finished_at - result.started_at).total_seconds() * 1000
            )
        result.trace = ctx.get_trace()
        result.trace["status"] = result.status
        result.trace["started_at"] = result.started_at.isoformat() if result.started_at else None
        result.trace["finished_at"] = result.finished_at.isoformat() if result.finished_at else None
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
