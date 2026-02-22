"""Pipeline decorator — marks a function as a Tau pipeline."""

from __future__ import annotations
from typing import Callable, Any
import functools

# Global registry of decorated pipelines
_pipeline_registry: dict[str, dict] = {}


def pipeline(
    name: str | None = None,
    description: str | None = None,
    schedule: str | None = None,
    retry: int = 3,
    timeout: str = "30m",
    tags: list[str] | None = None,
):
    """Decorator to mark an async function as a Tau pipeline."""

    def decorator(func: Callable) -> Callable:
        pipeline_name = name or func.__name__

        # Parse timeout string to seconds
        timeout_seconds = _parse_timeout(timeout)

        metadata = {
            "name": pipeline_name,
            "description": description or func.__doc__ or "",
            "schedule": schedule,
            "retry": retry,
            "timeout_seconds": timeout_seconds,
            "tags": tags or [],
            "func": func,
        }

        _pipeline_registry[pipeline_name] = metadata

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            return await func(*args, **kwargs)

        wrapper._tau_pipeline = metadata
        return wrapper

    return decorator


def _parse_timeout(timeout: str) -> int:
    """Parse timeout string like '30m', '1h', '90s' to seconds."""
    if timeout.endswith("m"):
        return int(timeout[:-1]) * 60
    elif timeout.endswith("h"):
        return int(timeout[:-1]) * 3600
    elif timeout.endswith("s"):
        return int(timeout[:-1])
    return int(timeout)


def get_registry() -> dict[str, dict]:
    return _pipeline_registry
