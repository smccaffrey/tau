"""DAG runner — executes pipelines respecting dependency order with parallelism."""

from __future__ import annotations
import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Awaitable, Any

from tau.dag.resolver import DAGResolver

logger = logging.getLogger("tau.dag")


@dataclass
class DAGRunResult:
    """Result of a full DAG execution."""
    status: str = "pending"  # pending | running | success | failed | partial
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: int | None = None
    results: dict[str, dict] = field(default_factory=dict)  # pipeline_name → result
    failed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    execution_order: list[list[str]] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_ms": self.duration_ms,
            "results": self.results,
            "failed": self.failed,
            "skipped": self.skipped,
            "execution_order": self.execution_order,
        }


class DAGRunner:
    """Executes a DAG of pipelines with dependency resolution and parallelism."""

    def __init__(
        self,
        dag: DAGResolver,
        run_fn: Callable[[str], Awaitable[dict]],
        max_parallel: int = 4,
        fail_fast: bool = False,
    ):
        """
        Args:
            dag: The resolved dependency graph
            run_fn: Async function that runs a pipeline by name, returns dict result
            max_parallel: Max pipelines to run concurrently within a group
            fail_fast: If True, skip all remaining pipelines when one fails
        """
        self.dag = dag
        self.run_fn = run_fn
        self.max_parallel = max_parallel
        self.fail_fast = fail_fast

    async def run(self, root: str | None = None) -> DAGRunResult:
        """Execute the DAG. Optionally start from a specific root node."""
        dag = self.dag.get_subgraph(root) if root else self.dag
        groups = dag.parallel_groups()

        result = DAGRunResult(
            status="running",
            started_at=datetime.now(tz=timezone.utc),
            execution_order=groups,
        )

        failed_set: set[str] = set()

        for group in groups:
            if self.fail_fast and failed_set:
                result.skipped.extend(group)
                continue

            # Filter out pipelines whose upstream failed
            runnable = []
            for name in group:
                upstream = dag.get_upstream(name)
                if upstream & failed_set:
                    result.skipped.append(name)
                    logger.info(f"Skipping {name} — upstream dependency failed")
                else:
                    runnable.append(name)

            # Run group with concurrency limit
            semaphore = asyncio.Semaphore(self.max_parallel)

            async def _run_one(name: str):
                async with semaphore:
                    try:
                        logger.info(f"Running pipeline: {name}")
                        r = await self.run_fn(name)
                        result.results[name] = r
                        status = r.get("status", "success")
                        if status in ("failed", "error"):
                            failed_set.add(name)
                            result.failed.append(name)
                        return r
                    except Exception as e:
                        logger.error(f"Pipeline {name} failed: {e}")
                        failed_set.add(name)
                        result.failed.append(name)
                        result.results[name] = {
                            "status": "failed",
                            "error": str(e),
                        }
                        return {"status": "failed", "error": str(e)}

            await asyncio.gather(*[_run_one(name) for name in runnable])

        result.finished_at = datetime.now(tz=timezone.utc)
        result.duration_ms = int(
            (result.finished_at - result.started_at).total_seconds() * 1000
        )

        if result.failed:
            result.status = "partial" if result.results else "failed"
        elif result.skipped:
            result.status = "partial"
        else:
            result.status = "success"

        return result

    async def run_downstream(self, trigger: str) -> DAGRunResult:
        """Run a pipeline and all its downstream dependents."""
        return await self.run(root=trigger)
