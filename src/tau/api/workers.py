"""Worker API endpoints — worker pool management."""

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from tau.core.auth import verify_api_key

router = APIRouter(prefix="/workers", tags=["workers"])

# Worker pool is initialized by the daemon on startup
_pool = None


def set_pool(pool):
    global _pool
    _pool = pool


def get_pool():
    return _pool


class ExecuteRequest(BaseModel):
    pipeline_name: str
    code: str
    run_id: str
    params: dict | None = None
    timeout_seconds: int = 1800
    retry_count: int = 0
    last_successful_run: datetime | None = None


class WorkerRegister(BaseModel):
    worker_id: str
    host: str
    api_key: str
    max_concurrent: int = 4


@router.get("")
async def list_workers(_: str = Depends(verify_api_key)):
    """List all workers and their status."""
    if not _pool:
        return {"workers": [], "total_capacity": 0, "total_load": 0}
    return _pool.info()


@router.post("/register")
async def register_worker(
    data: WorkerRegister,
    _: str = Depends(verify_api_key),
):
    """Register a remote worker.

    Idempotent: workers re-register periodically (see TAU_COORDINATOR_URL), so an online
    worker that registers again with the same host and key is left as it is.
    """
    if not _pool:
        raise HTTPException(503, "Worker pool not initialized")

    existing = _pool.get_remote(data.worker_id)
    if (
        existing is not None
        and existing.status != "offline"
        and existing.host == data.host.rstrip("/")
        and existing.api_key == data.api_key
        and existing.max_concurrent == data.max_concurrent
    ):
        return {"status": "registered", "worker_id": data.worker_id}

    worker = _pool.add_remote(
        worker_id=data.worker_id,
        host=data.host,
        api_key=data.api_key,
        max_concurrent=data.max_concurrent,
    )

    try:
        await worker.connect()
    except Exception as e:
        return {
            "status": "registered_offline",
            "worker_id": data.worker_id,
            "error": str(e),
        }

    return {"status": "registered", "worker_id": data.worker_id}


@router.post("/execute")
async def execute_on_this_worker(data: ExecuteRequest, _: str = Depends(verify_api_key)):
    """Run pipeline code on this daemon's local worker and return the result.

    Called by a coordinator. Nothing is stored here: the coordinator records the run.
    """
    from tau.daemon.executor import execute_pipeline

    kwargs = dict(
        pipeline_name=data.pipeline_name,
        code=data.code,
        run_id=data.run_id,
        params=data.params,
        timeout_seconds=data.timeout_seconds,
        last_successful_run=data.last_successful_run,
        retry_count=data.retry_count,
    )
    local = _pool.local_worker if _pool else None
    result = await (local.execute(**kwargs) if local else execute_pipeline(**kwargs))
    if local:
        result.worker_id = local.worker_id
    return result.to_dict()


@router.post("/heartbeat")
async def heartbeat(_: str = Depends(verify_api_key)):
    """Trigger a health check on all workers."""
    if not _pool:
        return {"results": []}
    results = await _pool.broadcast_heartbeat()
    return {"results": results}


@router.delete("/{worker_id}")
async def remove_worker(worker_id: str, _: str = Depends(verify_api_key)):
    """Remove a worker from the pool."""
    if not _pool:
        raise HTTPException(503, "Worker pool not initialized")
    if await _pool.remove_remote(worker_id):
        return {"status": "removed", "worker_id": worker_id}
    raise HTTPException(404, f"Worker '{worker_id}' not found")
