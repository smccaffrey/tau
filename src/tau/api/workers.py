"""Worker API endpoints — worker pool management."""

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
    """Register a remote worker."""
    if not _pool:
        raise HTTPException(503, "Worker pool not initialized")

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

    for i, w in enumerate(_pool._remote_workers):
        if w.worker_id == worker_id:
            await w.disconnect()
            _pool._remote_workers.pop(i)
            return {"status": "removed", "worker_id": worker_id}

    raise HTTPException(404, f"Worker '{worker_id}' not found")
