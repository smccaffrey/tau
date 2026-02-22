"""DAG API endpoints — dependency management and DAG execution."""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from tau.core.auth import verify_api_key
from tau.core.database import get_session
from tau.dag.resolver import DAGResolver, CycleError
from tau.repositories.pipeline_repo import PipelineRepository

router = APIRouter(prefix="/dag", tags=["dag"])

# In-memory DAG (rebuilt from DB on startup, modified via API)
_dag = DAGResolver()


def get_dag() -> DAGResolver:
    return _dag


class DependencyCreate(BaseModel):
    upstream: str
    downstream: str


class DependenciesSet(BaseModel):
    pipeline: str
    depends_on: list[str]


@router.get("")
async def get_dag_info(_: str = Depends(verify_api_key)):
    """Get the full DAG structure."""
    try:
        return _dag.to_dict()
    except CycleError as e:
        raise HTTPException(400, str(e))


@router.post("/dependency")
async def add_dependency(
    data: DependencyCreate,
    _: str = Depends(verify_api_key),
):
    """Add a single dependency edge."""
    _dag.add_dependency(data.upstream, data.downstream)
    cycle = _dag.detect_cycles()
    if cycle:
        # Roll back — remove the edge
        _dag._nodes[data.upstream].downstream.discard(data.downstream)
        _dag._nodes[data.downstream].upstream.discard(data.upstream)
        raise HTTPException(400, f"Would create cycle: {' → '.join(cycle)}")
    return {"status": "added", "upstream": data.upstream, "downstream": data.downstream}


@router.post("/dependencies")
async def set_dependencies(
    data: DependenciesSet,
    _: str = Depends(verify_api_key),
):
    """Set all dependencies for a pipeline."""
    # Clear existing upstream deps
    if data.pipeline in _dag._nodes:
        old_upstream = list(_dag._nodes[data.pipeline].upstream)
        for up in old_upstream:
            _dag._nodes[up].downstream.discard(data.pipeline)
        _dag._nodes[data.pipeline].upstream.clear()

    # Add new deps
    _dag.add_dependencies(data.pipeline, data.depends_on)

    cycle = _dag.detect_cycles()
    if cycle:
        raise HTTPException(400, f"Dependency cycle: {' → '.join(cycle)}")

    return {
        "status": "set",
        "pipeline": data.pipeline,
        "depends_on": data.depends_on,
    }


@router.get("/order")
async def execution_order(_: str = Depends(verify_api_key)):
    """Get topological execution order."""
    try:
        return {
            "order": _dag.topological_sort(),
            "groups": _dag.parallel_groups(),
        }
    except CycleError as e:
        raise HTTPException(400, str(e))


@router.get("/{name}/upstream")
async def get_upstream(name: str, _: str = Depends(verify_api_key)):
    """Get all upstream dependencies of a pipeline."""
    return {"pipeline": name, "upstream": sorted(_dag.get_upstream(name))}


@router.get("/{name}/downstream")
async def get_downstream(name: str, _: str = Depends(verify_api_key)):
    """Get all downstream dependents of a pipeline."""
    return {"pipeline": name, "downstream": sorted(_dag.get_downstream(name))}
