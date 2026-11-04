"""DAG API endpoints — dependency management and DAG execution."""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from tau.core.auth import verify_api_key
from tau.core.database import get_session
from tau.dag.resolver import CycleError, DAGResolver
from tau.models.pipeline import PipelineStatus
from tau.repositories.dependency_repo import DependencyRepository
from tau.repositories.pipeline_repo import PipelineRepository

router = APIRouter(prefix="/dag", tags=["dag"])

# In-memory DAG. Edges are stored in the pipeline_dependencies table and loaded on startup.
_dag = DAGResolver()


def get_dag() -> DAGResolver:
    return _dag


async def load_dag(session: AsyncSession) -> int:
    """Rebuild the in-memory DAG from the database. Returns the number of edges."""
    edges = await DependencyRepository(session).list_edges()
    _dag.nodes.clear()
    # Register every active pipeline as a node first, so pipelines with no
    # declared dependencies still appear in the DAG (otherwise the graph is
    # empty until a dependency edge exists).
    for pipeline in await PipelineRepository(session).list_all():
        if pipeline.status == PipelineStatus.ACTIVE.value:
            _dag.add_pipeline(pipeline.name)
    for up, down in edges:
        _dag.add_dependency(up, down)
    return len(edges)


class DependencyCreate(BaseModel):
    upstream: str
    downstream: str


class DependenciesSet(BaseModel):
    pipeline: str
    depends_on: list[str]


@router.get("")
async def get_dag_info(
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """Get the full DAG structure."""
    # Rebuild from the database so pipelines deployed since startup (including
    # standalone ones that add no dependency edge) are always reflected.
    await load_dag(session)
    try:
        return _dag.to_dict()
    except CycleError as e:
        raise HTTPException(400, str(e))


@router.post("/dependency")
async def add_dependency(
    data: DependencyCreate,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """Add a single dependency edge."""
    upstream = sorted(_dag.nodes[data.downstream].upstream) if data.downstream in _dag.nodes else []
    try:
        _dag.set_upstream(data.downstream, upstream + [data.upstream])
    except CycleError as e:
        raise HTTPException(400, f"Would create cycle: {' → '.join(e.cycle)}")
    await DependencyRepository(session).add_edge(data.upstream, data.downstream)
    return {"status": "added", "upstream": data.upstream, "downstream": data.downstream}


@router.post("/dependencies")
async def set_dependencies(
    data: DependenciesSet,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """Set all dependencies for a pipeline (replaces the existing ones)."""
    try:
        _dag.set_upstream(data.pipeline, data.depends_on)
    except CycleError as e:
        raise HTTPException(400, f"Dependency cycle: {' → '.join(e.cycle)}")
    await DependencyRepository(session).set_upstream(data.pipeline, data.depends_on)

    return {
        "status": "set",
        "pipeline": data.pipeline,
        "depends_on": data.depends_on,
    }


@router.post("/run")
async def run_dag(root: str | None = None, _: str = Depends(verify_api_key)):
    """Run the whole DAG, or ``root`` and everything downstream of it, in dependency order.

    Pipelines in the same level run in parallel; a pipeline whose upstream failed is skipped.
    """
    from tau.daemon.runner import run_dag as _run_dag

    if root is not None and root not in _dag.nodes:
        raise HTTPException(404, f"'{root}' is not part of the DAG")
    try:
        result = await _run_dag(root=root)
    except CycleError as e:
        raise HTTPException(400, str(e))
    return result.to_dict()


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
