"""Main API router."""

from fastapi import APIRouter
from tau.api.pipelines import router as pipelines_router
from tau.api.runs import router as runs_router
from tau.api.webhooks import router as webhooks_router
from tau.api.ai import router as ai_router
from tau.api.dag import router as dag_router
from tau.api.workers import router as workers_router
from tau.api.connections import router as connections_router

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(pipelines_router)
api_router.include_router(runs_router)
api_router.include_router(webhooks_router)
api_router.include_router(ai_router)
api_router.include_router(dag_router)
api_router.include_router(workers_router)
api_router.include_router(connections_router)
