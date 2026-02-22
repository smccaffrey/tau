"""Main API router."""

from fastapi import APIRouter
from tau.api.pipelines import router as pipelines_router
from tau.api.runs import router as runs_router

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(pipelines_router)
api_router.include_router(runs_router)
