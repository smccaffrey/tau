"""Pydantic schemas for pipeline runs."""

from datetime import datetime
from pydantic import BaseModel


class RunCreate(BaseModel):
    pipeline_name: str
    params: dict | None = None
    trigger: str = "manual"


class RunResponse(BaseModel):
    id: str
    pipeline_id: str
    pipeline_name: str
    status: str
    trigger: str
    started_at: datetime | None
    finished_at: datetime | None
    duration_ms: int | None
    params: dict | None
    result: dict | None
    error: str | None
    trace: dict | None
    created_at: datetime

    model_config = {"from_attributes": True}


class RunListResponse(BaseModel):
    runs: list[RunResponse]
    total: int


class RunLogsResponse(BaseModel):
    id: str
    pipeline_name: str
    logs: str | None
