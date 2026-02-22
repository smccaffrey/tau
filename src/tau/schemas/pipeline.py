"""Pydantic schemas for pipelines."""

from datetime import datetime
from pydantic import BaseModel, Field


class PipelineCreate(BaseModel):
    name: str
    description: str | None = None
    code: str
    filename: str | None = None
    tags: list[str] | None = None
    schedule_cron: str | None = None
    schedule_interval: int | None = None
    retry_count: int = 3
    timeout_seconds: int = 1800


class PipelineUpdate(BaseModel):
    description: str | None = None
    code: str | None = None
    tags: list[str] | None = None
    schedule_cron: str | None = None
    schedule_interval: int | None = None
    retry_count: int | None = None
    timeout_seconds: int | None = None
    status: str | None = None


class PipelineResponse(BaseModel):
    id: str
    name: str
    description: str | None
    filename: str | None
    tags: list[str] | None
    status: str
    schedule_cron: str | None
    schedule_interval: int | None
    retry_count: int
    timeout_seconds: int
    created_at: datetime
    updated_at: datetime
    last_run_at: datetime | None
    last_run_status: str | None

    model_config = {"from_attributes": True}


class PipelineListResponse(BaseModel):
    pipelines: list[PipelineResponse]
    total: int


class PipelineCodeResponse(BaseModel):
    name: str
    code: str
    filename: str | None
