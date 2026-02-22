"""Pydantic schemas for schedules."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class ScheduleCreate(BaseModel):
    pipeline_name: str
    cron_expression: str


class ScheduleResponse(BaseModel):
    model_config = {"from_attributes": True}

    id: str
    pipeline_id: str
    cron_expression: str
    is_active: bool
    created_at: datetime
    pipeline_name: str | None = None


class ScheduleListResponse(BaseModel):
    schedules: list[ScheduleResponse]
    total: int
