"""Worker model — tracks registered workers."""

import uuid
from datetime import datetime
from sqlalchemy import String, DateTime, Integer, JSON, Boolean
from sqlalchemy.orm import Mapped, mapped_column
from tau.core.database import Base
import enum


class WorkerStatus(str, enum.Enum):
    IDLE = "idle"
    BUSY = "busy"
    OFFLINE = "offline"
    DRAINING = "draining"  # Finishing current work, not accepting new


class Worker(Base):
    """A registered worker (local process or remote instance)."""
    __tablename__ = "workers"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    host: Mapped[str] = mapped_column(String(255), nullable=False)  # hostname:port or "local"
    status: Mapped[str] = mapped_column(String(20), default=WorkerStatus.IDLE.value)
    max_concurrent: Mapped[int] = mapped_column(Integer, default=4)
    current_load: Mapped[int] = mapped_column(Integer, default=0)
    tags: Mapped[dict | None] = mapped_column(JSON, nullable=True)  # Worker capabilities/labels
    last_heartbeat: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    registered_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    is_local: Mapped[bool] = mapped_column(Boolean, default=True)  # Local process vs remote
