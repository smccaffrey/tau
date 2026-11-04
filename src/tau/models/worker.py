"""Registered remote workers, so every coordinator sharing the database sees them."""

from datetime import datetime

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from tau.core.database import Base


class RegisteredWorker(Base):
    __tablename__ = "workers"

    worker_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    host: Mapped[str] = mapped_column(String(512), nullable=False)
    # The worker's own API key; coordinators use it to send runs to the worker.
    api_key: Mapped[str] = mapped_column(String(512), nullable=False)
    max_concurrent: Mapped[int] = mapped_column(Integer, default=4, nullable=False)
    registered_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
