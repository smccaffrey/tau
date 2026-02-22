"""DAG dependency model — pipeline dependency graph."""

import uuid
from datetime import datetime
from sqlalchemy import String, DateTime, ForeignKey, Integer, JSON
from sqlalchemy.orm import Mapped, mapped_column
from tau.core.database import Base


class PipelineDependency(Base):
    """Defines an edge in the pipeline DAG: upstream → downstream."""
    __tablename__ = "pipeline_dependencies"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    upstream_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    downstream_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
