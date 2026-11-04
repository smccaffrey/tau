"""Baseline schema (Tau 0.x before migrations existed).

Databases created by earlier versions already have these tables; they are stamped with
this revision on first startup instead of running it.

Revision ID: 0001
Revises:
"""

from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pipelines",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("code", sa.Text(), nullable=False),
        sa.Column("filename", sa.String(255), nullable=True),
        sa.Column("tags", sa.JSON(), nullable=True),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("schedule_cron", sa.String(100), nullable=True),
        sa.Column("schedule_interval", sa.Integer(), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.Column("timeout_seconds", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("last_run_at", sa.DateTime(), nullable=True),
        sa.Column("last_run_status", sa.String(20), nullable=True),
    )
    op.create_index("ix_pipelines_name", "pipelines", ["name"], unique=True)

    op.create_table(
        "pipeline_runs",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("pipeline_id", sa.String(36), sa.ForeignKey("pipelines.id"), nullable=False),
        sa.Column("pipeline_name", sa.String(255), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("trigger", sa.String(50), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column("params", sa.JSON(), nullable=True),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("trace", sa.JSON(), nullable=True),
        sa.Column("logs", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_pipeline_runs_pipeline_id", "pipeline_runs", ["pipeline_id"])
    op.create_index("ix_pipeline_runs_pipeline_name", "pipeline_runs", ["pipeline_name"])

    op.create_table(
        "pipeline_dependencies",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("upstream_name", sa.String(255), nullable=False),
        sa.Column("downstream_name", sa.String(255), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_pipeline_dependencies_upstream_name", "pipeline_dependencies", ["upstream_name"])
    op.create_index("ix_pipeline_dependencies_downstream_name", "pipeline_dependencies", ["downstream_name"])


def downgrade() -> None:
    op.drop_table("pipeline_dependencies")
    op.drop_table("pipeline_runs")
    op.drop_table("pipelines")
