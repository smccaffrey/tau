"""Run ownership and cancellation; persistent workers and webhooks.

Revision ID: 0002
Revises: 0001
"""

from alembic import op
import sqlalchemy as sa

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("pipeline_runs") as batch:
        batch.add_column(sa.Column("instance_id", sa.String(255), nullable=True))
        batch.add_column(sa.Column("worker_id", sa.String(255), nullable=True))
        batch.add_column(
            sa.Column("cancel_requested", sa.Boolean(), nullable=False, server_default=sa.false())
        )
        batch.create_index("ix_pipeline_runs_instance_id", ["instance_id"])

    op.create_table(
        "workers",
        sa.Column("worker_id", sa.String(255), primary_key=True),
        sa.Column("host", sa.String(512), nullable=False),
        sa.Column("api_key", sa.String(512), nullable=False),
        sa.Column("max_concurrent", sa.Integer(), nullable=False),
        sa.Column("registered_at", sa.DateTime(), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(), nullable=False),
    )

    op.create_table(
        "webhooks",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("url", sa.String(2048), nullable=False, unique=True),
        sa.Column("events", sa.JSON(), nullable=False),
        sa.Column("secret", sa.String(512), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("webhooks")
    op.drop_table("workers")
    with op.batch_alter_table("pipeline_runs") as batch:
        batch.drop_index("ix_pipeline_runs_instance_id")
        batch.drop_column("cancel_requested")
        batch.drop_column("worker_id")
        batch.drop_column("instance_id")
