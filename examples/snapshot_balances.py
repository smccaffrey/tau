"""Snapshot Balances — point-in-time snapshots with retention.

Demonstrates:
- Using named connections from tau.toml
- Snapshot materialization (full copy each run)
- Snapshot ID and timestamp columns
- Retention pruning (keep last 30 snapshots)

Configure in tau.toml:
[connections.warehouse]
type = "postgres"
dsn = "${WAREHOUSE_DSN}"
"""

from tau import pipeline, PipelineContext
from tau.materializations import SnapshotConfig


@pipeline(
    name="snapshot_balances",
    description="Daily snapshot of account balances for audit trail",
    schedule="0 0 * * *",  # Midnight daily
    tags=["warehouse", "snapshot", "finance"],
)
async def snapshot_balances(ctx: PipelineContext):
    # Get warehouse connection from registry
    warehouse = await ctx.connection("warehouse")

    config = SnapshotConfig(
        target_table="analytics.balance_snapshots",
        source_query="""
            SELECT
                account_id,
                account_name,
                balance,
                currency,
                last_transaction_at
            FROM raw.accounts
            WHERE is_active = true
        """,
        snapshot_timestamp_column="snapshot_at",
        snapshot_id_column="snapshot_id",
        retain_snapshots=30,  # Keep last 30 daily snapshots
    )

    result = await ctx.materialize(config, connector=warehouse, dialect="postgres")
    ctx.log(f"Snapshot taken: {result.get('snapshot_id')}, {result['rows']} total rows")
    return result
