"""Incremental Orders — merge new/changed orders into a target table.

Demonstrates:
- Incremental materialization with MERGE strategy
- Unique key matching for upserts
- Incremental column for filtering new rows
"""

from tau import pipeline, PipelineContext
from tau.materializations import IncrementalConfig


@pipeline(
    name="incremental_orders",
    description="Incrementally load orders — only process new/changed rows",
    schedule="0 */2 * * *",  # Every 2 hours
    tags=["warehouse", "incremental"],
)
async def incremental_orders(ctx: PipelineContext):
    config = IncrementalConfig(
        target_table="analytics.orders",
        source_query="""
            SELECT
                order_id,
                customer_id,
                status,
                total_amount,
                updated_at
            FROM raw.orders
            WHERE updated_at > COALESCE(
                (SELECT MAX(updated_at) FROM analytics.orders),
                '1970-01-01'
            )
        """,
        unique_key="order_id",
        incremental_column="updated_at",
        incremental_strategy="merge",  # merge | delete+insert | insert_overwrite
    )

    result = await ctx.materialize(config, connector=ctx.connector, dialect="postgres")
    ctx.log(f"Merged {result['rows']} orders (first_run={result.get('first_run', False)})")
    return result
