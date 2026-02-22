"""Full Warehouse ETL — multiple materializations in one pipeline.

Demonstrates:
- Composing multiple materialization strategies in a single pipeline
- Using connectors to talk to a warehouse
- A realistic multi-step data pipeline
"""

from tau import pipeline, PipelineContext
from tau.materializations import (
    FullRefreshConfig,
    IncrementalConfig,
    SCDType2Config,
    MaterializationConfig,
    MaterializationType,
)


@pipeline(
    name="warehouse_etl",
    description="Full warehouse refresh — dimensions, facts, and reporting views",
    schedule="0 4 * * *",  # Daily at 4 AM
    tags=["warehouse", "etl", "production"],
)
async def warehouse_etl(ctx: PipelineContext):
    connector = ctx.connector  # Pre-configured warehouse connector

    # Step 1: Refresh dimension table (full rebuild, small table)
    async with ctx.step("dim_products") as step:
        result = await ctx.materialize(
            FullRefreshConfig(
                target_table="analytics.dim_products",
                source_query="SELECT * FROM raw.products",
            ),
            connector=connector,
        )
        step.rows_out = result["rows"]

    # Step 2: Track customer changes with SCD2
    async with ctx.step("dim_customers") as step:
        result = await ctx.materialize(
            SCDType2Config(
                target_table="analytics.dim_customers",
                source_query="SELECT customer_id, name, email, tier, region FROM raw.customers",
                unique_key="customer_id",
                tracked_columns=["name", "email", "tier", "region"],
            ),
            connector=connector,
        )
        step.rows_out = result["rows"]

    # Step 3: Incrementally load orders
    async with ctx.step("fct_orders") as step:
        result = await ctx.materialize(
            IncrementalConfig(
                target_table="analytics.fct_orders",
                source_query="""
                    SELECT o.*, p.category, c.tier as customer_tier
                    FROM raw.orders o
                    JOIN analytics.dim_products p ON o.product_id = p.product_id
                    JOIN analytics.dim_customers c ON o.customer_id = c.customer_id AND c.is_current = TRUE
                """,
                unique_key="order_id",
                incremental_column="updated_at",
            ),
            connector=connector,
        )
        step.rows_out = result["rows"]

    # Step 4: Create reporting views
    async with ctx.step("reporting_views") as step:
        await ctx.materialize(
            MaterializationConfig(
                target_table="reporting.v_daily_revenue",
                source_query="""
                    SELECT DATE(created_at) as date, SUM(total) as revenue, COUNT(*) as orders
                    FROM analytics.fct_orders GROUP BY 1
                """,
                strategy=MaterializationType.VIEW,
            ),
            connector=connector,
        )
        step.rows_out = 1  # View created

    ctx.log("Warehouse ETL complete ✓")
    return {"steps_completed": 4}
