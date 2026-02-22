"""View — create or replace a SQL view (zero storage).

Demonstrates:
- View materialization (no table, just a query alias)
- Useful for reporting layers, semantic models
"""

from tau import pipeline, PipelineContext
from tau.materializations import MaterializationConfig, MaterializationType


@pipeline(
    name="view_active_users",
    description="Maintain a view of active users for dashboards",
    schedule="0 6 * * *",  # Daily at 6 AM
    tags=["warehouse", "view", "reporting"],
)
async def view_active_users(ctx: PipelineContext):
    config = MaterializationConfig(
        target_table="reporting.v_active_users",
        source_query="""
            SELECT
                c.customer_id,
                c.full_name,
                c.email,
                c.tier,
                COUNT(o.order_id) AS total_orders,
                SUM(o.total_amount) AS lifetime_value,
                MAX(o.created_at) AS last_order_at
            FROM analytics.dim_customers c
            JOIN analytics.orders o ON c.customer_id = o.customer_id
            WHERE c.is_current = TRUE
            GROUP BY c.customer_id, c.full_name, c.email, c.tier
            HAVING MAX(o.created_at) > CURRENT_DATE - INTERVAL '90 days'
        """,
        strategy=MaterializationType.VIEW,
    )

    result = await ctx.materialize(config, connector=ctx.connector, dialect="postgres")
    ctx.log(f"View {config.target_table} refreshed")
    return result
