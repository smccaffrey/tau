"""Full Refresh Products — drop and recreate the products table every run.

Demonstrates:
- Full refresh materialization (simplest strategy)
- Pre/post hooks for indexes and maintenance
"""

from tau import pipeline, PipelineContext
from tau.materializations import FullRefreshConfig


@pipeline(
    name="full_refresh_products",
    description="Rebuild the products dimension table from scratch",
    schedule="0 3 * * *",  # Daily at 3 AM
    tags=["warehouse", "full-refresh", "dimension"],
)
async def full_refresh_products(ctx: PipelineContext):
    config = FullRefreshConfig(
        target_table="analytics.dim_products",
        source_query="""
            SELECT
                p.product_id,
                p.name,
                p.sku,
                c.category_name,
                p.price,
                p.cost,
                p.price - p.cost AS margin,
                p.is_active,
                p.created_at
            FROM raw.products p
            JOIN raw.categories c ON p.category_id = c.category_id
        """,
        pre_hook="DROP INDEX IF EXISTS idx_dim_products_sku",
        post_hook="CREATE INDEX idx_dim_products_sku ON analytics.dim_products(sku)",
    )

    result = await ctx.materialize(config, connector=ctx.connector, dialect="postgres")
    ctx.log(f"Refreshed dim_products: {result['rows']} rows")
    return result
