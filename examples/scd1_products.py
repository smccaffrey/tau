"""SCD Type 1 Products — overwrite changed values in place.

Demonstrates:
- SCD Type 1 materialization (latest-value-wins)
- Tracked columns for selective change detection
- Auto-updated updated_at timestamp
"""

from tau import pipeline, PipelineContext
from tau.materializations import SCDType1Config


@pipeline(
    name="scd1_products",
    description="Keep product dimension current — overwrite changed fields",
    schedule="0 5 * * *",  # Daily at 5 AM
    tags=["warehouse", "scd1", "dimension"],
)
async def scd1_products(ctx: PipelineContext):
    config = SCDType1Config(
        target_table="analytics.dim_products_current",
        source_query="""
            SELECT
                product_id,
                name,
                price,
                category,
                supplier,
                is_active
            FROM raw.products
        """,
        unique_key="product_id",
        tracked_columns=["name", "price", "category", "supplier", "is_active"],
        updated_at_column="updated_at",
    )

    result = await ctx.materialize(config, connector=ctx.connector, dialect="postgres")
    ctx.log(f"SCD1 products: {result['rows']} rows (first_run={result.get('first_run', False)})")
    return result
