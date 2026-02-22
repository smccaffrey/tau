"""SCD Type 2 Customers — track full history of customer changes.

Demonstrates:
- SCD Type 2 materialization (slowly changing dimension)
- valid_from/valid_to/is_current for time-travel queries
- Row hash for change detection
- Hard delete invalidation
"""

from tau import pipeline, PipelineContext
from tau.materializations import SCDType2Config


@pipeline(
    name="scd2_customers",
    description="Track customer dimension history — every change creates a new version",
    schedule="0 4 * * *",  # Daily at 4 AM
    tags=["warehouse", "scd2", "dimension"],
)
async def scd2_customers(ctx: PipelineContext):
    config = SCDType2Config(
        target_table="analytics.dim_customers",
        source_query="""
            SELECT
                customer_id,
                full_name,
                email,
                tier,
                region,
                account_manager
            FROM raw.customers
            WHERE is_active = true
        """,
        unique_key="customer_id",
        tracked_columns=["full_name", "email", "tier", "region", "account_manager"],
        valid_from_column="valid_from",
        valid_to_column="valid_to",
        is_current_column="is_current",
        hash_column="row_hash",
        invalidate_hard_deletes=True,  # Close records that disappear from source
    )

    result = await ctx.materialize(config, connector=ctx.connector, dialect="postgres")
    ctx.log(f"SCD2 customers: {result['rows']} total rows")
    return result


# Query patterns for consuming this table:
#
# Current state:
#   SELECT * FROM analytics.dim_customers WHERE is_current = TRUE
#
# Point-in-time:
#   SELECT * FROM analytics.dim_customers
#   WHERE valid_from <= '2026-01-15' AND (valid_to IS NULL OR valid_to > '2026-01-15')
#
# Change history for one customer:
#   SELECT * FROM analytics.dim_customers
#   WHERE customer_id = 42 ORDER BY valid_from
