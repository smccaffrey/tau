"""Append-Only Logs — insert-only event log, never update.

Demonstrates:
- Using named connections from tau.toml
- Append-only materialization (immutable event log)
- No deduplication — every run appends

Configure in tau.toml:
[connections.warehouse]
type = "postgres"
dsn = "${WAREHOUSE_DSN}"
"""

from tau import pipeline, PipelineContext
from tau.materializations import MaterializationConfig, MaterializationType


@pipeline(
    name="append_only_logs",
    description="Append raw API logs — immutable audit trail",
    schedule="*/15 * * * *",  # Every 15 minutes
    tags=["warehouse", "append-only", "logs"],
)
async def append_only_logs(ctx: PipelineContext):
    # Get warehouse connection from registry
    warehouse = await ctx.connection("warehouse")

    config = MaterializationConfig(
        target_table="raw.api_logs",
        source_query="""
            SELECT
                log_id,
                endpoint,
                method,
                status_code,
                response_time_ms,
                user_agent,
                ip_address,
                created_at
            FROM staging.api_logs_buffer
        """,
        strategy=MaterializationType.APPEND_ONLY,
    )

    result = await ctx.materialize(config, connector=warehouse, dialect="postgres")
    ctx.log(f"Appended logs: {result['rows']} total rows")
    return result
