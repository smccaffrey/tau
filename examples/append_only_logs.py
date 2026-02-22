"""Append-Only Logs — insert-only event log, never update.

Demonstrates:
- Append-only materialization (immutable event log)
- No deduplication — every run appends
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

    result = await ctx.materialize(config, connector=ctx.connector, dialect="postgres")
    ctx.log(f"Appended logs: {result['rows']} total rows")
    return result
