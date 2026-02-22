"""Partitioned Events — partition-aware incremental with auto-expiry.

Demonstrates:
- Partitioned materialization by date
- BigQuery-native PARTITION BY + CLUSTER BY
- Automatic partition expiration (90 days)
- Incremental loading into partitioned table
"""

from tau import pipeline, PipelineContext
from tau.materializations import PartitionedConfig


@pipeline(
    name="partitioned_events",
    description="Load events into date-partitioned table with 90-day retention",
    schedule="*/30 * * * *",  # Every 30 minutes
    tags=["warehouse", "partitioned", "events"],
)
async def partitioned_events(ctx: PipelineContext):
    config = PartitionedConfig(
        target_table="analytics.events",
        source_query="""
            SELECT
                event_id,
                user_id,
                event_type,
                properties,
                event_date,
                created_at
            FROM raw.events
        """,
        unique_key="event_id",
        partition_by="event_date",
        partition_type="date",
        partition_granularity="day",
        cluster_by=["user_id", "event_type"],  # BigQuery/Snowflake clustering
        incremental_column="created_at",
        partition_expiration_days=90,  # Auto-delete partitions older than 90 days
    )

    result = await ctx.materialize(config, connector=ctx.connector, dialect="bigquery")
    ctx.log(f"Events partitioned: {result['rows']} total rows")
    return result
