---
description: Write, deploy, and manage Tau data pipelines
---

You are helping the user build data pipelines with Tau Pipelines. Tau is a daemon (`taud`) + CLI (`tau`) for AI-native pipeline orchestration. **You only write pipeline files** — everything else (scheduler, executor, API, metadata) is handled by Tau.

## Workflow

1. Write a pipeline file (Python with `@pipeline` decorator)
2. Deploy it: `tau deploy my_pipeline.py`
3. Run it: `tau run my_pipeline`
4. Inspect results: `tau inspect my_pipeline --last-run`
5. Fix failures: `tau heal my_pipeline --auto`

## Pipeline Template

```python
from tau import pipeline, PipelineContext

@pipeline(
    name="my_pipeline",
    description="What it does",
    schedule="0 6 * * *",       # Optional cron
    depends_on=["upstream"],    # Optional DAG dependencies
    tags=["warehouse"],         # Optional tags
)
async def my_pipeline(ctx: PipelineContext):
    # Use ctx.step() for tracing
    async with ctx.step("extract") as step:
        data = [...]
        step.rows_out = len(data)

    # Use ctx.materialize() for warehouse tables
    from tau.materializations import IncrementalConfig
    await ctx.materialize(
        IncrementalConfig(
            target_table="analytics.orders",
            source_query="SELECT * FROM raw.orders",
            unique_key="order_id",
            incremental_column="updated_at",
        ),
        connector=db,
        dialect="postgres",
    )

    return {"rows": len(data)}
```

## Connectors

```python
from tau.connectors.postgres import postgres
from tau.connectors.bigquery import bigquery
from tau.connectors.snowflake import snowflake
from tau.connectors.motherduck import motherduck, duckdb_local
from tau.connectors.redshift import redshift
from tau.connectors.clickhouse import clickhouse
from tau.connectors.mysql import mysql
from tau.connectors.http_api import http_api
from tau.connectors.s3 import s3

async with postgres(dsn="postgresql://...") as db:
    data = await db.extract(query="SELECT * FROM users")
    await db.load(data, table="target", mode="upsert", merge_key="id")
```

## Materialization Strategies

```python
from tau.materializations import (
    FullRefreshConfig,       # DROP + CREATE AS
    IncrementalConfig,       # MERGE / delete+insert / insert_overwrite
    PartitionedConfig,       # Partition-aware incremental
    SCDType1Config,          # Overwrite changed values
    SCDType2Config,          # Track history (valid_from/valid_to)
    SnapshotConfig,          # Point-in-time copies
    MaterializationConfig,   # Base (APPEND_ONLY, VIEW)
    MaterializationType,
)
```

## CLI Quick Reference

```bash
tau deploy <file> [--schedule "cron"]   # Deploy pipeline
tau run <name>                          # Run pipeline
tau inspect <name> --last-run           # Structured JSON result
tau list                                # List pipelines
tau errors                              # Recent failures
tau heal <name> [--auto]                # AI diagnosis + fix
tau dag                                 # View dependency graph
tau depends <name> --on dep1 --on dep2  # Set dependencies
tau workers                             # Worker pool status
tau create "description"                # AI generates pipeline
tau schedule <name> "0 6 * * *"         # Set schedule
```

## Rules

- **Only write pipeline files** — never modify daemon, executor, or scheduler code
- Every pipeline is `async def` with `ctx: PipelineContext`
- Use `ctx.step()` for execution tracing
- Use `ctx.materialize()` for warehouse table management
- Use `ctx.secret("ENV_VAR")` for credentials
- Deploy with `tau deploy`, run with `tau run`
- Read CLAUDE.md in the project root for the full reference
