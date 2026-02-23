---
description: Write, deploy, and operate Tau data pipelines
---

Tau is an embedded data orchestrator built for AI systems. You are the operator. On a laptop, it's a lightweight daemon running in the background — your embedded data engineer. In production, it's a full platform with distributed workers. Same interface either way.

**You write pipeline files. You deploy them. You monitor and fix them. The CLI returns structured JSON. That's the entire surface area.**

## Your Workflow

```bash
tau deploy my_pipeline.py                # Deploy
tau run my_pipeline                      # Execute
tau inspect my_pipeline --last-run       # Read structured JSON result
tau heal my_pipeline --auto              # Diagnose + fix failures
```

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
    # Use ctx.step() for execution tracing (structured JSON output)
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

**Preferred: Named Connections**
Configure in `tau.toml` and access via `ctx.connection()`:

```toml
# tau.toml
[connections.warehouse]
type = "postgres"
dsn = "${WAREHOUSE_DSN}"

[connections.api]
type = "http_api"
base_url = "https://api.example.com"
headers = { "Authorization" = "Bearer ${API_KEY}" }
```

```python
@pipeline(name="sync")
async def sync(ctx: PipelineContext):
    api = await ctx.connection("api")
    warehouse = await ctx.connection("warehouse")

    data = await api.extract(endpoint="/users")
    await warehouse.load(data, table="raw.users", mode="upsert", merge_key="id")
```

**Alternative: Inline Connectors**

```python
from tau.connectors import postgres, bigquery, snowflake, motherduck, duckdb_local, redshift, clickhouse, mysql, http_api, s3

# Uniform interface — extract, load, execute
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

## CLI Reference

Every command returns structured JSON. Every command is one line.

```bash
tau deploy <file> [--schedule "cron"]   # Deploy pipeline
tau run <name>                          # Run pipeline
tau inspect <name> --last-run           # Structured JSON result (read this)
tau list                                # List pipelines
tau errors                              # Recent failures
tau heal <name> [--auto]                # AI diagnosis + fix
tau connections                         # List configured connections
tau connections --test <name>           # Test connection
tau dag                                 # View dependency graph
tau depends <name> --on dep1 --on dep2  # Set dependencies
tau workers                             # Worker pool status
tau create "description"                # Generate pipeline from intent
tau schedule <name> "0 6 * * *"         # Set schedule
```

## Local vs Production

```bash
# Local — embedded orchestrator, zero config, SQLite
taud                                    # Just works

# Production — same commands, remote target
export TAU_HOST=https://tau.company.com
export TAU_API_KEY=tau_sk_prod_...
tau deploy pipeline.py                  # Deploys to production
```

## Rules

- **Only write pipeline files** — never modify daemon, executor, or scheduler code
- Every pipeline is `async def` with `ctx: PipelineContext`
- Use `ctx.step()` for execution tracing
- Use `ctx.materialize()` for warehouse table management
- Use `ctx.secret("ENV_VAR")` for credentials
- Deploy with `tau deploy`, run with `tau run`
- Read CLAUDE.md in the project root for the full reference
