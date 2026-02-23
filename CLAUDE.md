# CLAUDE.md — Tau Pipelines

## What This Is

Tau is an embedded data orchestrator designed for you to operate. It runs as a daemon (`taud`) on the user's machine or in the cloud. You write pipeline files, deploy them through the CLI, and manage the entire lifecycle — scheduling, monitoring, error recovery — without human intervention.

**You are the operator.** The CLI returns structured JSON. Every command is deterministic. The only code you author is pipeline files.

## Your Workflow

```bash
# The daemon is already running (installed via `curl | bash` or `pip install tau-pipelines && taud`)

# You write a pipeline file, then:
tau deploy my_pipeline.py --schedule "0 6 * * *"    # Deploy it
tau run my_pipeline                                  # Run it
tau inspect my_pipeline --last-run                   # Read structured result
tau errors                                           # Check for failures
tau heal my_pipeline --auto                          # Diagnose + fix
```

When running locally, Tau is your embedded data engineer — lightweight, zero-config, SQLite-backed. When `TAU_HOST` points to a remote instance, the same commands operate a full production platform with PostgreSQL, distributed workers, and authentication. You don't change anything.

## Writing Pipelines

Every pipeline is a single Python file with a `@pipeline` decorator:

```python
from tau import pipeline, PipelineContext

@pipeline(
    name="my_pipeline",
    description="What it does",
    schedule="0 6 * * *",       # Optional: cron schedule
    tags=["warehouse", "etl"],  # Optional: tags
)
async def my_pipeline(ctx: PipelineContext):
    # Your logic here
    ctx.log("message")
    return {"result": "value"}
```

### PipelineContext API

```python
ctx.log("message")                          # Structured log line
ctx.step("name")                            # Named step (async with ctx.step("extract") as step:)
ctx.secret("ENV_VAR")                       # Read from environment
ctx.materialize(config, connector, dialect) # Materialize a table
ctx.last_successful_run                     # Datetime of last success (or None)
ctx.run_id                                  # Current run ID
ctx.params                                  # Dict of params passed to this run
```

### Step Tracing

Always wrap logical stages in `ctx.step()`:

```python
async with ctx.step("extract") as step:
    data = await source.extract(query="SELECT * FROM users")
    step.rows_out = len(data)

async with ctx.step("load") as step:
    step.rows_in = len(data)
    await target.load(data, table="users")
```

## Connectors

Use connectors for data sources/targets. Import the factory function, configure, use as async context manager:

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

# All connectors follow the same pattern:
async with postgres(dsn="postgresql://...") as db:
    data = await db.extract(query="SELECT * FROM users")
    await db.load(data, table="target", mode="upsert", merge_key="id")
    await db.execute("CREATE INDEX ...")
```

### Connector methods:
- `extract(query, params)` → `list[dict]`
- `load(data, table, mode, merge_key)` → `int` (rows loaded)
- `execute(query, params)` → result
- `get_schema(table)` → column info

### Load modes:
- `"append"` — INSERT (default)
- `"replace"` — TRUNCATE + INSERT
- `"upsert"` — INSERT ... ON CONFLICT DO UPDATE (requires `merge_key`)

## Materializations

For SQL-capable connectors, use the materialization engine instead of raw SQL:

```python
from tau.materializations import (
    FullRefreshConfig,       # DROP + CREATE AS
    IncrementalConfig,       # MERGE / delete+insert / insert_overwrite
    PartitionedConfig,       # Partition-aware incremental
    SCDType1Config,          # Overwrite changed values
    SCDType2Config,          # Track full history (valid_from/valid_to)
    SnapshotConfig,          # Point-in-time copies with retention
    MaterializationConfig,   # Base (for APPEND_ONLY and VIEW)
    MaterializationType,     # Enum of all strategies
)

result = await ctx.materialize(config, connector=db, dialect="postgres")
```

### Full Refresh
```python
FullRefreshConfig(
    target_table="analytics.dim_products",
    source_query="SELECT * FROM raw.products",
    pre_hook="DROP INDEX IF EXISTS idx_sku",      # Optional
    post_hook="CREATE INDEX idx_sku ON ...(sku)",  # Optional
)
```

### Incremental
```python
IncrementalConfig(
    target_table="analytics.orders",
    source_query="SELECT * FROM raw.orders",
    unique_key="order_id",                         # Required for merge
    incremental_column="updated_at",               # Filter new rows
    incremental_strategy="merge",                  # merge | delete+insert | insert_overwrite
    on_schema_change="append_new_columns",         # ignore | fail | append_new_columns
)
```

### Partitioned
```python
PartitionedConfig(
    target_table="analytics.events",
    source_query="SELECT * FROM raw.events",
    unique_key="event_id",
    partition_by="event_date",
    partition_type="date",                         # date | range | list
    partition_granularity="day",                   # hour | day | month | year
    cluster_by=["user_id", "event_type"],         # BigQuery/Snowflake clustering
    incremental_column="created_at",
    partition_expiration_days=90,                   # Auto-delete old partitions
)
```

### SCD Type 1
```python
SCDType1Config(
    target_table="analytics.dim_products",
    source_query="SELECT * FROM raw.products",
    unique_key="product_id",
    tracked_columns=["name", "price", "category"], # Which columns trigger an update
    updated_at_column="updated_at",                # Auto-set on change
)
```

### SCD Type 2
```python
SCDType2Config(
    target_table="analytics.dim_customers",
    source_query="SELECT customer_id, name, tier FROM raw.customers",
    unique_key="customer_id",
    tracked_columns=["name", "tier"],              # Changes trigger new version
    valid_from_column="valid_from",
    valid_to_column="valid_to",
    is_current_column="is_current",
    hash_column="row_hash",
    invalidate_hard_deletes=True,                  # Close records that disappear
)
```

### Snapshot
```python
SnapshotConfig(
    target_table="analytics.balance_snapshots",
    source_query="SELECT * FROM raw.accounts",
    snapshot_timestamp_column="snapshot_at",
    snapshot_id_column="snapshot_id",
    retain_snapshots=30,                           # Prune older snapshots
)
```

### Append Only / View
```python
MaterializationConfig(
    target_table="raw.api_logs",
    source_query="SELECT * FROM staging.buffer",
    strategy=MaterializationType.APPEND_ONLY,
)

MaterializationConfig(
    target_table="reporting.v_active_users",
    source_query="SELECT * FROM ... WHERE ...",
    strategy=MaterializationType.VIEW,
)
```

### Dialects
Pass `dialect=` to `ctx.materialize()` to get warehouse-native SQL:
- `"postgres"` (default) — standard SQL
- `"bigquery"` — `PARTITION BY DATE_TRUNC(...)`, `CLUSTER BY`, `CURRENT_TIMESTAMP()`
- `"snowflake"` — MD5 hashing, standard SQL
- `"clickhouse"` — `MergeTree()` engine, `toString()`, `concat()`
- `"mysql"` — standard SQL
- `"duckdb"` — standard SQL

## Pipeline Dependencies

Declare dependencies with `depends_on`:

```python
@pipeline(name="fct_orders", depends_on=["stage_orders", "dim_customers"])
async def fct_orders(ctx: PipelineContext): ...
```

Or set via CLI:
```bash
tau depends fct_orders --on stage_orders --on dim_customers
tau dag  # View the full DAG
```

Tau resolves the graph into parallel execution groups. Pipelines with no unmet dependencies run concurrently. If upstream fails, downstream is skipped.

## Workers

Single machine (default): pipelines run in the daemon process with concurrency control.

Distributed: register remote `taud` instances as workers. The coordinator dispatches to the least-loaded worker.

```bash
tau workers  # View worker pool status
```

## CLI Commands You Use

```bash
# Deploy a pipeline file to the daemon
tau deploy my_pipeline.py [--schedule "0 6 * * *"] [--name override]

# Run a pipeline
tau run <name> [--params '{"key": "value"}']

# Inspect last run (structured JSON — read this to diagnose issues)
tau inspect <name> --last-run

# List all pipelines
tau list

# View pipeline source code
tau code <name>

# See recent failures
tau errors [--limit 20]

# View run logs
tau logs <name> [--run <run_id>]

# List runs
tau runs <name> [--last 10]

# Schedule management
tau schedule <name> "0 6 * * *"
tau schedule <name> --every 3600
tau schedule <name> --disable

# AI-powered
tau create "description of what you want"   # Generate pipeline from intent
tau heal <name> [--auto]                    # Diagnose + fix failures

# DAG
tau dag                                     # View the pipeline DAG
tau depends <name>                          # Show dependencies
tau depends <name> --on dep1 --on dep2      # Set dependencies

# Workers
tau workers                                 # Worker pool status

# Dashboard
tau dashboard                               # Open web dashboard in browser

# Daemon info
tau status
tau version
```

## Project Structure

```
src/tau/
├── __init__.py              # Exports: pipeline, PipelineContext
├── cli/main.py              # Typer CLI (tau command)
├── daemon/
│   ├── app.py               # FastAPI daemon (taud)
│   ├── scheduler.py         # APScheduler
│   └── executor.py          # Pipeline runner
├── pipeline/
│   ├── context.py           # PipelineContext
│   ├── decorators.py        # @pipeline decorator
│   └── loader.py            # Pipeline file loader
├── connectors/              # All data connectors
│   ├── base.py              # Connector ABC
│   ├── postgres.py, bigquery.py, snowflake.py, ...
├── materializations/        # Table materialization engine
│   ├── strategies.py        # Config dataclasses
│   └── engine.py            # SQL generation + execution
├── dag/                     # Pipeline dependency DAG
│   ├── resolver.py          # Topological sort, cycle detection, parallel groups
│   └── runner.py            # DAG execution with parallelism
├── workers/                 # Worker pool (local + distributed)
│   ├── pool.py              # WorkerPool dispatch
│   ├── local.py             # In-process worker
│   └── remote.py            # Remote taud worker
├── dashboard/               # Web dashboard (served by daemon)
│   └── app.py               # Single-page HTML dashboard
├── api/                     # REST API endpoints
├── services/                # Business logic
├── repositories/            # Data access
├── models/                  # SQLAlchemy models
└── schemas/                 # Pydantic DTOs
```

**Do not modify** anything outside of pipeline files unless explicitly asked to work on Tau's internals.

## Stack
- Python 3.12+, uv (NOT pip/poetry)
- FastAPI (daemon), Typer (CLI)
- SQLAlchemy async + SQLite (dev) / PostgreSQL (prod)
- APScheduler, Pydantic, httpx

## Testing

```bash
uv run pytest                    # All tests
uv run pytest tests/ -v          # Verbose
uv run pytest tests/test_materializations.py  # Specific file
```

## Common Patterns

### Full warehouse ETL (multiple materializations in one pipeline):
```python
@pipeline(name="warehouse_etl", schedule="0 4 * * *")
async def warehouse_etl(ctx: PipelineContext):
    db = postgres(dsn=ctx.secret("WAREHOUSE_DSN"))
    async with db:
        await ctx.materialize(FullRefreshConfig(...), connector=db)
        await ctx.materialize(SCDType2Config(...), connector=db)
        await ctx.materialize(IncrementalConfig(...), connector=db)
        await ctx.materialize(MaterializationConfig(..., strategy=MaterializationType.VIEW), connector=db)
```

### API → Warehouse:
```python
@pipeline(name="api_sync")
async def api_sync(ctx: PipelineContext):
    api = http_api(base_url="https://api.example.com")
    db = postgres(dsn=ctx.secret("WAREHOUSE_DSN"))
    async with api, db:
        data = await api.extract(endpoint="/users")
        await db.load(data, table="raw.users", mode="upsert", merge_key="id")
```

### Cross-warehouse sync:
```python
@pipeline(name="bq_to_pg")
async def bq_to_pg(ctx: PipelineContext):
    bq = bigquery(project="my-project", credentials_path="...")
    pg = postgres(dsn=ctx.secret("PG_DSN"))
    async with bq, pg:
        data = await bq.extract(query="SELECT * FROM dataset.users")
        await pg.load(data, table="users", mode="upsert", merge_key="user_id")
```
