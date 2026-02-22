# Tau

**AI-native data pipeline orchestration.**

Tau is a daemon + CLI for building, scheduling, and running data pipelines. Install it, start it, write pipelines — that's it. The only code you write is pipeline files.

```bash
pip install tau-pipelines
taud                                          # start the daemon
tau deploy examples/hello_pipeline.py         # deploy a pipeline
tau run hello_world                           # run it
tau inspect hello_world --last-run            # structured JSON result
```

Think of it like Airflow, but you only write DAGs (pipelines) and everything else is handled.

---

## Why Tau?

Existing orchestrators (Airflow, Dagster, Prefect) require you to manage infrastructure, write boilerplate, and babysit runs. AI integration is an afterthought.

Tau flips this: **it's a tool that AI agents operate.** An AI writes your pipelines, deploys them, monitors runs, and fixes failures — all through a CLI that returns structured JSON.

- **Zero boilerplate** — write pipeline logic, nothing else
- **Daemon architecture** — `taud` runs in the background with a built-in scheduler
- **CLI-first** — every operation is one command, returns machine-readable JSON
- **Local → Cloud** — same `tau` commands work against localhost or a remote instance
- **9 warehouse connectors** — Postgres, BigQuery, Snowflake, MotherDuck, Redshift, ClickHouse, MySQL, HTTP APIs, S3
- **8 materialization strategies** — full refresh, incremental, partitioned, SCD Type 1/2, snapshot, append-only, views
- **Self-healing** — `tau heal --auto` diagnoses failures and applies fixes

---

## Install

```bash
# Core (includes SQLite, HTTP, ClickHouse connectors)
pip install tau-pipelines

# With warehouse connectors
pip install tau-pipelines[postgres]
pip install tau-pipelines[bigquery]
pip install tau-pipelines[snowflake]
pip install tau-pipelines[motherduck]
pip install tau-pipelines[mysql]

# Everything
pip install tau-pipelines[all]
```

Requires Python 3.12+.

---

## Quick Start

### 1. Start the daemon

```bash
taud
```

Starts on port 8400 with a SQLite database. Zero configuration required.

### 2. Write a pipeline

Pipelines are Python files with a `@pipeline` decorator. This is the only code you write:

```python
# my_pipeline.py
from tau import pipeline, PipelineContext

@pipeline(
    name="hello_world",
    description="My first pipeline",
    tags=["example"],
)
async def hello_world(ctx: PipelineContext):
    ctx.log("Hello from Tau! 🚀")

    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    ctx.log(f"Extracted {len(data)} records")

    transformed = [{"id": r["id"], "name": r["name"].upper()} for r in data]
    assert len(transformed) > 0
    ctx.log("All checks passed ✓")

    return {"records_processed": len(transformed)}
```

### 3. Deploy and run

```bash
tau deploy my_pipeline.py
tau run hello_world
tau inspect hello_world --last-run
```

### 4. Schedule it

```bash
tau schedule hello_world "0 6 * * *"    # Daily at 6 AM
tau schedule hello_world --every 3600   # Every hour
```

---

## Pipelines

Pipelines are the only thing you write. Everything else — the daemon, scheduler, executor, metadata store, API — is handled by Tau.

### Pipeline Context

Every pipeline receives a `PipelineContext`:

```python
ctx.log("message")                     # Structured logging
ctx.step("extract")                    # Named execution step (with timing + row counts)
ctx.secret("API_KEY")                  # Access secrets (env vars)
ctx.materialize(config, connector)     # Materialize a table (see below)
ctx.last_successful_run                # Timestamp of last success
ctx.run_id                             # Current run ID
ctx.params                             # Parameters passed to this run
```

### Step Tracing

Wrap pipeline stages in `ctx.step()` for structured execution traces:

```python
@pipeline(name="etl_orders")
async def etl_orders(ctx: PipelineContext):
    async with ctx.step("extract") as step:
        data = await fetch_orders()
        step.rows_out = len(data)

    async with ctx.step("transform") as step:
        step.rows_in = len(data)
        cleaned = transform(data)
        step.rows_out = len(cleaned)

    async with ctx.step("load") as step:
        step.rows_in = len(cleaned)
        await load_to_warehouse(cleaned)
```

Each step is captured in the execution trace with timing, row counts, and errors — all as structured JSON.

---

## Materialization Strategies

Tau has a built-in materialization engine for SQL-capable connectors. Instead of writing raw SQL for table management, declare *what* you want and Tau generates the right SQL for your warehouse dialect.

### Full Refresh

Drop and recreate the table on every run. Best for small dimension tables.

```python
from tau.materializations import FullRefreshConfig

config = FullRefreshConfig(
    target_table="analytics.dim_products",
    source_query="SELECT * FROM raw.products",
    pre_hook="DROP INDEX IF EXISTS idx_sku",
    post_hook="CREATE INDEX idx_sku ON analytics.dim_products(sku)",
)
result = await ctx.materialize(config, connector=warehouse)
```

### Incremental

Merge new/changed rows into an existing table. Supports merge, delete+insert, and insert_overwrite strategies.

```python
from tau.materializations import IncrementalConfig

config = IncrementalConfig(
    target_table="analytics.orders",
    source_query="SELECT * FROM raw.orders",
    unique_key="order_id",
    incremental_column="updated_at",
    incremental_strategy="merge",  # merge | delete+insert | insert_overwrite
)
```

### Partitioned

Partition-aware incremental with native support for BigQuery `PARTITION BY` / `CLUSTER BY` and ClickHouse `MergeTree()`. Auto-expires old partitions.

```python
from tau.materializations import PartitionedConfig

config = PartitionedConfig(
    target_table="analytics.events",
    source_query="SELECT * FROM raw.events",
    unique_key="event_id",
    partition_by="event_date",
    partition_granularity="day",
    cluster_by=["user_id", "event_type"],
    partition_expiration_days=90,
)
```

### SCD Type 1

Overwrite changed values in place. Tracks `updated_at` automatically.

```python
from tau.materializations import SCDType1Config

config = SCDType1Config(
    target_table="analytics.dim_products",
    source_query="SELECT * FROM raw.products",
    unique_key="product_id",
    tracked_columns=["name", "price", "category"],
)
```

### SCD Type 2

Full history tracking with `valid_from`/`valid_to`/`is_current`. Hash-based change detection. Handles hard deletes.

```python
from tau.materializations import SCDType2Config

config = SCDType2Config(
    target_table="analytics.dim_customers",
    source_query="SELECT customer_id, name, email, tier FROM raw.customers",
    unique_key="customer_id",
    tracked_columns=["name", "email", "tier"],
    invalidate_hard_deletes=True,
)
```

Query the result:
```sql
-- Current state
SELECT * FROM analytics.dim_customers WHERE is_current = TRUE

-- Point-in-time
SELECT * FROM analytics.dim_customers
WHERE valid_from <= '2026-01-15' AND (valid_to IS NULL OR valid_to > '2026-01-15')
```

### Snapshot

Append a full copy of the data with a timestamp on each run. Optional retention pruning.

```python
from tau.materializations import SnapshotConfig

config = SnapshotConfig(
    target_table="analytics.balance_snapshots",
    source_query="SELECT * FROM raw.accounts WHERE is_active = true",
    retain_snapshots=30,  # Keep last 30 snapshots, prune older
)
```

### Append Only

Insert-only, no updates. For immutable event logs and audit trails.

```python
from tau.materializations import MaterializationConfig, MaterializationType

config = MaterializationConfig(
    target_table="raw.api_logs",
    source_query="SELECT * FROM staging.api_logs_buffer",
    strategy=MaterializationType.APPEND_ONLY,
)
```

### View

Create or replace a SQL view. Zero storage — just a query alias.

```python
config = MaterializationConfig(
    target_table="reporting.v_active_users",
    source_query="SELECT * FROM analytics.dim_customers WHERE is_current = TRUE",
    strategy=MaterializationType.VIEW,
)
```

---

## Connectors

Connect to any data source or warehouse. All connectors follow the same interface:

```python
async with connector:
    data = await connector.extract(query="SELECT * FROM users")
    await connector.load(data, table="target_table")
    await connector.execute("CREATE INDEX ...")
```

| Connector | Driver | Install |
|-----------|--------|---------|
| PostgreSQL | asyncpg | `pip install tau-pipelines[postgres]` |
| BigQuery | google-cloud-bigquery | `pip install tau-pipelines[bigquery]` |
| Snowflake | snowflake-connector-python | `pip install tau-pipelines[snowflake]` |
| MotherDuck | duckdb | `pip install tau-pipelines[motherduck]` |
| DuckDB (local) | duckdb | `pip install tau-pipelines[motherduck]` |
| Redshift | asyncpg | `pip install tau-pipelines[redshift]` |
| ClickHouse | httpx (built-in) | included |
| MySQL | aiomysql | `pip install tau-pipelines[mysql]` |
| HTTP API | httpx (built-in) | included |
| S3 | boto3 | `pip install tau-pipelines[all]` |

### Example: API → Warehouse

```python
from tau import pipeline, PipelineContext
from tau.connectors.http_api import http_api
from tau.connectors.postgres import postgres

@pipeline(name="api_to_warehouse")
async def api_to_warehouse(ctx: PipelineContext):
    source = http_api(base_url="https://api.example.com")
    target = postgres(dsn=ctx.secret("WAREHOUSE_DSN"))

    async with source, target:
        users = await source.extract(endpoint="/v1/users")
        await target.load(users, table="raw.users", mode="upsert", merge_key="user_id")
```

---

## CLI Reference

```bash
# Pipeline lifecycle
tau deploy <file> [--schedule "cron"] [--name override]
tau undeploy <name>
tau list
tau show <name>
tau code <name>

# Execution
tau run <name> [--params '{"key": "value"}']
tau runs <name> [--last 10]

# Inspection (returns structured JSON)
tau inspect <name> --last-run
tau logs <name> [--run <run_id>]
tau errors [--limit 20]

# Scheduling
tau schedule <name> "0 6 * * *"
tau schedule <name> --every 3600
tau schedule <name> --disable

# AI assistance
tau create "load Stripe payments into BigQuery daily"
tau heal <name> [--auto]

# Daemon
tau status
tau version
taud [--port 8400] [--host 0.0.0.0]
```

---

## Local → Cloud

The CLI talks to the daemon over HTTP. Point it at any Tau instance:

```bash
# Local development (default)
tau deploy pipeline.py && tau run my_pipeline

# Production
export TAU_HOST=https://tau.mycompany.com
export TAU_API_KEY=tau_sk_prod_...
tau deploy pipeline.py && tau run my_pipeline
```

Same commands, same pipeline files, different target.

---

## AI Agent Integration

Tau is designed to be called by AI coding agents (Claude, GPT, Cursor, etc.). The workflow:

1. **Human describes what they want** — "load Stripe payments into our warehouse daily"
2. **AI writes a pipeline file** — using `@pipeline`, connectors, and materializations
3. **AI deploys it** — `tau deploy stripe_payments.py --schedule "0 6 * * *"`
4. **AI monitors it** — `tau inspect stripe_payments --last-run`
5. **AI fixes failures** — `tau heal stripe_payments --auto`

The AI never needs to touch daemon code, scheduler config, or infrastructure. It only writes pipeline files and uses the CLI.

### Self-Healing

```
Pipeline fails at 2am
  → Tau captures structured error trace
  → AI agent sees failure via webhook or poll
  → Agent calls: tau inspect pipeline --last-run
  → Agent diagnoses: schema drift on a new column
  → Agent writes fix, calls: tau deploy pipeline.py
  → Next scheduled run succeeds
  → Human sleeps through the whole thing
```

---

## Configuration

Tau works with zero configuration (SQLite + localhost). For production:

```toml
# tau.toml
[daemon]
host = "0.0.0.0"
port = 8400

[database]
url = "postgresql+asyncpg://tau:tau@localhost:5432/tau"

[auth]
api_key = "tau_sk_..."

[scheduler]
timezone = "UTC"
max_concurrent = 10
```

Environment variables: `TAU_HOST`, `TAU_API_KEY`, `TAU_DATABASE_URL`, `TAU_PORT`.

---

## Architecture

```
┌──────────────────────────────────────────────┐
│         Terminal / AI Agent / CI/CD           │
│                                              │
│  tau deploy → tau run → tau inspect → heal   │
└─────────────────────┬────────────────────────┘
                      │ HTTP
┌─────────────────────▼────────────────────────┐
│               taud (daemon)                   │
│                                              │
│  ┌──────────┐ ┌──────────┐ ┌──────────────┐ │
│  │ FastAPI  │ │Scheduler │ │  Executor    │ │
│  │ API      │ │(APSched) │ │  (pipelines) │ │
│  └──────────┘ └──────────┘ └──────────────┘ │
│                                              │
│  ┌──────────────────────────────────────────┐│
│  │     Metadata Store (SQLite / Postgres)   ││
│  └──────────────────────────────────────────┘│
└──────────────────────────────────────────────┘
```

Pipelines are the only user-authored code. Everything else is Tau infrastructure.

---

## Examples

See the [`examples/`](examples/) directory:

| Example | Strategy | Description |
|---------|----------|-------------|
| [`hello_pipeline.py`](examples/hello_pipeline.py) | — | Simplest possible pipeline |
| [`full_refresh_products.py`](examples/full_refresh_products.py) | Full Refresh | Rebuild a dimension table |
| [`incremental_orders.py`](examples/incremental_orders.py) | Incremental | Merge new/changed orders |
| [`partitioned_events.py`](examples/partitioned_events.py) | Partitioned | Date-partitioned with auto-expiry |
| [`scd1_products.py`](examples/scd1_products.py) | SCD Type 1 | Overwrite changed values |
| [`scd2_customers.py`](examples/scd2_customers.py) | SCD Type 2 | Track full change history |
| [`snapshot_balances.py`](examples/snapshot_balances.py) | Snapshot | Point-in-time with retention |
| [`append_only_logs.py`](examples/append_only_logs.py) | Append Only | Immutable event log |
| [`view_active_users.py`](examples/view_active_users.py) | View | Reporting view |
| [`warehouse_etl.py`](examples/warehouse_etl.py) | Multiple | Full warehouse ETL pipeline |
| [`api_to_warehouse.py`](examples/api_to_warehouse.py) | — | REST API → PostgreSQL |
| [`scheduled_pipeline.py`](examples/scheduled_pipeline.py) | — | Pipeline with built-in schedule |
| [`failing_pipeline.py`](examples/failing_pipeline.py) | — | Error handling demo |

---

## Roadmap

- [x] Core daemon, CLI, scheduler, executor, structured traces
- [x] `tau create` (AI generates pipelines), `tau heal --auto` (self-healing)
- [x] 9 warehouse connectors
- [x] 8 materialization strategies with dialect-aware SQL
- [ ] Distributed workers, dependency DAGs, web dashboard
- [ ] Tau Cloud (managed hosting, multi-tenant)

---

## Contributing

```bash
git clone https://github.com/smccaffrey/tau.git
cd tau
uv sync --all-extras
uv run pytest
```

---

## License

MIT
