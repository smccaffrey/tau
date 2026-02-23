# Tau

**An embedded data orchestrator for AI systems.**

Tau is a lightweight daemon that gives any AI agent — coding assistants, autonomous agents, CI/CD pipelines — the ability to build, deploy, and operate data pipelines. Locally, it's an embedded orchestrator running on your laptop. In production, it's a full-blown pipeline platform.

```bash
curl -fsSL https://raw.githubusercontent.com/smccaffrey/tau/main/install.sh | bash
```

One command. The daemon starts, the CLI is ready, and your AI can immediately begin orchestrating data.

```bash
tau deploy pipeline.py        # AI deploys a pipeline
tau run my_pipeline           # AI triggers execution
tau inspect my_pipeline       # AI reads structured JSON result
tau heal my_pipeline --auto   # AI diagnoses and fixes failures
```

---

## What Makes Tau Different

Every existing orchestrator — Airflow, Dagster, Prefect — was built for humans to configure and operate. AI integration is bolted on.

Tau is built the other way around. **The AI is the operator.**

- **Every CLI command returns structured JSON** — no parsing HTML dashboards or log files
- **The entire interface is text in, text out** — perfect for LLM tool use
- **Pipelines are the only authored code** — the AI writes a Python file and deploys it. That's the whole workflow.
- **Self-healing is native** — `tau heal --auto` gives the AI a structured error trace it can reason about and fix
- **Local and cloud are the same interface** — `TAU_HOST=localhost` on your laptop, `TAU_HOST=https://tau.company.com` in production. Same commands, same pipeline files.

### On Your Laptop

Tau runs as a background daemon (`taud`) with zero configuration. SQLite for metadata, local executor, built-in scheduler. It's like having a data engineering team embedded in your terminal — your AI writes pipelines, Tau runs them, and you get results.

```bash
taud                          # Starts in the background, port 8400
tau deploy my_etl.py          # AI deploys
tau run my_etl                # Runs locally
```

### In Production

Point at a remote instance. Same CLI, same pipelines, full platform — PostgreSQL metadata, distributed workers, API authentication, web dashboard.

```bash
export TAU_HOST=https://tau.mycompany.com
export TAU_API_KEY=tau_sk_prod_...
tau deploy my_etl.py          # Deploys to production
tau run my_etl                # Runs on remote workers
```

---

## Install

### One-line setup (recommended)

```bash
curl -fsSL https://raw.githubusercontent.com/smccaffrey/tau/main/install.sh | bash
```

Installs Tau via `uv`, generates an API key, starts the daemon, and sets up Claude Code integration. Ready in 30 seconds.

### Manual install

```bash
pip install tau-pipelines        # Core
pip install tau-pipelines[all]   # All warehouse connectors

taud                             # Start the daemon
```

Connector extras: `[postgres]`, `[bigquery]`, `[snowflake]`, `[motherduck]`, `[mysql]`.

Requires Python 3.12+.

---

## Quick Start

### 1. Start the daemon

```bash
taud
```

Zero configuration. SQLite database, local executor, port 8400.

### 2. Write a pipeline

Pipelines are Python files with a `@pipeline` decorator. This is the only code anyone — human or AI — writes:

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

## The AI Workflow

This is how Tau is meant to be used. A human says what they want. An AI does the rest.

```
Human: "Sync our Stripe payments into the warehouse daily"

AI:
  1. Writes stripe_payments.py with @pipeline decorator
  2. tau deploy stripe_payments.py --schedule "0 6 * * *"
  3. Monitors: tau inspect stripe_payments --last-run

Pipeline fails at 2am:
  → Tau captures structured error trace (JSON)
  → AI reads it: tau inspect stripe_payments --last-run
  → AI diagnoses: schema drift, new column in source
  → AI writes fix, redeploys: tau deploy stripe_payments.py
  → Next run succeeds
  → Human sleeps through the whole thing
```

The AI never touches daemon code, scheduler config, or infrastructure. It writes pipeline files and talks to the CLI. That's the entire surface area.

---

## Pipelines

Pipelines are the only authored code in the system. Everything else — the daemon, scheduler, executor, metadata store, API — is Tau infrastructure.

### Pipeline Context

Every pipeline receives a `PipelineContext`:

```python
ctx.log("message")                     # Structured logging
ctx.step("extract")                    # Named execution step (with timing + row counts)
ctx.secret("API_KEY")                  # Access secrets (env vars)
ctx.materialize(config, connector)     # Materialize a table
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

Every step is captured as structured JSON — timing, row counts, errors — readable by any AI system.

---

## Materialization Strategies

Tau has a built-in materialization engine for SQL-capable connectors. Declare *what* you want and Tau generates the right SQL for your warehouse dialect.

### Full Refresh

Drop and recreate the table on every run.

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

Merge new/changed rows. Supports merge, delete+insert, and insert_overwrite.

```python
from tau.materializations import IncrementalConfig

config = IncrementalConfig(
    target_table="analytics.orders",
    source_query="SELECT * FROM raw.orders",
    unique_key="order_id",
    incremental_column="updated_at",
    incremental_strategy="merge",
)
```

### Partitioned

Partition-aware incremental with native BigQuery `PARTITION BY` / `CLUSTER BY` and ClickHouse `MergeTree()`.

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

Overwrite changed values in place.

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

Full history tracking with `valid_from`/`valid_to`/`is_current`. Hash-based change detection.

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

### Snapshot

Point-in-time copy with optional retention pruning.

```python
from tau.materializations import SnapshotConfig

config = SnapshotConfig(
    target_table="analytics.balance_snapshots",
    source_query="SELECT * FROM raw.accounts WHERE is_active = true",
    retain_snapshots=30,
)
```

### Append Only

Insert-only. For immutable event logs and audit trails.

```python
from tau.materializations import MaterializationConfig, MaterializationType

config = MaterializationConfig(
    target_table="raw.api_logs",
    source_query="SELECT * FROM staging.api_logs_buffer",
    strategy=MaterializationType.APPEND_ONLY,
)
```

### View

SQL view. Zero storage.

```python
config = MaterializationConfig(
    target_table="reporting.v_active_users",
    source_query="SELECT * FROM analytics.dim_customers WHERE is_current = TRUE",
    strategy=MaterializationType.VIEW,
)
```

---

## Connectors

Connect to any data source or warehouse. Two approaches: named connections from `tau.toml` (recommended) or inline connectors.

### Named Connections (Recommended)

Configure connections in `tau.toml` and access them via `ctx.connection()`:

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
@pipeline(name="api_sync")
async def api_sync(ctx: PipelineContext):
    api = await ctx.connection("api")
    warehouse = await ctx.connection("warehouse")

    data = await api.extract(endpoint="/users")
    await warehouse.load(data, table="raw.users", mode="upsert", merge_key="user_id")
```

### Inline Connectors

Traditional approach — import and configure connectors directly in pipeline code:

```python
from tau.connectors import postgres, http_api

@pipeline(name="inline_sync")
async def inline_sync(ctx: PipelineContext):
    api = http_api(base_url="https://api.example.com")
    warehouse = postgres(dsn=ctx.secret("WAREHOUSE_DSN"))

    async with api, warehouse:
        data = await api.extract(endpoint="/users")
        await warehouse.load(data, table="raw.users", mode="upsert", merge_key="user_id")
```

### Uniform Interface

All connectors support the same methods:

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

Using named connections from `tau.toml`:

```python
from tau import pipeline, PipelineContext

@pipeline(name="api_to_warehouse")
async def api_to_warehouse(ctx: PipelineContext):
    source = await ctx.connection("api")
    target = await ctx.connection("warehouse")

    users = await source.extract(endpoint="/v1/users")
    await target.load(users, table="raw.users", mode="upsert", merge_key="user_id")
```

Or with inline connectors:

```python
from tau import pipeline, PipelineContext
from tau.connectors import http_api, postgres

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

Every command returns structured JSON. Every command is one line. Designed for AI tool use.

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

# Inspection (structured JSON — the AI reads this)
tau inspect <name> --last-run
tau logs <name> [--run <run_id>]
tau errors [--limit 20]

# Scheduling
tau schedule <name> "0 6 * * *"
tau schedule <name> --every 3600
tau schedule <name> --disable

# AI-powered
tau create "load Stripe payments into BigQuery daily"
tau heal <name> [--auto]

# DAG
tau dag
tau depends <name>
tau depends <name> --on dep1 --on dep2

# Connections
tau connections
tau connections --test <name>

# Workers
tau workers

# Dashboard
tau dashboard

# Daemon
tau status
tau version
taud [--port 8400] [--host 0.0.0.0]
```

---

## Pipeline Dependencies (DAGs)

Declare dependencies with `depends_on`. Tau resolves the graph, runs independent pipelines in parallel, and skips downstream when upstream fails.

```python
@pipeline(name="extract_orders", schedule="0 4 * * *")
async def extract_orders(ctx): ...

@pipeline(name="dim_customers", depends_on=["extract_customers"])
async def dim_customers(ctx): ...

@pipeline(name="fct_orders", depends_on=["stage_orders", "dim_customers", "dim_products"])
async def fct_orders(ctx): ...
```

```bash
tau dag                                    # View the DAG
tau depends fct_orders                     # Show dependencies
```

---

## Workers

### Laptop (default)

Single machine, background daemon, local executor. Your AI's embedded data engineer.

```bash
taud                    # Starts with local worker pool
```

### Production (distributed)

Multiple `taud` instances, coordinated dispatch, least-loaded routing.

```bash
# Coordinator
taud --port 8400

# Worker machines
taud --port 8400

# Register
curl -X POST http://coordinator:8400/api/v1/workers/register \
  -H "Authorization: Bearer $TAU_API_KEY" \
  -d '{"worker_id":"worker-1","host":"http://w1:8400","api_key":"key","max_concurrent":4}'
```

Same interface at every scale.

---

## Architecture

```
┌──────────────────────────────────────────────┐
│          AI Agent / CLI / CI/CD              │
│                                              │
│  tau deploy → tau run → tau inspect → heal   │
└─────────────────────┬────────────────────────┘
                      │ HTTP (JSON)
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

On a laptop: everything runs in one process. In production: distributed across workers. The AI doesn't know or care which — the interface is the same.

---

## Configuration

Zero configuration by default. For production:

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

# Named connections
[connections.warehouse]
type = "postgres"
dsn = "${WAREHOUSE_DSN}"

[connections.api]
type = "http_api"
base_url = "https://api.example.com"
headers = { "Authorization" = "Bearer ${API_KEY}" }

[connections.lake]
type = "bigquery"
project = "${BIGQUERY_PROJECT}"
credentials_path = "${GOOGLE_APPLICATION_CREDENTIALS}"
```

Environment variables: `TAU_HOST`, `TAU_API_KEY`, `TAU_DATABASE_URL`, `TAU_PORT`.

---

## Examples

See [`examples/`](examples/):

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

---

## Roadmap

- [x] Core daemon, CLI, scheduler, executor, structured traces
- [x] AI-powered pipeline generation and self-healing
- [x] 9 warehouse connectors
- [x] 8 materialization strategies with dialect-aware SQL
- [x] Pipeline DAGs with dependency resolution and parallel execution
- [x] Distributed workers (local + remote)
- [x] Web dashboard
- [ ] **Tau Cloud** — managed hosting, multi-tenant, deploy from anywhere

---

## Contributing

```bash
git clone https://github.com/smccaffrey/tau.git
cd tau
uv sync --python ">=3.12" --all-extras
uv run pytest
```

---

## License

MIT
