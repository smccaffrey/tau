# Tau

**AI-native data pipeline orchestration.**

Tau is a standalone daemon with a CLI that AI agents use as a tool — build, schedule, and operate data pipelines from your terminal, whether they run locally or in the cloud.

```bash
pip install tau-pipelines

# Start the daemon
taud

# Deploy a pipeline
tau deploy my_pipeline.py --schedule "0 6 * * *"

# Run it
tau run my_pipeline

# Inspect the results (structured JSON — AI-readable)
tau inspect my_pipeline --last-run
```

## Why Tau?

Current orchestrators (Airflow, Dagster, Prefect) were built for humans to manually define, monitor, and fix every step. AI is bolted on as an afterthought.

Tau is different: **it's a tool that AI uses, not a tool that uses AI.**

- **Standalone daemon** — runs as a background service with built-in scheduler
- **CLI-first** — every operation is a single command an AI agent can call
- **Structured traces** — execution results are JSON, not log soup
- **Local → Cloud** — same commands work whether the daemon is on localhost or a remote server
- **Pure Python pipelines** — no YAML, no DSLs, no config files

## Quick Start

### 1. Install

```bash
pip install tau-pipelines
```

### 2. Start the daemon

```bash
taud
```

This starts the Tau daemon on port 8400 with a SQLite database (zero config).

### 3. Write a pipeline

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

    # Extract
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    ctx.log(f"Extracted {len(data)} records")

    # Transform
    transformed = [{"id": r["id"], "name": r["name"].upper()} for r in data]

    # Validate
    assert len(transformed) > 0
    ctx.log("All checks passed ✓")

    return {"records_processed": len(transformed)}
```

### 4. Deploy and run

```bash
tau deploy my_pipeline.py
tau run hello_world
```

### 5. Inspect the result

```bash
tau inspect hello_world --last-run
```

Returns structured JSON:
```json
{
  "run_id": "abc123",
  "pipeline": "hello_world",
  "status": "success",
  "duration_ms": 42,
  "result": {"records_processed": 2},
  "trace": {
    "steps": [...],
    "log_lines": 3
  }
}
```

## CLI Reference

```bash
# Pipeline lifecycle
tau deploy <file> [--schedule "0 6 * * *"] [--name my_pipeline]
tau undeploy <name>
tau list
tau show <name>
tau code <name>

# Execution
tau run <name> [--params '{"key": "value"}']
tau runs <name> [--last 10]

# Inspection (AI-optimized, returns JSON)
tau inspect <name> --last-run
tau logs <name> [--run <run_id>]
tau errors [--limit 20]

# Scheduling
tau schedule <name> "0 6 * * *"
tau schedule <name> --every 3600
tau schedule <name> --disable

# Daemon
tau status
tau version
taud [--port 8400] [--host 0.0.0.0]
```

## Pipeline API

### PipelineContext

Every pipeline receives a `PipelineContext` with:

```python
ctx.log("message")                    # Structured logging
ctx.extract(source=..., resource=...) # Pull data from a source
ctx.transform(data, steps=[...])      # Transform data
ctx.load(target=..., data=...)        # Push data to a target
ctx.sql("SELECT ...", params={})      # Run SQL in a warehouse
ctx.check(assertion1, assertion2)     # Run assertions
ctx.secret("API_KEY")                 # Access secrets (env vars)
ctx.last_successful_run               # Timestamp of last success
ctx.run_id                            # Current run ID
ctx.params                            # Parameters passed to this run
```

### Step Tracing

Use `ctx.step()` for fine-grained execution traces:

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

Each step is captured in the execution trace with timing, row counts, and errors.

## Local → Cloud

The `tau` CLI talks to the daemon over HTTP. Point it at any Tau instance:

```bash
# Local development
export TAU_HOST=http://localhost:8400
tau deploy pipeline.py && tau run my_pipeline

# Production (same commands, different target)
export TAU_HOST=https://tau.prod.mycompany.com
export TAU_API_KEY=tau_sk_prod_...
tau deploy pipeline.py && tau run my_pipeline
```

Or use environment configs:

```bash
# ~/.config/tau/environments.toml
# [local]
# host = "http://localhost:8400"
#
# [production]
# host = "https://tau.prod.mycompany.com"
# api_key = "tau_sk_prod_..."

tau --env production deploy pipeline.py
```

## AI Agent Integration

Tau is designed to be called by AI agents. Every command returns structured, machine-readable output. Example tool definition:

```json
{
  "name": "tau",
  "description": "Deploy, run, inspect, and manage data pipelines",
  "commands": [
    "tau deploy <file> [--schedule <cron>]",
    "tau run <pipeline>",
    "tau inspect <pipeline> --last-run",
    "tau errors",
    "tau heal <pipeline> --auto"
  ]
}
```

### Self-Healing Flow

```
Pipeline fails at 2am
    → Tau captures structured error trace
    → AI agent sees failure via webhook/poll
    → Agent calls: tau inspect pipeline --last-run
    → Agent diagnoses: schema drift on new column
    → Agent writes fix, calls: tau deploy pipeline.py
    → Next scheduled run recovers
    → Human sleeps through the whole thing
```

## Configuration

Copy `tau.example.toml` to `tau.toml`:

```toml
[daemon]
host = "0.0.0.0"
port = 8400
log_level = "info"

[database]
# SQLite (default, zero-setup)
url = "sqlite+aiosqlite:///tau.db"
# PostgreSQL (production)
# url = "postgresql+asyncpg://tau:tau@localhost:5432/tau"

[auth]
api_key = "your-secret-key"

[scheduler]
timezone = "UTC"
max_concurrent = 10
```

Environment variables override config: `TAU_HOST`, `TAU_API_KEY`, `TAU_DATABASE_URL`, `TAU_PORT`.

## Architecture

```
┌─────────────────────────────────────────────────┐
│              Your Terminal / AI Agent             │
│                                                  │
│  tau deploy → tau run → tau inspect → tau heal    │
└──────────────────────┬──────────────────────────┘
                       │ HTTP
┌──────────────────────▼──────────────────────────┐
│                 taud (daemon)                     │
│                                                  │
│  ┌───────────┐ ┌──────────┐ ┌────────────────┐  │
│  │ FastAPI   │ │Scheduler │ │   Executor     │  │
│  │ API       │ │(cron,    │ │   (runs        │  │
│  │ Server    │ │ interval)│ │    pipelines)  │  │
│  └───────────┘ └──────────┘ └────────────────┘  │
│                                                  │
│  ┌────────────────────────────────────────────┐  │
│  │        Metadata Store (SQLite / PG)        │  │
│  │        runs, traces, schedules             │  │
│  └────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
```

## Roadmap

- [x] **Phase 1** — Daemon, CLI, scheduler, executor, structured traces
- [ ] **Phase 2** — `tau create` (AI generates pipelines from intent), `tau heal --auto` (self-healing), schema drift detection
- [ ] **Phase 3** — Distributed workers, semantic layer, web dashboard, dependency chains
- [ ] **Phase 4** — Tau Cloud (managed hosting), multi-tenant, marketplace

## License

MIT
