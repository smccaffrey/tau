# CLAUDE.md — Tau Pipelines

## What This Is
Tau Pipelines is an AI-native data orchestration framework. A standalone daemon (`taud`) with a CLI (`tau`) that AI agents use as a tool to build, schedule, and operate data pipelines.

## Stack
- Python 3.12+
- **uv** for dependency management (NOT pip, NOT poetry)
- FastAPI for the daemon API server
- Typer for the CLI
- SQLAlchemy (async) + Alembic for PostgreSQL metadata store
- APScheduler for the built-in scheduler
- Pydantic for all schemas/models

## Project Structure
```
tau-pipelines/
├── pyproject.toml              # uv project config
├── alembic.ini
├── alembic/
│   └── versions/
├── src/
│   └── tau/
│       ├── __init__.py         # version, public API
│       ├── cli/
│       │   ├── __init__.py
│       │   └── main.py         # Typer CLI app (tau command)
│       ├── daemon/
│       │   ├── __init__.py
│       │   ├── app.py          # FastAPI app (taud)
│       │   ├── scheduler.py    # APScheduler integration
│       │   └── executor.py     # Pipeline execution engine
│       ├── core/
│       │   ├── __init__.py
│       │   ├── config.py       # Settings (from tau.toml + env vars)
│       │   ├── database.py     # Async SQLAlchemy engine/session
│       │   └── auth.py         # API key auth
│       ├── models/
│       │   ├── __init__.py
│       │   ├── pipeline.py     # Pipeline SQLAlchemy model
│       │   ├── run.py          # PipelineRun model
│       │   └── schedule.py     # Schedule model
│       ├── schemas/
│       │   ├── __init__.py
│       │   ├── pipeline.py     # Pydantic schemas
│       │   ├── run.py
│       │   └── schedule.py
│       ├── api/
│       │   ├── __init__.py
│       │   ├── router.py       # Main API router
│       │   ├── pipelines.py    # Pipeline CRUD endpoints
│       │   ├── runs.py         # Run endpoints
│       │   └── schedules.py    # Schedule endpoints
│       ├── services/
│       │   ├── __init__.py
│       │   ├── pipeline_service.py
│       │   ├── run_service.py
│       │   └── schedule_service.py
│       ├── repositories/
│       │   ├── __init__.py
│       │   ├── pipeline_repo.py
│       │   ├── run_repo.py
│       │   └── schedule_repo.py
│       ├── pipeline/
│       │   ├── __init__.py
│       │   ├── context.py      # PipelineContext
│       │   ├── decorators.py   # @pipeline decorator
│       │   ├── loader.py       # Load pipeline code from files
│       │   └── types.py        # Pipeline types/enums
│       └── connectors/
│           ├── __init__.py
│           ├── base.py         # Base connector interface
│           ├── postgres.py
│           ├── http_api.py
│           └── s3.py
├── tests/
│   ├── conftest.py
│   ├── test_cli.py
│   ├── test_daemon.py
│   └── test_pipeline.py
├── examples/
│   ├── hello_pipeline.py       # Simplest possible pipeline
│   ├── shopify_sync.py         # Real-world example
│   └── sql_transform.py        # ELT example with ctx.sql()
└── tau.example.toml            # Example config
```

## Architecture Rules
1. **Layered architecture**: api → services → repositories → models (same as Synapse)
2. **Async everything**: All DB operations, all HTTP, all pipeline execution
3. **Pydantic schemas for all API input/output** — never expose SQLAlchemy models directly
4. **Every API endpoint authenticated** via API key (Bearer token in Authorization header)
5. **Structured JSON responses** — AI agents consume the API, so responses must be machine-readable
6. **No YAML config for pipelines** — pipelines are pure Python code

## Key Components to Build

### 1. CLI (`tau`) — Typer app
Entry points: `tau` and `taud`
- `tau deploy <file>` — POST pipeline code to daemon
- `tau run <name>` — trigger a run
- `tau list` — list pipelines
- `tau inspect <name> --last-run` — get structured execution trace
- `tau schedule <name> <cron>` — set schedule
- `tau schedules` — list all schedules
- `tau errors` — recent failures
- `tau logs <name> --run <id>` — run logs
- `tau heal <name>` — diagnose failure (Phase 2, stub for now)
- `taud start [--daemon] [--port PORT]` — start the daemon

The CLI reads TAU_HOST and TAU_API_KEY from env (or ~/.config/tau/environments.toml) and makes HTTP calls to the daemon.

### 2. Daemon (`taud`) — FastAPI app
- Starts on port 8400 by default
- Serves the REST API
- Runs the scheduler (APScheduler)
- Manages pipeline execution
- Stores metadata in PostgreSQL

### 3. Pipeline Runtime
- `@pipeline` decorator that registers metadata
- `PipelineContext` with:
  - `ctx.extract()` — pull data from a source
  - `ctx.transform()` — transform data
  - `ctx.load()` — push data to a target
  - `ctx.sql()` — run SQL in a warehouse
  - `ctx.check()` — run assertions
  - `ctx.secret()` — access secrets
  - `ctx.last_successful_run` — timestamp of last success
- Pipeline code is stored as files on the daemon

### 4. Executor
- Runs pipelines as subprocesses
- Captures structured execution traces (JSON)
- Handles timeouts, retries
- Stores results in PostgreSQL

### 5. Scheduler
- Uses APScheduler with PostgreSQL job store
- Supports cron expressions and intervals
- Triggers pipeline runs via the executor

## pyproject.toml
Use uv. Package name: `tau-pipelines`. Entry points:
```toml
[project.scripts]
tau = "tau.cli.main:app"
taud = "tau.daemon.app:main"
```

## What to Build Now (Phase 1 MVP)
Build everything in the structure above. Make it functional end-to-end:
1. `taud start` — starts daemon with FastAPI + scheduler
2. `tau deploy examples/hello_pipeline.py` — registers a pipeline
3. `tau run hello_pipeline` — executes it
4. `tau inspect hello_pipeline --last-run` — shows structured trace
5. `tau schedule hello_pipeline "*/5 * * * *"` — schedules it
6. `tau list` — shows registered pipelines
7. `tau errors` — shows failures

For the MVP, use SQLite instead of PostgreSQL so there's zero setup required. We'll add PostgreSQL support later.

## Example Pipeline (build this as examples/hello_pipeline.py)
```python
from tau import pipeline, PipelineContext

@pipeline(
    name="hello_world",
    description="A simple hello world pipeline",
    tags=["example"],
)
async def hello_world(ctx: PipelineContext):
    ctx.log("Hello from Tau!")
    
    # Simple extract
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    ctx.log(f"Extracted {len(data)} records")
    
    # Simple transform
    transformed = [{"id": r["id"], "name": r["name"].upper()} for r in data]
    ctx.log(f"Transformed {len(transformed)} records")
    
    # Check
    assert len(transformed) > 0, "No records to process"
    
    return {"records_processed": len(transformed)}
```

## Commit when done
Commit with message: "feat: tau pipelines phase 1 MVP — daemon, CLI, scheduler, executor"
