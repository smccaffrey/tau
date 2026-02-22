# Tau Pipelines — development commands
# Run `just` to see all available commands

set dotenv-load

default:
    @just --list

# ─── Development ───

# Install all dependencies (dev + all connectors)
install:
    uv sync --python ">=3.12" --all-extras

# Start the daemon (dev mode, port 8400)
dev:
    uv run taud --port 8400

# Start daemon with auto-reload on file changes
dev-watch:
    uv run uvicorn tau.daemon.main:create_app --factory --reload --host 0.0.0.0 --port 8400

# Open the web dashboard
dashboard:
    open http://localhost:8400/

# ─── Testing ───

# Run all tests
test:
    uv run pytest tests/ -v

# Run tests with short output
test-short:
    uv run pytest tests/ --tb=short

# Run a specific test file
test-file file:
    uv run pytest tests/{{file}} -v

# Run tests matching a pattern
test-k pattern:
    uv run pytest tests/ -v -k "{{pattern}}"

# Run tests with coverage
test-cov:
    uv run pytest tests/ --cov=tau --cov-report=term-missing

# ─── Linting & Formatting ───

# Type check
typecheck:
    uv run mypy src/tau/ --ignore-missing-imports

# Format code
fmt:
    uv run ruff format src/ tests/

# Lint
lint:
    uv run ruff check src/ tests/

# Lint and auto-fix
lint-fix:
    uv run ruff check src/ tests/ --fix

# ─── CLI Shortcuts ───

# Deploy a pipeline file
deploy file:
    uv run tau deploy {{file}}

# Run a pipeline by name
run name:
    uv run tau run {{name}}

# List all pipelines
list:
    uv run tau list

# Inspect last run of a pipeline
inspect name:
    uv run tau inspect {{name}} --last-run

# Show recent errors
errors:
    uv run tau errors

# Show the DAG
dag:
    uv run tau dag

# Show worker pool status
workers:
    uv run tau workers

# Daemon status
status:
    uv run tau status

# ─── Database ───

# Reset the dev database
db-reset:
    rm -f tau.db
    @echo "Database reset. Restart the daemon to recreate."

# ─── Build & Release ───

# Build the package
build:
    uv build

# Publish to PyPI (requires token)
publish:
    uv publish

# Bump version (pass: patch, minor, major)
bump version:
    #!/usr/bin/env bash
    current=$(grep '__version__' src/tau/__init__.py | cut -d'"' -f2)
    echo "Current: $current"
    IFS='.' read -r major minor patch <<< "$current"
    case "{{version}}" in
        patch) patch=$((patch + 1)) ;;
        minor) minor=$((minor + 1)); patch=0 ;;
        major) major=$((major + 1)); minor=0; patch=0 ;;
        *) echo "Usage: just bump [patch|minor|major]"; exit 1 ;;
    esac
    new="$major.$minor.$patch"
    sed -i '' "s/__version__ = \".*\"/__version__ = \"$new\"/" src/tau/__init__.py
    echo "Bumped: $current → $new"

# ─── Docker ───

# Build docker image
docker-build:
    docker build -t tau-pipelines .

# Run daemon in docker
docker-run:
    docker run -p 8400:8400 tau-pipelines

# ─── Utilities ───

# Count lines of code
loc:
    @find src/tau -name '*.py' | xargs wc -l | tail -1

# Count tests
test-count:
    @uv run pytest tests/ --co -q 2>/dev/null | tail -1

# Clean build artifacts
clean:
    rm -rf dist/ build/ *.egg-info .pytest_cache .mypy_cache .ruff_cache
    find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
