"""Tau configuration — reads from tau.toml, env vars, and CLI args."""

import os
import sys
from pathlib import Path
from typing import Dict, Any
from pydantic_settings import BaseSettings
from pydantic import Field

# Handle tomli import for Python < 3.11 compatibility
if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None


class TauSettings(BaseSettings):
    """Daemon settings."""

    # Server. Binds to loopback by default; pass --host 0.0.0.0 (or TAU_BIND_HOST) to expose it.
    # (TAU_HOST is the *client's* daemon URL, so the bind address uses its own variable.)
    host: str = Field(default="127.0.0.1", alias="TAU_BIND_HOST")
    port: int = Field(default=8400, alias="TAU_PORT")
    workers: int = 4
    log_level: str = "info"

    # Database (SQLite by default for zero-setup)
    database_url: str = Field(
        default="sqlite+aiosqlite:///tau.db",
        alias="TAU_DATABASE_URL",
    )

    # Auth
    api_key: str = Field(default="tau_dev_key", alias="TAU_API_KEY")

    # Scheduler
    timezone: str = "UTC"
    max_concurrent: int = 10
    retry_default: int = 3

    # Execution: "thread" runs each pipeline attempt on its own thread and event loop so
    # blocking pipeline code cannot stall the daemon; "inline" uses the daemon's loop.
    execution_mode: str = Field(default="thread", alias="TAU_EXECUTION_MODE")
    # Extra seconds past a pipeline's timeout before a blocked run thread is abandoned.
    timeout_grace: int = Field(default=30, alias="TAU_TIMEOUT_GRACE")
    # Identifies this daemon in run records, so a restart only fails its own stale runs.
    # Defaults to the hostname; set it when several daemons share a host and database.
    instance_id: str | None = Field(default=None, alias="TAU_INSTANCE_ID")

    # Distributed mode
    # Coordinator: run pipelines in this process too (set false so only remote workers run them).
    local_execution: bool = Field(default=True, alias="TAU_LOCAL_EXECUTION")
    # Seconds between coordinator health checks of remote workers.
    heartbeat_interval: int = Field(default=15, alias="TAU_HEARTBEAT_INTERVAL")
    # Worker: register with this coordinator on startup (and every heartbeat interval).
    coordinator_url: str | None = Field(default=None, alias="TAU_COORDINATOR_URL")
    # Worker: API key of the coordinator (defaults to this daemon's TAU_API_KEY).
    coordinator_api_key: str | None = Field(default=None, alias="TAU_COORDINATOR_API_KEY")
    # Worker: URL the coordinator should use to reach this daemon, e.g. http://worker1:8400.
    advertise_url: str | None = Field(default=None, alias="TAU_ADVERTISE_URL")
    # Worker: name shown in `tau workers` (defaults to the hostname).
    worker_id: str | None = Field(default=None, alias="TAU_WORKER_ID")

    # Storage
    pipelines_dir: str = Field(default="./pipelines", alias="TAU_PIPELINES_DIR")
    # Deploy every *.py pipeline in this folder at startup (create or update).
    autodeploy_dir: str | None = Field(default=None, alias="TAU_AUTODEPLOY_DIR")
    artifacts_dir: str = Field(default="./artifacts", alias="TAU_ARTIFACTS_DIR")

    # Connections (loaded from tau.toml [connections] section)
    connections: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

    model_config = {"env_prefix": "TAU_", "env_file": ".env"}


class ClientSettings(BaseSettings):
    """CLI client settings."""

    host: str = Field(default="http://localhost:8400", alias="TAU_HOST")
    api_key: str = Field(default="tau_dev_key", alias="TAU_API_KEY")

    model_config = {"env_prefix": "TAU_"}


def _load_toml_config() -> Dict[str, Any]:
    """Load configuration from tau.toml files.

    Searches for tau.toml in:
    1. TAU_HOME (~/.tau/tau.toml by default)
    2. Current directory (./tau.toml)

    Returns:
        Combined configuration dict from found files
    """
    if tomllib is None:
        return {}

    config = {}

    # Try TAU_HOME first (global config)
    tau_home = Path(os.environ.get("TAU_HOME", "~/.tau")).expanduser()
    global_config_path = tau_home / "tau.toml"

    if global_config_path.exists():
        try:
            with global_config_path.open("rb") as f:
                global_config = tomllib.load(f)
                config.update(global_config)
        except Exception:
            # Ignore config file errors for now
            pass

    # Try local tau.toml (project-specific config, takes precedence)
    local_config_path = Path("tau.toml")
    if local_config_path.exists():
        try:
            with local_config_path.open("rb") as f:
                local_config = tomllib.load(f)
                # Merge connections specifically to avoid overwriting all
                if "connections" in local_config:
                    config.setdefault("connections", {}).update(local_config["connections"])
                # Update other config sections
                for key, value in local_config.items():
                    if key != "connections":
                        config[key] = value
        except Exception:
            # Ignore config file errors for now
            pass

    return config


# tau.toml [section] key -> (settings field name or alias, environment variable that overrides it)
_TOML_KEYS = {
    ("daemon", "host"): ("TAU_BIND_HOST", "TAU_BIND_HOST"),
    ("daemon", "port"): ("TAU_PORT", "TAU_PORT"),
    ("daemon", "workers"): ("workers", "TAU_WORKERS"),
    ("daemon", "log_level"): ("log_level", "TAU_LOG_LEVEL"),
    ("database", "url"): ("TAU_DATABASE_URL", "TAU_DATABASE_URL"),
    ("auth", "api_key"): ("TAU_API_KEY", "TAU_API_KEY"),
    ("scheduler", "timezone"): ("timezone", "TAU_TIMEZONE"),
    ("scheduler", "max_concurrent"): ("max_concurrent", "TAU_MAX_CONCURRENT"),
}


def get_settings() -> TauSettings:
    """Build settings with precedence: environment variables > tau.toml > defaults."""
    toml_config = _load_toml_config()

    overrides: Dict[str, Any] = {}
    for (section, key), (field, env_var) in _TOML_KEYS.items():
        value = toml_config.get(section, {}).get(key)
        if value is not None and env_var not in os.environ:
            overrides[field] = value

    settings = TauSettings(**overrides)
    if "connections" in toml_config:
        settings.connections = toml_config["connections"]

    return settings


def instance_id(settings: TauSettings | None = None) -> str:
    import socket

    settings = settings or get_settings()
    return settings.instance_id or socket.gethostname()


def get_client_settings() -> ClientSettings:
    return ClientSettings()
