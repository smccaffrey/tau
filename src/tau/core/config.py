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

    # Server
    host: str = "0.0.0.0"
    port: int = 8400
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

    # Storage
    pipelines_dir: str = Field(default="./pipelines", alias="TAU_PIPELINES_DIR")
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


def get_settings() -> TauSettings:
    # Load from tau.toml files
    toml_config = _load_toml_config()

    # Create settings with connections from toml
    settings = TauSettings()
    if "connections" in toml_config:
        settings.connections = toml_config["connections"]

    return settings


def get_client_settings() -> ClientSettings:
    return ClientSettings()
