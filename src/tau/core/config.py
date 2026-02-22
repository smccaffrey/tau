"""Tau configuration — reads from tau.toml, env vars, and CLI args."""

import os
from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field


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

    model_config = {"env_prefix": "TAU_", "env_file": ".env"}


class ClientSettings(BaseSettings):
    """CLI client settings."""

    host: str = Field(default="http://localhost:8400", alias="TAU_HOST")
    api_key: str = Field(default="tau_dev_key", alias="TAU_API_KEY")

    model_config = {"env_prefix": "TAU_"}


def get_settings() -> TauSettings:
    return TauSettings()


def get_client_settings() -> ClientSettings:
    return ClientSettings()
