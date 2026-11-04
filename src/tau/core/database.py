"""Async database engine and session management."""

from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import inspect, text

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

engine = None
async_session_factory = None


class Base(DeclarativeBase):
    pass


def to_db_time(value):
    """Store timestamps as naive UTC.

    The columns are ``TIMESTAMP WITHOUT TIME ZONE``. SQLite accepts aware datetimes,
    but asyncpg rejects them for these columns, which broke every run on PostgreSQL.
    """
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def init_engine(database_url: str):
    global engine, async_session_factory
    connect_args = {}
    if "sqlite" in database_url:
        connect_args["check_same_thread"] = False
    engine = create_async_engine(database_url, echo=False, connect_args=connect_args)
    async_session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_session() -> AsyncSession:
    async with async_session_factory() as session:
        yield session


MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
BASELINE_REVISION = "0001"


def _alembic_config(connection):
    from alembic.config import Config

    cfg = Config()
    cfg.set_main_option("script_location", str(MIGRATIONS_DIR))
    cfg.attributes["connection"] = connection
    return cfg


# Held for the duration of the migration transaction, so daemons that start together
# against one PostgreSQL database migrate one at a time.
MIGRATION_LOCK_KEY = 7_461_750_000


def _upgrade(connection) -> None:
    from alembic import command

    if connection.dialect.name == "postgresql":
        connection.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": MIGRATION_LOCK_KEY})
    tables = set(inspect(connection).get_table_names())
    cfg = _alembic_config(connection)
    if "pipelines" in tables and "alembic_version" not in tables:
        # Created by a Tau version that predates migrations: its schema is the baseline.
        command.stamp(cfg, BASELINE_REVISION)
    elif "pipelines" not in tables and "alembic_version" in tables:
        # Tables were dropped but the version marker survived; start from scratch.
        connection.execute(text("DELETE FROM alembic_version"))
    command.upgrade(cfg, "head")


async def create_tables():
    """Create or upgrade the schema to the latest migration (safe to call on every start)."""
    import tau.models  # noqa: F401  (register every table)

    async with engine.begin() as conn:
        await conn.run_sync(_upgrade)
