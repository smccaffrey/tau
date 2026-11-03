"""Async database engine and session management."""

from datetime import datetime, timezone

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


async def create_tables():
    import tau.models.dag  # noqa: F401  (register tables that nothing else imports)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
