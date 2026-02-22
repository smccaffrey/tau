"""Shared test fixtures for Tau tests."""

import asyncio
import os
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import AsyncSession

from tau.core.database import init_engine, create_tables, async_session_factory, engine, Base
from tau.daemon.main import create_app


@pytest.fixture(scope="session")
def event_loop():
    """Use a single event loop for all tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="function")
async def app():
    """Create a fresh app with in-memory SQLite for each test."""
    os.environ["TAU_DATABASE_URL"] = "sqlite+aiosqlite://"
    os.environ["TAU_API_KEY"] = "test_key"

    init_engine("sqlite+aiosqlite://")
    await create_tables()

    _app = create_app()

    yield _app

    # Cleanup: drop all tables
    from tau.core.database import engine as _engine
    if _engine is not None:
        async with _engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture(scope="function")
async def client(app):
    """Async HTTP client pointed at the test app."""
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": "Bearer test_key"},
    ) as c:
        yield c


@pytest_asyncio.fixture(scope="function")
async def unauthed_client(app):
    """Async HTTP client without auth."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture(scope="function")
async def db_session() -> AsyncSession:
    """Get a database session for direct DB operations in tests."""
    async with async_session_factory() as session:
        yield session


# ─── Sample pipeline code ───

HELLO_PIPELINE_CODE = '''
from tau import pipeline, PipelineContext

@pipeline(
    name="hello_world",
    description="Test pipeline",
    tags=["test"],
)
async def hello_world(ctx: PipelineContext):
    ctx.log("Hello from test!")
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    ctx.log(f"Processed {len(data)} records")
    return {"records": len(data)}
'''

FAILING_PIPELINE_CODE = '''
from tau import pipeline, PipelineContext

@pipeline(name="failing_test")
async def failing_test(ctx: PipelineContext):
    ctx.log("About to fail...")
    raise ValueError("Intentional failure for testing")
'''

SLOW_PIPELINE_CODE = '''
import asyncio
from tau import pipeline, PipelineContext

@pipeline(name="slow_test", timeout="2s")
async def slow_test(ctx: PipelineContext):
    ctx.log("Starting slow pipeline...")
    await asyncio.sleep(10)
    return {"done": True}
'''

STEP_PIPELINE_CODE = '''
from tau import pipeline, PipelineContext

@pipeline(name="step_test")
async def step_test(ctx: PipelineContext):
    async with ctx.step("extract") as step:
        data = [{"id": i} for i in range(5)]
        step.rows_out = len(data)

    async with ctx.step("transform") as step:
        step.rows_in = len(data)
        transformed = [{"id": d["id"], "doubled": d["id"] * 2} for d in data]
        step.rows_out = len(transformed)

    async with ctx.step("load") as step:
        step.rows_in = len(transformed)

    return {"processed": len(transformed)}
'''
