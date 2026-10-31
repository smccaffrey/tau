"""Tests for daemon startup: pipeline file loading, auto-deploy, schedule restore."""

import pytest

from tau.core.database import create_tables, init_engine
from tau.daemon import bootstrap
from tau.daemon.scheduler import get_scheduler
from tau.pipeline.loader import load_pipeline_from_file

PIPELINE = '''
from tau import pipeline, PipelineContext

@pipeline(name="nightly", description="Nightly job", schedule="0 3 * * *", tags=["t"])
async def nightly(ctx: PipelineContext):
    return {"ok": True}
'''


def test_load_pipeline_from_file(tmp_path):
    f = tmp_path / "nightly.py"
    f.write_text(PIPELINE)
    metas = load_pipeline_from_file(f)
    assert [m["name"] for m in metas] == ["nightly"]
    assert metas[0]["schedule"] == "0 3 * * *"


@pytest.mark.asyncio
async def test_autodeploy_and_restore(tmp_path):
    init_engine(f"sqlite+aiosqlite:///{tmp_path / 'tau.db'}")
    await create_tables()
    pipelines = tmp_path / "pipelines"
    pipelines.mkdir()
    (pipelines / "nightly.py").write_text(PIPELINE)
    (pipelines / "_helpers.py").write_text("X = 1\n")
    (pipelines / "broken.py").write_text("this is not python")

    assert await bootstrap.autodeploy(pipelines) == ["nightly"]
    # Idempotent: a second run updates instead of duplicating
    assert await bootstrap.autodeploy(pipelines) == ["nightly"]

    scheduler = get_scheduler()
    scheduler.remove_all_jobs()
    assert await bootstrap.restore_schedules() == 1
    assert scheduler.get_job("pipeline:nightly") is not None


@pytest.mark.asyncio
async def test_autodeploy_missing_dir(tmp_path):
    assert await bootstrap.autodeploy(tmp_path / "nope") == []
