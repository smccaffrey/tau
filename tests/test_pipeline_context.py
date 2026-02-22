"""Tests for PipelineContext — the runtime context passed to pipelines."""

import pytest
import pytest_asyncio
from tau.pipeline.context import PipelineContext, StepContext


class TestPipelineContext:
    def test_init(self):
        ctx = PipelineContext(pipeline_name="test", run_id="run-1")
        assert ctx.pipeline_name == "test"
        assert ctx.run_id == "run-1"
        assert ctx.params == {}
        assert ctx.last_successful_run is None
        assert ctx._logs == []
        assert ctx._steps == []

    def test_init_with_params(self):
        ctx = PipelineContext(
            pipeline_name="test",
            run_id="run-1",
            params={"key": "value"},
        )
        assert ctx.params == {"key": "value"}

    def test_log(self):
        ctx = PipelineContext(pipeline_name="test", run_id="run-1")
        ctx.log("Hello")
        ctx.log("World")
        assert len(ctx._logs) == 2
        assert "Hello" in ctx._logs[0]
        assert "World" in ctx._logs[1]

    def test_get_logs(self):
        ctx = PipelineContext(pipeline_name="test", run_id="run-1")
        ctx.log("Line 1")
        ctx.log("Line 2")
        logs = ctx.get_logs()
        assert "Line 1" in logs
        assert "Line 2" in logs
        assert "\n" in logs

    def test_get_trace_empty(self):
        ctx = PipelineContext(pipeline_name="test", run_id="run-1")
        trace = ctx.get_trace()
        assert trace["run_id"] == "run-1"
        assert trace["pipeline"] == "test"
        assert trace["steps"] == []
        assert trace["log_lines"] == 0

    def test_get_trace_with_logs(self):
        ctx = PipelineContext(pipeline_name="test", run_id="run-1")
        ctx.log("msg")
        trace = ctx.get_trace()
        assert trace["log_lines"] == 1

    @pytest.mark.asyncio
    async def test_extract_stub(self):
        ctx = PipelineContext(pipeline_name="test", run_id="run-1")
        result = await ctx.extract(source="test", resource="items")
        assert result == []
        assert len(ctx._logs) == 1

    @pytest.mark.asyncio
    async def test_extract_with_data(self):
        ctx = PipelineContext(pipeline_name="test", run_id="run-1")
        data = [{"id": 1}]
        result = await ctx.extract(source="test", data=data)
        assert result == data

    @pytest.mark.asyncio
    async def test_transform_stub(self):
        ctx = PipelineContext(pipeline_name="test", run_id="run-1")
        data = [{"id": 1}, {"id": 2}]
        result = await ctx.transform(data)
        assert result == data

    @pytest.mark.asyncio
    async def test_load_stub(self):
        ctx = PipelineContext(pipeline_name="test", run_id="run-1")
        await ctx.load(target="warehouse.table", data=[{"id": 1}])
        assert "Load:" in ctx._logs[0]

    @pytest.mark.asyncio
    async def test_sql_stub(self):
        ctx = PipelineContext(pipeline_name="test", run_id="run-1")
        result = await ctx.sql("SELECT 1")
        assert result == []
        assert "SQL:" in ctx._logs[0]

    @pytest.mark.asyncio
    async def test_check_passes(self):
        ctx = PipelineContext(pipeline_name="test", run_id="run-1")
        await ctx.check(True, True, True)
        assert "3 assertions" in ctx._logs[0]

    @pytest.mark.asyncio
    async def test_check_fails(self):
        ctx = PipelineContext(pipeline_name="test", run_id="run-1")
        with pytest.raises(AssertionError, match="Check #2 failed"):
            await ctx.check(True, False, True)

    def test_secret_from_env(self, monkeypatch):
        monkeypatch.setenv("MY_SECRET", "supersecret")
        ctx = PipelineContext(pipeline_name="test", run_id="run-1")
        assert ctx.secret("MY_SECRET") == "supersecret"

    def test_secret_missing(self):
        ctx = PipelineContext(pipeline_name="test", run_id="run-1")
        assert ctx.secret("NONEXISTENT") == ""


class TestStepContext:
    @pytest.mark.asyncio
    async def test_step_success(self):
        ctx = PipelineContext(pipeline_name="test", run_id="run-1")
        async with ctx.step("my_step") as step:
            step.rows_out = 10

        assert len(ctx._steps) == 1
        assert ctx._steps[0].name == "my_step"
        assert ctx._steps[0].status == "success"
        assert ctx._steps[0].rows_out == 10
        assert ctx._steps[0].duration_ms is not None

    @pytest.mark.asyncio
    async def test_step_failure(self):
        ctx = PipelineContext(pipeline_name="test", run_id="run-1")
        with pytest.raises(ValueError):
            async with ctx.step("bad_step") as step:
                raise ValueError("oops")

        assert ctx._steps[0].status == "failed"
        assert ctx._steps[0].error["type"] == "ValueError"
        assert ctx._steps[0].error["message"] == "oops"

    @pytest.mark.asyncio
    async def test_step_in_trace(self):
        ctx = PipelineContext(pipeline_name="test", run_id="run-1")
        async with ctx.step("s1") as step:
            step.rows_out = 5
        async with ctx.step("s2") as step:
            step.rows_in = 5

        trace = ctx.get_trace()
        assert len(trace["steps"]) == 2
        assert trace["steps"][0]["name"] == "s1"
        assert trace["steps"][0]["rows_out"] == 5
        assert trace["steps"][1]["name"] == "s2"
