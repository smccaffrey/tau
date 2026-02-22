"""Tests for the pipeline executor."""

import pytest
from tau.daemon.executor import execute_pipeline, _load_pipeline_function
from tests.conftest import (
    HELLO_PIPELINE_CODE,
    FAILING_PIPELINE_CODE,
    STEP_PIPELINE_CODE,
)


class TestExecutePipeline:
    @pytest.mark.asyncio
    async def test_successful_execution(self):
        result = await execute_pipeline(
            pipeline_name="hello_world",
            code=HELLO_PIPELINE_CODE,
            run_id="test-run-1",
        )
        assert result.status == "success"
        assert result.error is None
        assert result.result is not None
        assert result.result["records"] == 2
        assert result.duration_ms is not None
        assert result.started_at is not None
        assert result.finished_at is not None

    @pytest.mark.asyncio
    async def test_successful_execution_logs(self):
        result = await execute_pipeline(
            pipeline_name="hello_world",
            code=HELLO_PIPELINE_CODE,
            run_id="test-run-2",
        )
        assert "Hello from test!" in result.logs
        assert "Processed 2 records" in result.logs

    @pytest.mark.asyncio
    async def test_successful_execution_trace(self):
        result = await execute_pipeline(
            pipeline_name="hello_world",
            code=HELLO_PIPELINE_CODE,
            run_id="test-run-3",
        )
        assert result.trace is not None
        assert result.trace["run_id"] == "test-run-3"
        assert result.trace["pipeline"] == "hello_world"
        assert result.trace["status"] == "success"

    @pytest.mark.asyncio
    async def test_failing_execution(self):
        result = await execute_pipeline(
            pipeline_name="failing_test",
            code=FAILING_PIPELINE_CODE,
            run_id="test-run-fail",
        )
        assert result.status == "failed"
        assert result.error is not None
        assert "Intentional failure" in result.error
        assert "ValueError" in result.error

    @pytest.mark.asyncio
    async def test_failing_execution_trace(self):
        result = await execute_pipeline(
            pipeline_name="failing_test",
            code=FAILING_PIPELINE_CODE,
            run_id="test-run-fail-2",
        )
        assert result.trace["status"] == "failed"
        assert "About to fail" in result.logs

    @pytest.mark.asyncio
    async def test_timeout(self):
        result = await execute_pipeline(
            pipeline_name="slow_test",
            code='''
import asyncio
from tau import pipeline, PipelineContext

@pipeline(name="slow_test")
async def slow_test(ctx: PipelineContext):
    await asyncio.sleep(10)
''',
            run_id="test-run-timeout",
            timeout_seconds=1,
        )
        assert result.status == "timeout"
        assert "timed out" in result.error

    @pytest.mark.asyncio
    async def test_step_tracing(self):
        result = await execute_pipeline(
            pipeline_name="step_test",
            code=STEP_PIPELINE_CODE,
            run_id="test-run-steps",
        )
        assert result.status == "success"
        steps = result.trace["steps"]
        assert len(steps) == 3
        assert steps[0]["name"] == "extract"
        assert steps[0]["status"] == "success"
        assert steps[0]["rows_out"] == 5
        assert steps[1]["name"] == "transform"
        assert steps[1]["rows_in"] == 5
        assert steps[2]["name"] == "load"

    @pytest.mark.asyncio
    async def test_params_passed(self):
        code = '''
from tau import pipeline, PipelineContext

@pipeline(name="param_test")
async def param_test(ctx: PipelineContext):
    return {"got_params": ctx.params}
'''
        result = await execute_pipeline(
            pipeline_name="param_test",
            code=code,
            run_id="test-params",
            params={"key": "value"},
        )
        assert result.status == "success"
        assert result.result["got_params"] == {"key": "value"}

    @pytest.mark.asyncio
    async def test_no_pipeline_function(self):
        code = '''
def not_a_pipeline():
    pass
'''
        result = await execute_pipeline(
            pipeline_name="missing",
            code=code,
            run_id="test-missing",
        )
        assert result.status == "failed"
        assert "No @pipeline decorated function found" in result.error


class TestLoadPipelineFunction:
    def test_load_valid(self):
        func = _load_pipeline_function("hello_world", HELLO_PIPELINE_CODE)
        assert func is not None
        assert hasattr(func, "_tau_pipeline")

    def test_load_no_decorator(self):
        func = _load_pipeline_function("test", "def foo(): pass")
        assert func is None

    def test_load_wrong_name_still_finds(self):
        # Should find any @pipeline decorated function as fallback
        func = _load_pipeline_function("wrong_name", HELLO_PIPELINE_CODE)
        assert func is not None
