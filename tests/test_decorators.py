"""Tests for @pipeline decorator."""

import pytest
from tau.pipeline.decorators import pipeline, get_registry, _parse_timeout, _pipeline_registry


class TestPipelineDecorator:
    def setup_method(self):
        _pipeline_registry.clear()

    def test_basic_decorator(self):
        @pipeline(name="test_pipe")
        async def my_pipeline(ctx):
            pass

        assert hasattr(my_pipeline, "_tau_pipeline")
        assert my_pipeline._tau_pipeline["name"] == "test_pipe"

    def test_default_name_from_function(self):
        @pipeline()
        async def my_func(ctx):
            pass

        # Name defaults to function name when name=None
        # Actually our decorator requires name or uses func.__name__
        assert my_func._tau_pipeline["name"] == "my_func"

    def test_full_metadata(self):
        @pipeline(
            name="full_test",
            description="A test pipeline",
            schedule="0 6 * * *",
            retry=5,
            timeout="1h",
            tags=["test", "example"],
        )
        async def full_pipe(ctx):
            pass

        meta = full_pipe._tau_pipeline
        assert meta["name"] == "full_test"
        assert meta["description"] == "A test pipeline"
        assert meta["schedule"] == "0 6 * * *"
        assert meta["retry"] == 5
        assert meta["timeout_seconds"] == 3600
        assert meta["tags"] == ["test", "example"]

    def test_registry(self):
        @pipeline(name="reg_test")
        async def reg_pipe(ctx):
            pass

        registry = get_registry()
        assert "reg_test" in registry
        assert registry["reg_test"]["name"] == "reg_test"

    def test_multiple_pipelines(self):
        @pipeline(name="pipe_a")
        async def a(ctx):
            pass

        @pipeline(name="pipe_b")
        async def b(ctx):
            pass

        registry = get_registry()
        assert "pipe_a" in registry
        assert "pipe_b" in registry

    @pytest.mark.asyncio
    async def test_decorated_function_still_callable(self):
        @pipeline(name="callable_test")
        async def my_pipe(ctx):
            return "hello"

        result = await my_pipe(None)
        assert result == "hello"


class TestParseTimeout:
    def test_seconds(self):
        assert _parse_timeout("30s") == 30

    def test_minutes(self):
        assert _parse_timeout("30m") == 1800

    def test_hours(self):
        assert _parse_timeout("2h") == 7200

    def test_plain_number(self):
        assert _parse_timeout("60") == 60
