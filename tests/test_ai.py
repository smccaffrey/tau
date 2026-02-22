"""Tests for AI generator module (unit tests, no API calls)."""

import pytest
from tau.ai.generator import generate_pipeline, diagnose_failure


class TestGeneratorInputValidation:
    @pytest.mark.asyncio
    async def test_no_api_key_raises(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("TAU_AI_API_KEY", raising=False)

        with pytest.raises(ValueError, match="No AI API key"):
            await generate_pipeline("test pipeline")

    @pytest.mark.asyncio
    async def test_diagnose_no_api_key_raises(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("TAU_AI_API_KEY", raising=False)

        with pytest.raises(ValueError, match="No AI API key"):
            await diagnose_failure(
                pipeline_name="test",
                pipeline_code="code",
                error="error",
            )


class TestAIEndpoints:
    @pytest.mark.asyncio
    async def test_generate_no_key_returns_500(self, client, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("TAU_AI_API_KEY", raising=False)

        resp = await client.post("/api/v1/ai/generate", json={
            "intent": "sync shopify orders",
        })
        assert resp.status_code == 500

    @pytest.mark.asyncio
    async def test_heal_no_pipeline(self, client):
        resp = await client.post("/api/v1/ai/heal", json={
            "pipeline_name": "nonexistent",
        })
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_heal_healthy_pipeline(self, client):
        from tests.conftest import HELLO_PIPELINE_CODE

        # Deploy and run successfully
        await client.post("/api/v1/pipelines", json={
            "name": "healthy_pipe", "code": HELLO_PIPELINE_CODE,
        })
        await client.post("/api/v1/pipelines/healthy_pipe/run")

        resp = await client.post("/api/v1/ai/heal", json={
            "pipeline_name": "healthy_pipe",
        })
        assert resp.status_code == 200
        assert resp.json()["status"] == "healthy"
