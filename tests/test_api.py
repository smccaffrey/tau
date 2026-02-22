"""Tests for the Tau API endpoints."""

import pytest
from tests.conftest import HELLO_PIPELINE_CODE, FAILING_PIPELINE_CODE


class TestHealth:
    @pytest.mark.asyncio
    async def test_health(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "version" in data


class TestAuth:
    @pytest.mark.asyncio
    async def test_no_auth_rejected(self, unauthed_client):
        resp = await unauthed_client.get("/api/v1/pipelines")
        assert resp.status_code in (401, 403)

    @pytest.mark.asyncio
    async def test_bad_auth_rejected(self, app):
        from httpx import AsyncClient, ASGITransport
        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": "Bearer wrong_key"},
        ) as c:
            resp = await c.get("/api/v1/pipelines")
            assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_valid_auth(self, client):
        resp = await client.get("/api/v1/pipelines")
        assert resp.status_code == 200


class TestPipelineCRUD:
    @pytest.mark.asyncio
    async def test_deploy_pipeline(self, client):
        resp = await client.post("/api/v1/pipelines", json={
            "name": "test_pipe",
            "code": HELLO_PIPELINE_CODE,
            "description": "A test pipeline",
            "tags": ["test"],
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "test_pipe"
        assert data["status"] == "active"
        assert data["description"] == "A test pipeline"

    @pytest.mark.asyncio
    async def test_deploy_updates_existing(self, client):
        # Deploy first
        await client.post("/api/v1/pipelines", json={
            "name": "update_test",
            "code": HELLO_PIPELINE_CODE,
        })
        # Deploy again with updated description
        resp = await client.post("/api/v1/pipelines", json={
            "name": "update_test",
            "code": HELLO_PIPELINE_CODE,
            "description": "Updated",
        })
        assert resp.status_code == 200
        assert resp.json()["description"] == "Updated"

    @pytest.mark.asyncio
    async def test_list_pipelines_empty(self, client):
        resp = await client.get("/api/v1/pipelines")
        assert resp.status_code == 200
        data = resp.json()
        assert data["pipelines"] == []
        assert data["total"] == 0

    @pytest.mark.asyncio
    async def test_list_pipelines(self, client):
        await client.post("/api/v1/pipelines", json={
            "name": "pipe_a", "code": HELLO_PIPELINE_CODE,
        })
        await client.post("/api/v1/pipelines", json={
            "name": "pipe_b", "code": HELLO_PIPELINE_CODE,
        })
        resp = await client.get("/api/v1/pipelines")
        data = resp.json()
        assert data["total"] == 2
        names = [p["name"] for p in data["pipelines"]]
        assert "pipe_a" in names
        assert "pipe_b" in names

    @pytest.mark.asyncio
    async def test_get_pipeline(self, client):
        await client.post("/api/v1/pipelines", json={
            "name": "get_test", "code": HELLO_PIPELINE_CODE,
        })
        resp = await client.get("/api/v1/pipelines/get_test")
        assert resp.status_code == 200
        assert resp.json()["name"] == "get_test"

    @pytest.mark.asyncio
    async def test_get_pipeline_not_found(self, client):
        resp = await client.get("/api/v1/pipelines/nonexistent")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_get_pipeline_code(self, client):
        await client.post("/api/v1/pipelines", json={
            "name": "code_test", "code": HELLO_PIPELINE_CODE,
        })
        resp = await client.get("/api/v1/pipelines/code_test/code")
        assert resp.status_code == 200
        data = resp.json()
        assert "hello_world" in data["code"]

    @pytest.mark.asyncio
    async def test_undeploy_pipeline(self, client):
        await client.post("/api/v1/pipelines", json={
            "name": "delete_test", "code": HELLO_PIPELINE_CODE,
        })
        resp = await client.delete("/api/v1/pipelines/delete_test")
        assert resp.status_code == 200
        assert resp.json()["status"] == "removed"

        # Verify it's gone
        resp = await client.get("/api/v1/pipelines/delete_test")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_undeploy_not_found(self, client):
        resp = await client.delete("/api/v1/pipelines/nonexistent")
        assert resp.status_code == 404


class TestPipelineExecution:
    @pytest.mark.asyncio
    async def test_run_pipeline(self, client):
        await client.post("/api/v1/pipelines", json={
            "name": "run_test", "code": HELLO_PIPELINE_CODE,
        })
        resp = await client.post("/api/v1/pipelines/run_test/run")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["pipeline_name"] == "run_test"
        assert data["trigger"] == "manual"
        assert data["result"]["records"] == 2

    @pytest.mark.asyncio
    async def test_run_failing_pipeline(self, client):
        await client.post("/api/v1/pipelines", json={
            "name": "fail_run", "code": FAILING_PIPELINE_CODE,
        })
        resp = await client.post("/api/v1/pipelines/fail_run/run")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "failed"
        assert "Intentional failure" in data["error"]

    @pytest.mark.asyncio
    async def test_run_not_found(self, client):
        resp = await client.post("/api/v1/pipelines/nonexistent/run")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_run_updates_pipeline_last_run(self, client):
        await client.post("/api/v1/pipelines", json={
            "name": "last_run_test", "code": HELLO_PIPELINE_CODE,
        })
        await client.post("/api/v1/pipelines/last_run_test/run")

        resp = await client.get("/api/v1/pipelines/last_run_test")
        data = resp.json()
        assert data["last_run_status"] == "success"
        assert data["last_run_at"] is not None


class TestRuns:
    @pytest.mark.asyncio
    async def test_list_runs(self, client):
        await client.post("/api/v1/pipelines", json={
            "name": "runs_test", "code": HELLO_PIPELINE_CODE,
        })
        await client.post("/api/v1/pipelines/runs_test/run")
        await client.post("/api/v1/pipelines/runs_test/run")

        resp = await client.get("/api/v1/runs/runs_test")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2

    @pytest.mark.asyncio
    async def test_get_last_run(self, client):
        await client.post("/api/v1/pipelines", json={
            "name": "last_test", "code": HELLO_PIPELINE_CODE,
        })
        await client.post("/api/v1/pipelines/last_test/run")

        resp = await client.get("/api/v1/runs/last_test/last")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["trace"] is not None

    @pytest.mark.asyncio
    async def test_get_last_run_not_found(self, client):
        resp = await client.get("/api/v1/runs/nonexistent/last")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_list_errors(self, client):
        await client.post("/api/v1/pipelines", json={
            "name": "err_test", "code": FAILING_PIPELINE_CODE,
        })
        await client.post("/api/v1/pipelines/err_test/run")

        resp = await client.get("/api/v1/runs/errors")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] >= 1
        assert data["runs"][0]["status"] == "failed"

    @pytest.mark.asyncio
    async def test_get_run_logs(self, client):
        await client.post("/api/v1/pipelines", json={
            "name": "logs_test", "code": HELLO_PIPELINE_CODE,
        })
        run_resp = await client.post("/api/v1/pipelines/logs_test/run")
        run_id = run_resp.json()["id"]

        resp = await client.get(f"/api/v1/runs/logs_test/{run_id}/logs")
        assert resp.status_code == 200
        data = resp.json()
        assert "Hello from test!" in data["logs"]


class TestSchedule:
    @pytest.mark.asyncio
    async def test_set_cron_schedule(self, client):
        await client.post("/api/v1/pipelines", json={
            "name": "sched_test", "code": HELLO_PIPELINE_CODE,
        })
        resp = await client.post(
            "/api/v1/pipelines/sched_test/schedule",
            params={"cron": "0 6 * * *"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "scheduled"
        assert data["cron"] == "0 6 * * *"

    @pytest.mark.asyncio
    async def test_schedule_on_deploy(self, client):
        resp = await client.post("/api/v1/pipelines", json={
            "name": "sched_deploy",
            "code": HELLO_PIPELINE_CODE,
            "schedule_cron": "*/10 * * * *",
        })
        assert resp.status_code == 200
        assert resp.json()["schedule_cron"] == "*/10 * * * *"

    @pytest.mark.asyncio
    async def test_schedule_not_found(self, client):
        resp = await client.post(
            "/api/v1/pipelines/nonexistent/schedule",
            params={"cron": "0 * * * *"},
        )
        assert resp.status_code == 404
