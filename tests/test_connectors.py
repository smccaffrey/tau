"""Tests for connectors."""

import pytest
from tau.connectors.base import Connector
from tau.connectors.http_api import HttpApiConnector, http_api
from tau.connectors.s3 import S3Connector, s3
from tau.connectors.postgres import PostgresConnector, postgres


class TestConnectorFactories:
    def test_postgres_factory(self):
        conn = postgres(dsn="postgresql://localhost/test")
        assert isinstance(conn, PostgresConnector)
        assert conn.dsn == "postgresql://localhost/test"

    def test_http_api_factory(self):
        conn = http_api(url="https://api.example.com", auth={"type": "bearer", "token": "tok"})
        assert isinstance(conn, HttpApiConnector)
        assert conn.base_url == "https://api.example.com"
        assert conn.auth["token"] == "tok"

    def test_http_api_strips_trailing_slash(self):
        conn = http_api(url="https://api.example.com/")
        assert conn.base_url == "https://api.example.com"

    def test_s3_factory(self):
        conn = s3(bucket="my-bucket", prefix="data/", aws_key="key", aws_secret="secret")
        assert isinstance(conn, S3Connector)
        assert conn.bucket == "my-bucket"
        assert conn.prefix == "data"
        assert conn.aws_key == "key"

    def test_bigquery_factory(self):
        from tau.connectors.bigquery import bigquery, BigQueryConnector
        conn = bigquery(project="my-project", location="EU")
        assert isinstance(conn, BigQueryConnector)
        assert conn.project == "my-project"
        assert conn.location == "EU"

    def test_motherduck_factory(self):
        from tau.connectors.motherduck import motherduck, duckdb_local, MotherDuckConnector
        conn = motherduck(token="md_token", database="analytics")
        assert isinstance(conn, MotherDuckConnector)
        assert conn.token == "md_token"
        assert conn.database == "analytics"
        assert conn.local is False

    def test_duckdb_local_factory(self):
        from tau.connectors.motherduck import duckdb_local, MotherDuckConnector
        conn = duckdb_local(database=":memory:")
        assert isinstance(conn, MotherDuckConnector)
        assert conn.local is True

    def test_snowflake_factory(self):
        from tau.connectors.snowflake import snowflake, SnowflakeConnector
        conn = snowflake(account="abc123", user="admin", password="pw", warehouse="COMPUTE_WH")
        assert isinstance(conn, SnowflakeConnector)
        assert conn.account == "abc123"
        assert conn.warehouse == "COMPUTE_WH"

    def test_redshift_factory(self):
        from tau.connectors.redshift import redshift, RedshiftConnector
        conn = redshift(host="cluster.abc.us-east-1.redshift.amazonaws.com", database="dev", user="admin", password="pw")
        assert isinstance(conn, RedshiftConnector)
        assert conn.port == 5439

    def test_clickhouse_factory(self):
        from tau.connectors.clickhouse import clickhouse, ClickHouseConnector
        conn = clickhouse(host="ch.example.com", port=8443, secure=True, database="analytics")
        assert isinstance(conn, ClickHouseConnector)
        assert conn.secure is True
        assert conn.database == "analytics"

    def test_mysql_factory(self):
        from tau.connectors.mysql import mysql, MySQLConnector
        conn = mysql(host="db.example.com", database="app", user="root", password="pw")
        assert isinstance(conn, MySQLConnector)
        assert conn.port == 3306
        assert conn.database == "app"


class TestHttpApiConnector:
    @pytest.mark.asyncio
    async def test_connect_with_bearer_auth(self):
        conn = HttpApiConnector(
            base_url="https://api.example.com",
            auth={"type": "bearer", "token": "mytoken"},
        )
        await conn.connect()
        assert conn._client is not None
        assert conn._client.headers.get("authorization") == "Bearer mytoken"
        await conn.disconnect()

    @pytest.mark.asyncio
    async def test_connect_with_api_key_auth(self):
        conn = HttpApiConnector(
            base_url="https://api.example.com",
            auth={"type": "api_key", "key": "mykey", "header": "X-Custom-Key"},
        )
        await conn.connect()
        assert conn._client.headers.get("x-custom-key") == "mykey"
        await conn.disconnect()

    @pytest.mark.asyncio
    async def test_disconnect(self):
        conn = HttpApiConnector(base_url="https://api.example.com")
        await conn.connect()
        assert conn._client is not None
        await conn.disconnect()
        assert conn._client is None

    @pytest.mark.asyncio
    async def test_context_manager(self):
        conn = HttpApiConnector(base_url="https://api.example.com")
        async with conn:
            assert conn._client is not None
        assert conn._client is None


class TestWebhooks:
    @pytest.mark.asyncio
    async def test_register_and_list(self, client):
        resp = await client.post("/api/v1/webhooks", json={
            "url": "https://example.com/webhook",
            "events": ["run.failed", "run.success"],
        })
        assert resp.status_code == 200
        assert resp.json()["status"] == "registered"

        resp = await client.get("/api/v1/webhooks")
        assert resp.status_code == 200
        webhooks = resp.json()["webhooks"]
        assert len(webhooks) >= 1
        assert webhooks[0]["url"] == "https://example.com/webhook"

    @pytest.mark.asyncio
    async def test_delete_webhook(self, client):
        await client.post("/api/v1/webhooks", json={
            "url": "https://example.com/delete-me",
        })
        resp = await client.delete("/api/v1/webhooks", params={"url": "https://example.com/delete-me"})
        assert resp.status_code == 200
