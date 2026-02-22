"""Google BigQuery connector."""

from __future__ import annotations
import asyncio
from functools import partial
from tau.connectors.base import Connector


class BigQueryConnector(Connector):
    """Connect to Google BigQuery for extract and load operations."""

    def __init__(self, project: str, credentials_path: str | None = None, location: str = "US"):
        self.project = project
        self.credentials_path = credentials_path
        self.location = location
        self._client = None

    async def connect(self) -> None:
        try:
            from google.cloud import bigquery
        except ImportError:
            raise ImportError("Install google-cloud-bigquery: pip install tau-pipelines[bigquery]")

        kwargs = {"project": self.project}
        if self.credentials_path:
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_file(self.credentials_path)
            kwargs["credentials"] = creds

        self._client = bigquery.Client(**kwargs)

    async def disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._client = None

    async def extract(self, query: str, params: dict | None = None, **kwargs) -> list[dict]:
        """Run a query and return results as list of dicts."""
        if not self._client:
            await self.connect()

        from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

        job_config = None
        if params:
            job_config = QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter(k, "STRING", str(v)) for k, v in params.items()
                ]
            )

        loop = asyncio.get_event_loop()
        query_job = await loop.run_in_executor(
            None, partial(self._client.query, query, job_config=job_config, location=self.location)
        )
        rows = await loop.run_in_executor(None, query_job.result)
        return [dict(row) for row in rows]

    async def load(
        self,
        data: list[dict],
        table: str,
        mode: str = "append",
        schema: list | None = None,
        **kwargs,
    ) -> int:
        """Load data into a BigQuery table."""
        if not self._client or not data:
            return 0

        from google.cloud.bigquery import LoadJobConfig, WriteDisposition

        disposition = {
            "append": WriteDisposition.WRITE_APPEND,
            "replace": WriteDisposition.WRITE_TRUNCATE,
        }.get(mode, WriteDisposition.WRITE_APPEND)

        job_config = LoadJobConfig(write_disposition=disposition)
        if schema:
            job_config.schema = schema

        loop = asyncio.get_event_loop()
        job = await loop.run_in_executor(
            None, partial(self._client.load_table_from_json, data, table, job_config=job_config)
        )
        await loop.run_in_executor(None, job.result)
        return len(data)

    async def execute(self, query: str, params: dict | None = None) -> None:
        """Execute a DDL/DML statement."""
        if not self._client:
            await self.connect()
        loop = asyncio.get_event_loop()
        job = await loop.run_in_executor(
            None, partial(self._client.query, query, location=self.location)
        )
        await loop.run_in_executor(None, job.result)

    async def get_schema(self, table: str) -> list[dict]:
        """Get schema for a table."""
        if not self._client:
            await self.connect()
        loop = asyncio.get_event_loop()
        t = await loop.run_in_executor(None, self._client.get_table, table)
        return [{"name": f.name, "type": f.field_type, "mode": f.mode} for f in t.schema]


def bigquery(project: str, credentials_path: str | None = None, location: str = "US") -> BigQueryConnector:
    """Create a BigQuery connector."""
    return BigQueryConnector(project=project, credentials_path=credentials_path, location=location)
