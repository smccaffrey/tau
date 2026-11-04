"""ClickHouse connector (HTTP interface)."""

from __future__ import annotations
import json
import uuid

from tau.connectors._load import check_load_mode
from tau.connectors.base import Connector


class ClickHouseError(RuntimeError):
    """A query failed; the message carries ClickHouse's own error text."""


class ClickHouseConnector(Connector):
    """Connect to ClickHouse for extract and load operations.

    Every request of one connector shares an HTTP session (``session_id``), so temporary
    tables live until ``disconnect``. Credentials go in headers, not the URL.
    """

    def __init__(self, host: str = "localhost", port: int = 8123, database: str = "default",
                 user: str = "default", password: str = "", secure: bool = False):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.secure = secure
        self._client = None

    async def connect(self) -> None:
        import httpx

        scheme = "https" if self.secure else "http"
        self._base_url = f"{scheme}://{self.host}:{self.port}"
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            params={"database": self.database, "session_id": f"tau-{uuid.uuid4()}"},
            headers={"X-ClickHouse-User": self.user, "X-ClickHouse-Key": self.password},
            timeout=300,
        )

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _post(self, content: str, **params):
        if not self._client:
            await self.connect()
        resp = await self._client.post("/", content=content.encode(), params=params or None)
        if resp.status_code >= 400:
            raise ClickHouseError(f"ClickHouse {resp.status_code}: {resp.text.strip()[:2000]}")
        return resp

    async def extract(self, query: str, params: dict | None = None, **kwargs) -> list[dict]:
        q = query.rstrip().rstrip(";")
        resp = await self._post(
            f"{q} FORMAT JSON",
            output_format_json_quote_64bit_integers="0",
        )
        return resp.json().get("data", [])

    async def load(self, data: list[dict], table: str, mode: str = "append",
                   merge_key: str | list[str] | None = None, **kwargs) -> int:
        """Load rows with ``JSONEachRow``.

        Upsert deletes rows with the same merge_key (a lightweight ``DELETE``) and inserts.
        """
        keys = check_load_mode(mode, merge_key)
        if mode == "replace":
            await self.execute(f"TRUNCATE TABLE IF EXISTS {table}")
        if not data:
            return 0
        lines = "\n".join(json.dumps(record, default=str) for record in data)
        if mode == "upsert":
            target = f"({', '.join(keys)})" if len(keys) > 1 else keys[0]
            # A real (not TEMPORARY) table: mutations run server-side and cannot see
            # session-scoped temporary tables.
            stage = f"__tau_upsert_{uuid.uuid4().hex[:12]}"
            await self.execute(
                f"CREATE TABLE {stage} ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM {table} WHERE 0"
            )
            try:
                await self._post(lines, query=f"INSERT INTO {stage} FORMAT JSONEachRow")
                await self.execute(f"DELETE FROM {table} WHERE {target} IN (SELECT {target} FROM {stage})")
                await self.execute(f"INSERT INTO {table} SELECT * FROM {stage}")
            finally:
                await self.execute(f"DROP TABLE IF EXISTS {stage}")
            return len(data)

        await self._post(lines, query=f"INSERT INTO {table} FORMAT JSONEachRow")
        return len(data)

    async def execute(self, query: str, params: dict | None = None) -> str:
        # mutations_sync makes ALTER ... UPDATE/DELETE finish before returning.
        resp = await self._post(query, mutations_sync="2")
        return resp.text

    async def get_columns(self, relation: str) -> list[str]:
        resp = await self._post(f"SELECT * FROM {relation} LIMIT 0 FORMAT JSON")
        return [m["name"] for m in resp.json().get("meta", [])]

    async def get_schema(self, table: str) -> list[dict]:
        return await self.extract(f"DESCRIBE TABLE {table}")


def clickhouse(
    host: str = "localhost", port: int = 8123, database: str = "default",
    user: str = "default", password: str = "", secure: bool = False,
) -> ClickHouseConnector:
    """Create a ClickHouse connector."""
    return ClickHouseConnector(
        host=host, port=port, database=database,
        user=user, password=password, secure=secure,
    )
