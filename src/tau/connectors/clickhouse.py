"""ClickHouse connector."""

from __future__ import annotations
from tau.connectors.base import Connector


class ClickHouseConnector(Connector):
    """Connect to ClickHouse for extract and load operations."""

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
        try:
            import httpx
        except ImportError:
            raise ImportError("httpx is required for ClickHouse connector")

        scheme = "https" if self.secure else "http"
        self._base_url = f"{scheme}://{self.host}:{self.port}"
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            params={"database": self.database, "user": self.user, "password": self.password},
            timeout=60,
        )

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def extract(self, query: str, params: dict | None = None, **kwargs) -> list[dict]:
        if not self._client:
            await self.connect()

        # Append FORMAT JSON to get structured output
        q = query.rstrip().rstrip(";")
        resp = await self._client.post("/", content=f"{q} FORMAT JSON")
        resp.raise_for_status()
        result = resp.json()
        return result.get("data", [])

    async def load(self, data: list[dict], table: str, mode: str = "append", **kwargs) -> int:
        if not self._client or not data:
            return 0

        if mode == "replace":
            await self.execute(f"TRUNCATE TABLE IF EXISTS {table}")

        import json
        # ClickHouse accepts JSONEachRow format
        lines = "\n".join(json.dumps(record, default=str) for record in data)
        insert_params = {
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "query": f"INSERT INTO {table} FORMAT JSONEachRow",
        }
        resp = await self._client.post(
            "/",
            content=lines,
            params=insert_params,
        )
        resp.raise_for_status()
        return len(data)

    async def execute(self, query: str, params: dict | None = None) -> str:
        if not self._client:
            await self.connect()
        resp = await self._client.post("/", content=query)
        resp.raise_for_status()
        return resp.text

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
