"""Amazon Redshift connector."""

from __future__ import annotations
from tau.connectors.base import Connector


class RedshiftConnector(Connector):
    """Connect to Amazon Redshift for extract and load operations."""

    def __init__(self, dsn: str | None = None, host: str = "", port: int = 5439,
                 database: str = "", user: str = "", password: str = ""):
        self.dsn = dsn
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self._conn = None

    async def connect(self) -> None:
        try:
            import asyncpg
        except ImportError:
            raise ImportError("Install asyncpg: pip install tau-pipelines[postgres]")

        if self.dsn:
            self._conn = await asyncpg.connect(self.dsn)
        else:
            self._conn = await asyncpg.connect(
                host=self.host, port=self.port, database=self.database,
                user=self.user, password=self.password,
            )

    async def disconnect(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def extract(self, query: str, params: list | None = None, **kwargs) -> list[dict]:
        if not self._conn:
            await self.connect()
        rows = await self._conn.fetch(query, *(params or []))
        return [dict(row) for row in rows]

    async def load(self, data: list[dict], table: str, mode: str = "append",
                   merge_key: str | None = None, **kwargs) -> int:
        if not self._conn or not data:
            return 0

        columns = list(data[0].keys())
        col_str = ", ".join(columns)
        placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))

        if mode == "replace":
            await self._conn.execute(f"TRUNCATE TABLE {table}")

        query = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"
        for record in data:
            values = [record[c] for c in columns]
            await self._conn.execute(query, *values)

        return len(data)

    async def execute(self, query: str, params: list | None = None) -> str:
        if not self._conn:
            await self.connect()
        return await self._conn.execute(query, *(params or []))

    async def get_schema(self, table: str) -> list[dict]:
        return await self.extract("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = $1
            ORDER BY ordinal_position
        """, [table])


def redshift(
    dsn: str | None = None, host: str = "", port: int = 5439,
    database: str = "", user: str = "", password: str = "",
) -> RedshiftConnector:
    """Create a Redshift connector."""
    return RedshiftConnector(dsn=dsn, host=host, port=port, database=database, user=user, password=password)
