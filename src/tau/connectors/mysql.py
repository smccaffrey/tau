"""MySQL connector."""

from __future__ import annotations
from tau.connectors._load import check_load_mode
from tau.connectors.base import Connector


class MySQLConnector(Connector):
    """Connect to MySQL/MariaDB for extract and load operations."""

    def __init__(self, host: str = "localhost", port: int = 3306, database: str = "",
                 user: str = "root", password: str = "", ssl: bool = False):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.ssl = ssl
        self._conn = None

    async def connect(self) -> None:
        try:
            import aiomysql
        except ImportError:
            raise ImportError("Install aiomysql: pip install tau-pipelines[mysql]")

        ssl_ctx = None
        if self.ssl:
            import ssl as _ssl
            ssl_ctx = _ssl.create_default_context()
        self._conn = await aiomysql.connect(
            host=self.host, port=self.port, db=self.database,
            user=self.user, password=self.password,
            autocommit=True, ssl=ssl_ctx,
        )

    async def disconnect(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    async def _ensure(self):
        if not self._conn:
            await self.connect()
        return self._conn

    async def extract(self, query: str, params: tuple | None = None, **kwargs) -> list[dict]:
        conn = await self._ensure()
        import aiomysql
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute(query, params)
            return list(await cursor.fetchall())

    async def load(self, data: list[dict], table: str, mode: str = "append",
                   merge_key: str | list[str] | None = None, **kwargs) -> int:
        """Load rows. Upsert uses ``ON DUPLICATE KEY UPDATE`` (merge_key needs a unique key)."""
        keys = check_load_mode(mode, merge_key)
        conn = await self._ensure()
        if not data:
            if mode == "replace":
                async with conn.cursor() as cursor:
                    await cursor.execute(f"TRUNCATE TABLE {table}")
            return 0

        columns = list(data[0].keys())
        col_str = ", ".join(f"`{c}`" for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"
        if mode == "upsert":
            update_cols = [c for c in columns if c not in keys] or keys[:1]
            query += " ON DUPLICATE KEY UPDATE " + ", ".join(f"`{c}` = VALUES(`{c}`)" for c in update_cols)

        async with conn.cursor() as cursor:
            if mode == "replace":
                await cursor.execute(f"TRUNCATE TABLE {table}")
            await cursor.executemany(query, [tuple(r.get(c) for c in columns) for r in data])
        return len(data)

    async def execute(self, query: str, params: tuple | None = None) -> None:
        conn = await self._ensure()
        async with conn.cursor() as cursor:
            await cursor.execute(query, params)

    async def get_columns(self, relation: str) -> list[str]:
        """Column names of a table, view or temporary table."""
        conn = await self._ensure()
        async with conn.cursor() as cursor:
            await cursor.execute(f"SELECT * FROM {relation} LIMIT 0")
            return [d[0] for d in cursor.description]

    async def get_schema(self, table: str) -> list[dict]:
        quoted = ".".join(f"`{p.strip('`')}`" for p in table.split("."))
        return await self.extract(f"DESCRIBE {quoted}")


def mysql(
    host: str = "localhost", port: int = 3306, database: str = "",
    user: str = "root", password: str = "", ssl: bool = False,
) -> MySQLConnector:
    """Create a MySQL connector."""
    return MySQLConnector(host=host, port=port, database=database, user=user, password=password, ssl=ssl)
