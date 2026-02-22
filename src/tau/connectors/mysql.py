"""MySQL connector."""

from __future__ import annotations
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

        self._conn = await aiomysql.connect(
            host=self.host, port=self.port, db=self.database,
            user=self.user, password=self.password,
            autocommit=True,
        )

    async def disconnect(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    async def extract(self, query: str, params: tuple | None = None, **kwargs) -> list[dict]:
        if not self._conn:
            await self.connect()
        import aiomysql
        async with self._conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute(query, params)
            return await cursor.fetchall()

    async def load(self, data: list[dict], table: str, mode: str = "append",
                   merge_key: str | None = None, **kwargs) -> int:
        if not self._conn or not data:
            return 0

        import aiomysql
        async with self._conn.cursor() as cursor:
            if mode == "replace":
                await cursor.execute(f"TRUNCATE TABLE {table}")

            columns = list(data[0].keys())
            col_str = ", ".join(f"`{c}`" for c in columns)
            placeholders = ", ".join(["%s"] * len(columns))

            if mode == "upsert" and merge_key:
                update_cols = [c for c in columns if c != merge_key]
                update_str = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in update_cols)
                query = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_str}"
            else:
                query = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"

            for record in data:
                values = tuple(record.get(c) for c in columns)
                await cursor.execute(query, values)

        return len(data)

    async def execute(self, query: str, params: tuple | None = None) -> None:
        if not self._conn:
            await self.connect()
        async with self._conn.cursor() as cursor:
            await cursor.execute(query, params)

    async def get_schema(self, table: str) -> list[dict]:
        return await self.extract(f"DESCRIBE `{table}`")


def mysql(
    host: str = "localhost", port: int = 3306, database: str = "",
    user: str = "root", password: str = "", ssl: bool = False,
) -> MySQLConnector:
    """Create a MySQL connector."""
    return MySQLConnector(host=host, port=port, database=database, user=user, password=password, ssl=ssl)
