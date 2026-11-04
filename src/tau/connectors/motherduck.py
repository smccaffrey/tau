"""MotherDuck (DuckDB cloud) connector."""

from __future__ import annotations
from tau.connectors.base import Connector


class MotherDuckConnector(Connector):
    """Connect to MotherDuck / local DuckDB for extract and load operations."""

    def __init__(self, token: str | None = None, database: str = "my_db", local: bool = False):
        self.token = token
        self.database = database
        self.local = local
        self._conn = None

    async def connect(self) -> None:
        try:
            import duckdb
        except ImportError:
            raise ImportError("Install duckdb: pip install tau-pipelines[motherduck]")

        if self.local:
            self._conn = duckdb.connect(self.database)
        else:
            dsn = f"md:{self.database}?motherduck_token={self.token}" if self.token else f"md:{self.database}"
            self._conn = duckdb.connect(dsn)

    async def disconnect(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    async def extract(self, query: str, params: list | None = None, **kwargs) -> list[dict]:
        if not self._conn:
            await self.connect()
        result = self._conn.execute(query, params or [])
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    async def load(self, data: list[dict], table: str, mode: str = "append",
                   merge_key: str | list[str] | None = None, **kwargs) -> int:
        """Load rows. Creates the table (VARCHAR columns) if it does not exist.

        Upsert deletes rows with the same merge_key and inserts, in one transaction.
        """
        from tau.connectors._load import check_load_mode, key_delete_sql

        keys = check_load_mode(mode, merge_key)
        if not self._conn:
            await self.connect()
        if mode == "replace":
            self._conn.execute(f"DROP TABLE IF EXISTS {table}")
        if not data:
            return 0

        columns = list(data[0].keys())
        try:
            self._conn.execute(f"SELECT 1 FROM {table} LIMIT 0")
        except Exception:
            col_defs = ", ".join(f'"{c}" VARCHAR' for c in columns)
            self._conn.execute(f"CREATE TABLE {table} ({col_defs})")

        placeholders = ", ".join(["?"] * len(columns))
        col_str = ", ".join(f'"{c}"' for c in columns)
        self._conn.execute("BEGIN TRANSACTION")
        try:
            if mode == "upsert":
                sql, _ = key_delete_sql(table, keys, len(data), lambda i: "?")
                self._conn.execute(sql, [r.get(k) for r in data for k in keys])
            self._conn.executemany(
                f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})",
                [[r.get(c) for c in columns] for r in data],
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return len(data)

    async def get_columns(self, relation: str) -> list[str]:
        if not self._conn:
            await self.connect()
        return [d[0] for d in self._conn.execute(f"SELECT * FROM {relation} LIMIT 0").description]

    async def execute(self, query: str, params: list | None = None) -> None:
        if not self._conn:
            await self.connect()
        self._conn.execute(query, params or [])

    async def get_schema(self, table: str) -> list[dict]:
        rows = await self.extract(f"DESCRIBE {table}")
        return rows


def motherduck(token: str | None = None, database: str = "my_db") -> MotherDuckConnector:
    """Create a MotherDuck connector."""
    return MotherDuckConnector(token=token, database=database)


def duckdb_local(database: str = ":memory:") -> MotherDuckConnector:
    """Create a local DuckDB connector."""
    return MotherDuckConnector(database=database, local=True)
