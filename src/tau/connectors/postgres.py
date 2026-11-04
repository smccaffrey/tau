"""PostgreSQL connector."""

from __future__ import annotations
from tau.connectors._load import check_load_mode
from tau.connectors.base import Connector


def _split_table(table: str) -> tuple[str | None, str]:
    if "." in table:
        schema, name = table.rsplit(".", 1)
        return schema.strip('"'), name.strip('"')
    return None, table.strip('"')


class PostgresConnector(Connector):
    """Connect to PostgreSQL for extract and load operations."""

    def __init__(self, dsn: str):
        self.dsn = dsn
        self._conn = None

    async def connect(self) -> None:
        try:
            import asyncpg
        except ImportError:
            raise ImportError("Install asyncpg: pip install tau-pipelines[postgres]")
        self._conn = await asyncpg.connect(self.dsn)

    async def disconnect(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def _ensure(self):
        if not self._conn:
            await self.connect()
        return self._conn

    async def extract(self, query: str, params: list | None = None, **kwargs) -> list[dict]:
        """Run a query and return results as list of dicts."""
        conn = await self._ensure()
        rows = await conn.fetch(query, *(params or []))
        return [dict(row) for row in rows]

    async def load(
        self,
        data: list[dict],
        table: str,
        mode: str = "append",
        merge_key: str | list[str] | None = None,
        **kwargs,
    ) -> int:
        """Load rows into an existing table. Modes: append, replace (truncate first), upsert.

        Upsert uses ``INSERT ... ON CONFLICT (merge_key)``, so merge_key needs a unique index.
        Everything runs in one transaction.
        """
        keys = check_load_mode(mode, merge_key)
        conn = await self._ensure()
        if not data:
            if mode == "replace":
                await conn.execute(f"TRUNCATE TABLE {table}")
            return 0

        columns = list(data[0].keys())
        col_str = ", ".join(columns)
        placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
        query = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"
        if mode == "upsert":
            update_cols = [c for c in columns if c not in keys]
            action = (
                "DO UPDATE SET " + ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
                if update_cols else "DO NOTHING"
            )
            query += f" ON CONFLICT ({', '.join(keys)}) {action}"

        async with conn.transaction():
            if mode == "replace":
                await conn.execute(f"TRUNCATE TABLE {table}")
            await conn.executemany(query, [[r.get(c) for c in columns] for r in data])
        return len(data)

    async def execute(self, query: str, params: list | None = None) -> str:
        """Execute a SQL statement (DDL, DML)."""
        conn = await self._ensure()
        return await conn.execute(query, *(params or []))

    async def get_columns(self, relation: str) -> list[str]:
        """Column names of a table, view or temp table (any schema)."""
        conn = await self._ensure()
        stmt = await conn.prepare(f"SELECT * FROM {relation} LIMIT 0")
        return [a.name for a in stmt.get_attributes()]

    async def get_schema(self, table: str) -> list[dict]:
        """Get column info for a table (``schema.table`` or a table on the search path)."""
        schema, name = _split_table(table)
        if schema:
            return await self.extract("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
            """, [schema, name])
        return await self.extract("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = $1 AND table_schema = ANY(current_schemas(true))
            ORDER BY ordinal_position
        """, [name])


def postgres(dsn: str) -> PostgresConnector:
    """Create a PostgreSQL connector."""
    return PostgresConnector(dsn=dsn)
