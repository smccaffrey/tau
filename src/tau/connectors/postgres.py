"""PostgreSQL connector."""

from __future__ import annotations
from typing import Any
from tau.connectors.base import Connector


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

    async def extract(self, query: str, params: list | None = None, **kwargs) -> list[dict]:
        """Run a query and return results as list of dicts."""
        if not self._conn:
            await self.connect()
        rows = await self._conn.fetch(query, *(params or []))
        return [dict(row) for row in rows]

    async def load(
        self,
        data: list[dict],
        table: str,
        mode: str = "append",
        merge_key: str | None = None,
        **kwargs,
    ) -> int:
        """Load data into a PostgreSQL table."""
        if not self._conn or not data:
            return 0

        columns = list(data[0].keys())
        col_str = ", ".join(columns)
        placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))

        if mode == "replace":
            await self._conn.execute(f"TRUNCATE TABLE {table}")

        if mode == "upsert" and merge_key:
            # Build upsert query
            update_cols = [c for c in columns if c != merge_key]
            update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
            query = f"""
                INSERT INTO {table} ({col_str}) VALUES ({placeholders})
                ON CONFLICT ({merge_key}) DO UPDATE SET {update_str}
            """
        else:
            query = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"

        # Batch insert
        for record in data:
            values = [record[c] for c in columns]
            await self._conn.execute(query, *values)

        return len(data)

    async def execute(self, query: str, params: list | None = None) -> str:
        """Execute a SQL statement (DDL, DML)."""
        if not self._conn:
            await self.connect()
        return await self._conn.execute(query, *(params or []))

    async def get_schema(self, table: str) -> list[dict]:
        """Get column info for a table."""
        rows = await self.extract("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = $1
            ORDER BY ordinal_position
        """, [table])
        return rows


def postgres(dsn: str) -> PostgresConnector:
    """Create a PostgreSQL connector."""
    return PostgresConnector(dsn=dsn)
