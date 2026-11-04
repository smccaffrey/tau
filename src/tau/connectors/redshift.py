"""Amazon Redshift connector (PostgreSQL wire protocol, via asyncpg)."""

from __future__ import annotations
from tau.connectors._load import check_load_mode
from tau.connectors.postgres import PostgresConnector


class RedshiftConnector(PostgresConnector):
    """Connect to Amazon Redshift for extract and load operations.

    Redshift has no ``ON CONFLICT``, so upsert stages the rows in a temp table and does
    ``DELETE ... USING`` + ``INSERT`` in one transaction.
    """

    def __init__(self, dsn: str | None = None, host: str = "", port: int = 5439,
                 database: str = "", user: str = "", password: str = ""):
        super().__init__(dsn=dsn or "")
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    async def connect(self) -> None:
        try:
            import asyncpg
        except ImportError:
            raise ImportError("Install asyncpg: pip install tau-pipelines[redshift]")

        if self.dsn:
            self._conn = await asyncpg.connect(self.dsn)
        else:
            self._conn = await asyncpg.connect(
                host=self.host, port=self.port, database=self.database,
                user=self.user, password=self.password,
            )

    async def load(self, data: list[dict], table: str, mode: str = "append",
                   merge_key: str | list[str] | None = None, **kwargs) -> int:
        keys = check_load_mode(mode, merge_key)
        if mode != "upsert":
            return await super().load(data, table, mode=mode, merge_key=merge_key, **kwargs)
        conn = await self._ensure()
        if not data:
            return 0

        columns = list(data[0].keys())
        col_str = ", ".join(columns)
        placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
        stage = "__tau_upsert_" + table.replace(".", "_").replace('"', "")
        key_join = " AND ".join(f"{table}.{k} = {stage}.{k}" for k in keys)
        async with conn.transaction():
            await conn.execute(f"DROP TABLE IF EXISTS {stage}")
            await conn.execute(f"CREATE TEMP TABLE {stage} (LIKE {table})")
            await conn.executemany(
                f"INSERT INTO {stage} ({col_str}) VALUES ({placeholders})",
                [[r.get(c) for c in columns] for r in data],
            )
            await conn.execute(f"DELETE FROM {table} USING {stage} WHERE {key_join}")
            await conn.execute(f"INSERT INTO {table} ({col_str}) SELECT {col_str} FROM {stage}")
            await conn.execute(f"DROP TABLE {stage}")
        return len(data)


def redshift(
    dsn: str | None = None, host: str = "", port: int = 5439,
    database: str = "", user: str = "", password: str = "",
) -> RedshiftConnector:
    """Create a Redshift connector."""
    return RedshiftConnector(dsn=dsn, host=host, port=port, database=database, user=user, password=password)
