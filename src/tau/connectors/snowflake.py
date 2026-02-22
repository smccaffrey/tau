"""Snowflake connector."""

from __future__ import annotations
import asyncio
from functools import partial
from tau.connectors.base import Connector


class SnowflakeConnector(Connector):
    """Connect to Snowflake for extract and load operations."""

    def __init__(
        self,
        account: str,
        user: str,
        password: str | None = None,
        private_key_path: str | None = None,
        warehouse: str | None = None,
        database: str | None = None,
        schema: str = "PUBLIC",
        role: str | None = None,
    ):
        self.account = account
        self.user = user
        self.password = password
        self.private_key_path = private_key_path
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self._conn = None

    async def connect(self) -> None:
        try:
            import snowflake.connector
        except ImportError:
            raise ImportError("Install snowflake-connector-python: pip install tau-pipelines[snowflake]")

        kwargs = {
            "account": self.account,
            "user": self.user,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
        }
        if self.password:
            kwargs["password"] = self.password
        if self.role:
            kwargs["role"] = self.role
        if self.private_key_path:
            from cryptography.hazmat.primitives import serialization
            with open(self.private_key_path, "rb") as f:
                p_key = serialization.load_pem_private_key(f.read(), password=None)
            kwargs["private_key"] = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

        self._conn = snowflake.connector.connect(**kwargs)

    async def disconnect(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    async def extract(self, query: str, params: dict | None = None, **kwargs) -> list[dict]:
        if not self._conn:
            await self.connect()
        import snowflake.connector
        loop = asyncio.get_event_loop()
        def _extract():
            cursor = self._conn.cursor(snowflake.connector.DictCursor)
            try:
                cursor.execute(query, params)
                return cursor.fetchall()
            finally:
                cursor.close()
        return await loop.run_in_executor(None, _extract)

    async def load(
        self,
        data: list[dict],
        table: str,
        mode: str = "append",
        **kwargs,
    ) -> int:
        if not self._conn or not data:
            return 0

        loop = asyncio.get_event_loop()
        def _load():
            cursor = self._conn.cursor()
            try:
                if mode == "replace":
                    cursor.execute(f"TRUNCATE TABLE IF EXISTS {table}")
                columns = list(data[0].keys())
                col_str = ", ".join(columns)
                placeholders = ", ".join(["%s"] * len(columns))
                for record in data:
                    values = [record.get(c) for c in columns]
                    cursor.execute(f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})", values)
                return len(data)
            finally:
                cursor.close()
        return await loop.run_in_executor(None, _load)

    async def execute(self, query: str, params: dict | None = None) -> None:
        if not self._conn:
            await self.connect()
        loop = asyncio.get_event_loop()
        def _execute():
            cursor = self._conn.cursor()
            try:
                cursor.execute(query, params)
            finally:
                cursor.close()
        await loop.run_in_executor(None, _execute)

    async def get_schema(self, table: str) -> list[dict]:
        return await self.extract(f"DESCRIBE TABLE {table}")


def snowflake(
    account: str,
    user: str,
    password: str | None = None,
    private_key_path: str | None = None,
    warehouse: str | None = None,
    database: str | None = None,
    schema: str = "PUBLIC",
    role: str | None = None,
) -> SnowflakeConnector:
    """Create a Snowflake connector."""
    return SnowflakeConnector(
        account=account, user=user, password=password,
        private_key_path=private_key_path, warehouse=warehouse,
        database=database, schema=schema, role=role,
    )
