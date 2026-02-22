"""Materialization engine — generates and executes SQL for each strategy."""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, Protocol
import hashlib
import uuid

from tau.materializations.strategies import (
    MaterializationConfig,
    MaterializationType,
    FullRefreshConfig,
    IncrementalConfig,
    PartitionedConfig,
    SCDType1Config,
    SCDType2Config,
    SnapshotConfig,
)


class SQLExecutor(Protocol):
    """Any connector that can execute SQL."""
    async def execute(self, query: str, params: Any = None) -> Any: ...
    async def extract(self, query: str, params: Any = None, **kwargs) -> list[dict]: ...


class MaterializationEngine:
    """Generates and executes SQL for materialization strategies."""

    def __init__(self, executor: SQLExecutor, dialect: str = "postgres"):
        """
        Args:
            executor: A connector that implements execute() and extract()
            dialect: SQL dialect (postgres, bigquery, snowflake, clickhouse, mysql, duckdb)
        """
        self.executor = executor
        self.dialect = dialect

    async def materialize(self, config: MaterializationConfig) -> dict:
        """Execute a materialization strategy. Returns stats."""
        handler = {
            MaterializationType.FULL_REFRESH: self._full_refresh,
            MaterializationType.INCREMENTAL: self._incremental,
            MaterializationType.PARTITIONED: self._partitioned,
            MaterializationType.SCD_TYPE_1: self._scd_type_1,
            MaterializationType.SCD_TYPE_2: self._scd_type_2,
            MaterializationType.SNAPSHOT: self._snapshot,
            MaterializationType.APPEND_ONLY: self._append_only,
            MaterializationType.VIEW: self._view,
        }.get(config.strategy)

        if not handler:
            raise ValueError(f"Unknown materialization strategy: {config.strategy}")

        return await handler(config)

    # ─── Full Refresh ───

    async def _full_refresh(self, config: FullRefreshConfig) -> dict:
        table = config.target_table
        query = config.source_query

        if hasattr(config, 'pre_hook') and config.pre_hook:
            await self.executor.execute(config.pre_hook)

        await self.executor.execute(self._drop_table(table))
        await self.executor.execute(self._create_table_as(table, query))

        if hasattr(config, 'post_hook') and config.post_hook:
            await self.executor.execute(config.post_hook)

        count = await self._count_rows(table)
        return {"strategy": "full_refresh", "table": table, "rows": count}

    # ─── Incremental ───

    async def _incremental(self, config: IncrementalConfig) -> dict:
        table = config.target_table
        query = config.source_query
        unique_key = self._normalize_keys(config.unique_key)

        # Check if table exists
        exists = await self._table_exists(table)

        if not exists:
            # First run: create table from query
            await self.executor.execute(self._create_table_as(table, query))
            count = await self._count_rows(table)
            return {"strategy": "incremental", "table": table, "rows": count, "first_run": True}

        # Get max value of incremental column for filtering
        if config.incremental_column:
            max_val = await self._get_max_value(table, config.incremental_column)
            # Inject the filter into the source query
            filtered_query = f"""
                SELECT * FROM ({query}) __src
                WHERE {config.incremental_column} > '{max_val}'
            """ if max_val else query
        else:
            filtered_query = query

        if config.incremental_strategy == "merge" and unique_key:
            await self._merge_into(table, filtered_query, unique_key, config.on_schema_change)
        elif config.incremental_strategy == "delete+insert" and unique_key:
            await self._delete_insert(table, filtered_query, unique_key)
        elif config.incremental_strategy == "insert_overwrite":
            await self._insert_overwrite(table, filtered_query, config.incremental_column)
        else:
            # Default: simple append
            await self.executor.execute(
                f"INSERT INTO {table} SELECT * FROM ({filtered_query}) __src"
            )

        count = await self._count_rows(table)
        return {"strategy": "incremental", "table": table, "rows": count, "first_run": False}

    # ─── Partitioned ───

    async def _partitioned(self, config: PartitionedConfig) -> dict:
        table = config.target_table
        query = config.source_query
        unique_key = self._normalize_keys(config.unique_key)

        exists = await self._table_exists(table)

        if not exists:
            create_sql = self._create_partitioned_table(table, query, config)
            await self.executor.execute(create_sql)
            count = await self._count_rows(table)
            return {"strategy": "partitioned", "table": table, "rows": count, "first_run": True}

        # Incremental into partitioned table
        if config.incremental_column:
            max_val = await self._get_max_value(table, config.incremental_column)
            filtered_query = f"""
                SELECT * FROM ({query}) __src
                WHERE {config.incremental_column} > '{max_val}'
            """ if max_val else query
        else:
            filtered_query = query

        if unique_key:
            await self._merge_into(table, filtered_query, unique_key)
        else:
            await self.executor.execute(
                f"INSERT INTO {table} SELECT * FROM ({filtered_query}) __src"
            )

        # Expire old partitions if configured
        if config.partition_expiration_days:
            await self._expire_partitions(table, config)

        count = await self._count_rows(table)
        return {"strategy": "partitioned", "table": table, "rows": count, "first_run": False}

    # ─── SCD Type 1 ───

    async def _scd_type_1(self, config: SCDType1Config) -> dict:
        table = config.target_table
        query = config.source_query
        unique_key = self._normalize_keys(config.unique_key)
        updated_col = config.updated_at_column

        exists = await self._table_exists(table)

        if not exists:
            # Create with updated_at column
            await self.executor.execute(
                f"""CREATE TABLE {table} AS
                SELECT *, {self._current_timestamp()} AS {updated_col}
                FROM ({query}) __src"""
            )
            count = await self._count_rows(table)
            return {"strategy": "scd_type_1", "table": table, "rows": count, "first_run": True}

        # Get source columns for tracked columns comparison
        tracked = config.tracked_columns

        # Merge: update changed rows, insert new ones
        staging = f"__staging_{table.replace('.', '_')}"
        await self.executor.execute(
            f"CREATE TEMP TABLE {staging} AS SELECT * FROM ({query}) __src"
        )

        # Update existing rows where tracked columns changed
        key_join = " AND ".join(f"t.{k} = s.{k}" for k in unique_key)
        if tracked:
            change_check = " OR ".join(
                f"t.{c} IS DISTINCT FROM s.{c}" for c in tracked
            )
        else:
            change_check = "TRUE"  # Update all matching rows

        # Get all columns from staging
        cols = await self._get_columns(staging)
        non_key_cols = [c for c in cols if c not in unique_key]
        update_set = ", ".join(f"{c} = s.{c}" for c in non_key_cols)
        update_set += f", {updated_col} = {self._current_timestamp()}"

        await self.executor.execute(f"""
            UPDATE {table} t SET {update_set}
            FROM {staging} s
            WHERE {key_join} AND ({change_check})
        """)

        # Insert new rows
        await self.executor.execute(f"""
            INSERT INTO {table}
            SELECT s.*, {self._current_timestamp()} AS {updated_col}
            FROM {staging} s
            LEFT JOIN {table} t ON {key_join}
            WHERE t.{unique_key[0]} IS NULL
        """)

        await self.executor.execute(f"DROP TABLE IF EXISTS {staging}")

        count = await self._count_rows(table)
        return {"strategy": "scd_type_1", "table": table, "rows": count, "first_run": False}

    # ─── SCD Type 2 ───

    async def _scd_type_2(self, config: SCDType2Config) -> dict:
        table = config.target_table
        query = config.source_query
        unique_key = self._normalize_keys(config.unique_key)
        vf = config.valid_from_column
        vt = config.valid_to_column
        ic = config.is_current_column
        hc = config.hash_column

        exists = await self._table_exists(table)
        now = self._current_timestamp()

        if not exists:
            # Create initial table with SCD2 columns
            tracked = config.tracked_columns
            hash_expr = self._build_hash_expression(tracked) if tracked else f"'initial'"

            await self.executor.execute(f"""
                CREATE TABLE {table} AS
                SELECT
                    *,
                    {now} AS {vf},
                    CAST(NULL AS TIMESTAMP) AS {vt},
                    TRUE AS {ic},
                    {hash_expr} AS {hc}
                FROM ({query}) __src
            """)
            count = await self._count_rows(table)
            return {"strategy": "scd_type_2", "table": table, "rows": count, "first_run": True}

        # Stage incoming data
        tracked = config.tracked_columns
        hash_expr = self._build_hash_expression(tracked) if tracked else f"'hash'"

        staging = f"__staging_scd2_{table.replace('.', '_')}"
        await self.executor.execute(f"""
            CREATE TEMP TABLE {staging} AS
            SELECT *, {hash_expr} AS __new_hash
            FROM ({query}) __src
        """)

        key_join = " AND ".join(f"t.{k} = s.{k}" for k in unique_key)

        # Close changed records (set valid_to, is_current = FALSE)
        await self.executor.execute(f"""
            UPDATE {table} t SET
                {vt} = {now},
                {ic} = FALSE
            FROM {staging} s
            WHERE {key_join}
                AND t.{ic} = TRUE
                AND t.{hc} != s.__new_hash
        """)

        # Insert new versions of changed records
        all_cols = await self._get_columns(staging)
        src_cols = [c for c in all_cols if c != "__new_hash"]
        src_cols_str = ", ".join(f"s.{c}" for c in src_cols)

        await self.executor.execute(f"""
            INSERT INTO {table} ({', '.join(src_cols)}, {vf}, {vt}, {ic}, {hc})
            SELECT {src_cols_str}, {now}, NULL, TRUE, s.__new_hash
            FROM {staging} s
            INNER JOIN {table} t ON {key_join}
            WHERE t.{ic} = FALSE
                AND t.{vt} = {now}
                AND t.{hc} != s.__new_hash
        """)

        # Insert brand new records (no match in target)
        await self.executor.execute(f"""
            INSERT INTO {table} ({', '.join(src_cols)}, {vf}, {vt}, {ic}, {hc})
            SELECT {src_cols_str}, {now}, NULL, TRUE, s.__new_hash
            FROM {staging} s
            LEFT JOIN {table} t ON {key_join} AND t.{ic} = TRUE
            WHERE t.{unique_key[0]} IS NULL
        """)

        # Handle hard deletes
        if config.invalidate_hard_deletes:
            await self.executor.execute(f"""
                UPDATE {table} t SET
                    {vt} = {now},
                    {ic} = FALSE
                WHERE t.{ic} = TRUE
                AND NOT EXISTS (
                    SELECT 1 FROM {staging} s WHERE {key_join}
                )
            """)

        await self.executor.execute(f"DROP TABLE IF EXISTS {staging}")

        count = await self._count_rows(table)
        return {"strategy": "scd_type_2", "table": table, "rows": count, "first_run": False}

    # ─── Snapshot ───

    async def _snapshot(self, config: SnapshotConfig) -> dict:
        table = config.target_table
        query = config.source_query
        ts_col = config.snapshot_timestamp_column
        id_col = config.snapshot_id_column
        now = self._current_timestamp()
        snap_id = f"'{str(uuid.uuid4())[:8]}'"

        exists = await self._table_exists(table)

        if not exists:
            await self.executor.execute(f"""
                CREATE TABLE {table} AS
                SELECT *, {now} AS {ts_col}, {snap_id} AS {id_col}
                FROM ({query}) __src
            """)
        else:
            # Append new snapshot
            await self.executor.execute(f"""
                INSERT INTO {table}
                SELECT *, {now} AS {ts_col}, {snap_id} AS {id_col}
                FROM ({query}) __src
            """)

            # Prune old snapshots if retain limit set
            if config.retain_snapshots:
                await self._prune_snapshots(table, ts_col, config.retain_snapshots)

        count = await self._count_rows(table)
        return {"strategy": "snapshot", "table": table, "rows": count, "snapshot_id": snap_id}

    # ─── Append Only ───

    async def _append_only(self, config: MaterializationConfig) -> dict:
        table = config.target_table
        query = config.source_query

        exists = await self._table_exists(table)
        if not exists:
            await self.executor.execute(self._create_table_as(table, query))
        else:
            await self.executor.execute(
                f"INSERT INTO {table} SELECT * FROM ({query}) __src"
            )

        count = await self._count_rows(table)
        return {"strategy": "append_only", "table": table, "rows": count}

    # ─── View ───

    async def _view(self, config: MaterializationConfig) -> dict:
        table = config.target_table
        query = config.source_query
        await self.executor.execute(f"CREATE OR REPLACE VIEW {table} AS {query}")
        return {"strategy": "view", "table": table}

    # ─── SQL Helpers (dialect-aware) ───

    def _normalize_keys(self, key: str | list[str]) -> list[str]:
        if isinstance(key, str):
            return [k.strip() for k in key.split(",") if k.strip()]
        return key

    def _drop_table(self, table: str) -> str:
        return f"DROP TABLE IF EXISTS {table}"

    def _create_table_as(self, table: str, query: str) -> str:
        return f"CREATE TABLE {table} AS {query}"

    def _current_timestamp(self) -> str:
        if self.dialect in ("bigquery",):
            return "CURRENT_TIMESTAMP()"
        return "CURRENT_TIMESTAMP"

    def _build_hash_expression(self, columns: list[str]) -> str:
        concat = " || '|' || ".join(f"COALESCE(CAST({c} AS VARCHAR), '')" for c in columns)
        if self.dialect == "bigquery":
            concat = " || '|' || ".join(f"COALESCE(CAST({c} AS STRING), '')" for c in columns)
            return f"MD5({concat})"
        elif self.dialect == "snowflake":
            return f"MD5({concat})"
        elif self.dialect == "clickhouse":
            concat_inner = ", ".join(f"COALESCE(toString({c}), '')" for c in columns)
            return f"MD5(concat({concat_inner}))"
        return f"MD5({concat})"

    async def _table_exists(self, table: str) -> bool:
        try:
            await self.executor.extract(f"SELECT 1 FROM {table} LIMIT 0")
            return True
        except Exception:
            return False

    async def _count_rows(self, table: str) -> int:
        try:
            rows = await self.executor.extract(f"SELECT COUNT(*) as cnt FROM {table}")
            return rows[0].get("cnt", rows[0].get("count", 0)) if rows else 0
        except Exception:
            return -1

    async def _get_max_value(self, table: str, column: str) -> str | None:
        rows = await self.executor.extract(f"SELECT MAX({column}) as max_val FROM {table}")
        if rows and rows[0].get("max_val"):
            return str(rows[0]["max_val"])
        return None

    async def _get_columns(self, table: str) -> list[str]:
        rows = await self.executor.extract(f"SELECT * FROM {table} LIMIT 0")
        # This won't work for empty result sets in all dialects
        # Fallback: query information_schema or DESCRIBE
        if not rows:
            try:
                if self.dialect in ("postgres", "redshift", "mysql"):
                    schema_rows = await self.executor.extract(
                        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table.split('.')[-1]}' ORDER BY ordinal_position"
                    )
                    return [r["column_name"] for r in schema_rows]
                else:
                    desc_rows = await self.executor.extract(f"DESCRIBE {table}")
                    return [r.get("column_name", r.get("name", list(r.values())[0])) for r in desc_rows]
            except Exception:
                return []
        return list(rows[0].keys())

    async def _merge_into(self, table: str, source_query: str, unique_key: list[str],
                          on_schema_change: str = "ignore") -> None:
        staging = f"__merge_staging_{table.replace('.', '_')}"
        await self.executor.execute(f"CREATE TEMP TABLE {staging} AS SELECT * FROM ({source_query}) __src")

        key_join = " AND ".join(f"t.{k} = s.{k}" for k in unique_key)
        cols = await self._get_columns(staging)
        non_key_cols = [c for c in cols if c not in unique_key]

        if non_key_cols:
            update_set = ", ".join(f"{c} = s.{c}" for c in non_key_cols)
            await self.executor.execute(f"""
                UPDATE {table} t SET {update_set}
                FROM {staging} s WHERE {key_join}
            """)

        col_str = ", ".join(cols)
        src_col_str = ", ".join(f"s.{c}" for c in cols)
        await self.executor.execute(f"""
            INSERT INTO {table} ({col_str})
            SELECT {src_col_str} FROM {staging} s
            LEFT JOIN {table} t ON {key_join}
            WHERE t.{unique_key[0]} IS NULL
        """)

        await self.executor.execute(f"DROP TABLE IF EXISTS {staging}")

    async def _delete_insert(self, table: str, source_query: str, unique_key: list[str]) -> None:
        staging = f"__di_staging_{table.replace('.', '_')}"
        await self.executor.execute(f"CREATE TEMP TABLE {staging} AS SELECT * FROM ({source_query}) __src")

        key_join = " AND ".join(f"{table}.{k} = {staging}.{k}" for k in unique_key)
        await self.executor.execute(f"DELETE FROM {table} WHERE EXISTS (SELECT 1 FROM {staging} WHERE {key_join})")

        await self.executor.execute(f"INSERT INTO {table} SELECT * FROM {staging}")
        await self.executor.execute(f"DROP TABLE IF EXISTS {staging}")

    async def _insert_overwrite(self, table: str, source_query: str, partition_col: str) -> None:
        staging = f"__io_staging_{table.replace('.', '_')}"
        await self.executor.execute(f"CREATE TEMP TABLE {staging} AS SELECT * FROM ({source_query}) __src")

        # Delete rows in partitions that appear in source
        await self.executor.execute(f"""
            DELETE FROM {table}
            WHERE {partition_col} IN (SELECT DISTINCT {partition_col} FROM {staging})
        """)
        await self.executor.execute(f"INSERT INTO {table} SELECT * FROM {staging}")
        await self.executor.execute(f"DROP TABLE IF EXISTS {staging}")

    def _create_partitioned_table(self, table: str, query: str, config: PartitionedConfig) -> str:
        base = f"CREATE TABLE {table} AS {query}"

        if self.dialect == "bigquery":
            partition_clause = f"PARTITION BY DATE_TRUNC({config.partition_by}, {config.partition_granularity.upper()})"
            cluster_clause = f"CLUSTER BY {', '.join(config.cluster_by)}" if config.cluster_by else ""
            return f"CREATE TABLE {table} {partition_clause} {cluster_clause} AS {query}"
        elif self.dialect == "clickhouse":
            return f"CREATE TABLE {table} ENGINE = MergeTree() PARTITION BY {config.partition_by} ORDER BY ({', '.join(config.cluster_by) or config.partition_by}) AS {query}"

        # For Postgres, Snowflake, DuckDB, etc. — just create the table (partitioning handled differently)
        return base

    async def _expire_partitions(self, table: str, config: PartitionedConfig) -> None:
        if not config.partition_expiration_days or not config.partition_by:
            return
        days = config.partition_expiration_days
        await self.executor.execute(f"""
            DELETE FROM {table}
            WHERE {config.partition_by} < CURRENT_DATE - INTERVAL '{days} days'
        """)

    async def _prune_snapshots(self, table: str, ts_col: str, retain: int) -> None:
        await self.executor.execute(f"""
            DELETE FROM {table}
            WHERE {ts_col} NOT IN (
                SELECT DISTINCT {ts_col} FROM {table}
                ORDER BY {ts_col} DESC
                LIMIT {retain}
            )
        """)
