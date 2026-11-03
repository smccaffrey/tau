"""Materialization engine — generates and executes SQL for each strategy."""

from __future__ import annotations
from typing import Any, Protocol
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

        if getattr(config, "pre_hook", None):
            await self.executor.execute(config.pre_hook)

        await self.executor.execute(self._drop_table(table))
        await self.executor.execute(self._create_table_as(table, query))

        if getattr(config, "post_hook", None):
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
        now = self._current_timestamp()

        staging = await self._stage(table, f"SELECT * FROM ({query}) __src")
        try:
            # The source may already have a column named like updated_at_column (common with
            # SELECT *). Tau owns that column in the target, so leave the source's copy out.
            cols = [c for c in await self._get_columns(staging) if c != updated_col]
            col_list = ", ".join(cols)
            src_cols = ", ".join(f"s.{c}" for c in cols)

            if not await self._table_exists(table):
                await self.executor.execute(self._create_table_as(
                    table, f"SELECT {src_cols}, {now} AS {updated_col} FROM {staging} s"
                ))
                count = await self._count_rows(table)
                return {"strategy": "scd_type_1", "table": table, "rows": count, "first_run": True}

            tracked = config.tracked_columns or [c for c in cols if c not in unique_key]
            change_check = " OR ".join(self._distinct(f"t.{c}", f"s.{c}") for c in tracked) or "1 = 1"
            key_join = " AND ".join(f"t.{k} = s.{k}" for k in unique_key)
            keys = self._key_tuple(unique_key)

            if self.dialect == "clickhouse":
                # No UPDATE ... FROM: delete the changed rows, then insert changed + new rows.
                # A mutation cannot read the table it mutates, so collect the keys first.
                changed = await self._stage(table, f"""
                    SELECT {', '.join(f's.{k} AS {k}' for k in unique_key)} FROM {staging} s
                    INNER JOIN {table} t ON {key_join} WHERE {change_check}
                """)
                try:
                    await self.executor.execute(
                        f"DELETE FROM {table} WHERE {keys} IN (SELECT {keys} FROM {changed})"
                    )
                finally:
                    await self.executor.execute(self._drop_temp(changed))
                await self.executor.execute(f"""
                    INSERT INTO {table} ({col_list}, {updated_col})
                    SELECT {src_cols}, {now} FROM {staging} s
                    WHERE {self._key_tuple(unique_key, 's')} NOT IN (SELECT {keys} FROM {table})
                """)
            else:
                non_key_cols = [c for c in cols if c not in unique_key]
                sets = [(c, f"s.{c}") for c in non_key_cols] + [(updated_col, now)]
                await self.executor.execute(
                    self._update_join(table, staging, sets, key_join, f"({change_check})")
                )
                await self.executor.execute(f"""
                    INSERT INTO {table} ({col_list}, {updated_col})
                    SELECT {src_cols}, {now}
                    FROM {staging} s
                    LEFT JOIN {table} t ON {key_join}
                    WHERE t.{unique_key[0]} IS NULL
                """)
        finally:
            await self.executor.execute(self._drop_temp(staging))

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
        now = self._current_timestamp()
        tracked = config.tracked_columns
        hash_expr = self._build_hash_expression(tracked) if tracked else "'initial'"

        if not await self._table_exists(table):
            await self.executor.execute(self._create_table_as(table, f"""
                SELECT
                    *,
                    {now} AS {vf},
                    {self._null_timestamp()} AS {vt},
                    TRUE AS {ic},
                    {hash_expr} AS {hc}
                FROM ({query}) __src
            """))
            count = await self._count_rows(table)
            return {"strategy": "scd_type_2", "table": table, "rows": count, "first_run": True}

        staging = await self._stage(
            table, f"SELECT *, {hash_expr} AS __new_hash FROM ({query}) __src", "__staging_scd2_"
        )
        try:
            key_join = " AND ".join(f"t.{k} = s.{k}" for k in unique_key)
            keys = self._key_tuple(unique_key)
            src_cols = [c for c in await self._get_columns(staging) if c != "__new_hash"]
            src_cols_str = ", ".join(f"s.{c}" for c in src_cols)
            insert_cols = f"{', '.join(src_cols)}, {vf}, {vt}, {ic}, {hc}"

            if self.dialect == "clickhouse":
                # Close current rows whose hash changed (synchronous mutation).
                await self.executor.execute(f"""
                    ALTER TABLE {table} UPDATE {vt} = {now}, {ic} = FALSE
                    WHERE {ic} AND {keys} IN (SELECT {keys} FROM {staging})
                    AND ({', '.join(unique_key)}, {hc}) NOT IN
                        (SELECT {', '.join(unique_key)}, __new_hash FROM {staging})
                """)
                # Insert a current version for every key that has none (new or just closed).
                await self.executor.execute(f"""
                    INSERT INTO {table} ({insert_cols})
                    SELECT {src_cols_str}, {now}, NULL, TRUE, s.__new_hash FROM {staging} s
                    WHERE {self._key_tuple(unique_key, 's')} NOT IN
                        (SELECT {keys} FROM {table} WHERE {ic})
                """)
                if config.invalidate_hard_deletes:
                    await self.executor.execute(f"""
                        ALTER TABLE {table} UPDATE {vt} = {now}, {ic} = FALSE
                        WHERE {ic} AND {keys} NOT IN (SELECT {keys} FROM {staging})
                    """)
            else:
                # Close current rows whose hash changed.
                await self.executor.execute(self._update_join(
                    table, staging, [(vt, now), (ic, "FALSE")], key_join,
                    f"t.{ic} = TRUE AND t.{hc} <> s.__new_hash",
                ))
                # Insert a current version for every key that has none (new or just closed).
                await self.executor.execute(f"""
                    INSERT INTO {table} ({insert_cols})
                    SELECT {src_cols_str}, {now}, NULL, TRUE, s.__new_hash
                    FROM {staging} s
                    LEFT JOIN {table} t ON {key_join} AND t.{ic} = TRUE
                    WHERE t.{unique_key[0]} IS NULL
                """)
                if config.invalidate_hard_deletes:
                    await self.executor.execute(f"""
                        UPDATE {table} SET {vt} = {now}, {ic} = FALSE
                        WHERE {ic} = TRUE AND NOT EXISTS (
                            SELECT 1 FROM {staging} s
                            WHERE {" AND ".join(f"{table}.{k} = s.{k}" for k in unique_key)}
                        )
                    """)
        finally:
            await self.executor.execute(self._drop_temp(staging))

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
        select = f"SELECT *, {now} AS {ts_col}, {snap_id} AS {id_col} FROM ({query}) __src"

        if not await self._table_exists(table):
            await self.executor.execute(self._create_table_as(table, select))
        else:
            await self.executor.execute(f"INSERT INTO {table} {select}")
            if config.retain_snapshots:
                await self._prune_snapshots(table, id_col, ts_col, config.retain_snapshots)

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
        if self.dialect == "clickhouse":
            return f"CREATE TABLE {table} ENGINE = MergeTree ORDER BY tuple() AS {query}"
        return f"CREATE TABLE {table} AS {query}"

    async def _stage(self, table: str, query: str, prefix: str = "__staging_") -> str:
        """Materialize ``query`` into a staging table and return its name."""
        if self.dialect == "clickhouse":
            # Mutations (ALTER UPDATE / DELETE) run server-side and cannot see TEMPORARY
            # tables, so stage into a real, uniquely named table that is dropped afterwards.
            name = f"__tau_stage_{uuid.uuid4().hex[:12]}"
            await self.executor.execute(
                f"CREATE TABLE {name} ENGINE = MergeTree ORDER BY tuple() AS {query}"
            )
            return name
        name = f"{prefix}{table.replace('.', '_')}"
        await self.executor.execute(self._drop_temp(name))
        if self.dialect == "bigquery":
            await self.executor.execute(f"CREATE TEMP TABLE {name} AS {query}")
        else:
            await self.executor.execute(f"CREATE TEMPORARY TABLE {name} AS {query}")
        return name

    def _drop_temp(self, name: str) -> str:
        if self.dialect == "mysql":
            return f"DROP TEMPORARY TABLE IF EXISTS {name}"
        return f"DROP TABLE IF EXISTS {name}"

    def _null_timestamp(self) -> str:
        if self.dialect == "mysql":
            return "CAST(NULL AS DATETIME)"
        if self.dialect == "clickhouse":
            return "CAST(NULL AS Nullable(DateTime))"
        return "CAST(NULL AS TIMESTAMP)"

    def _distinct(self, a: str, b: str) -> str:
        """Null-safe "a differs from b"."""
        if self.dialect == "mysql":
            return f"NOT ({a} <=> {b})"
        if self.dialect == "clickhouse":
            return f"NOT (ifNull(({a}) = ({b}), 0) OR (({a}) IS NULL AND ({b}) IS NULL))"
        return f"{a} IS DISTINCT FROM {b}"

    def _key_tuple(self, keys: list[str], alias: str | None = None) -> str:
        cols = [f"{alias}.{k}" if alias else k for k in keys]
        return f"({', '.join(cols)})" if len(cols) > 1 else cols[0]

    def _update_join(self, table: str, staging: str, sets: list[tuple[str, str]],
                     key_join: str, where: str) -> str:
        """``UPDATE table t ... FROM staging s`` in the dialect's syntax (not ClickHouse)."""
        if self.dialect == "mysql":
            set_sql = ", ".join(f"t.{c} = {v}" for c, v in sets)
            return f"UPDATE {table} t JOIN {staging} s ON {key_join} SET {set_sql} WHERE {where}"
        set_sql = ", ".join(f"{c} = {v}" for c, v in sets)
        return f"UPDATE {table} t SET {set_sql} FROM {staging} s WHERE {key_join} AND {where}"

    def _current_timestamp(self) -> str:
        if self.dialect in ("bigquery",):
            return "CURRENT_TIMESTAMP()"
        if self.dialect == "clickhouse":
            return "now()"
        return "CURRENT_TIMESTAMP"

    def _build_hash_expression(self, columns: list[str]) -> str:
        if self.dialect == "bigquery":
            concat = " || '|' || ".join(f"COALESCE(CAST({c} AS STRING), '')" for c in columns)
            return f"TO_HEX(MD5({concat}))"
        if self.dialect == "clickhouse":
            concat_inner = ", '|', ".join(f"COALESCE(toString({c}), '')" for c in columns)
            return f"lower(hex(MD5(concat({concat_inner}))))"
        if self.dialect == "mysql":
            parts = ", ".join(f"COALESCE(CAST({c} AS CHAR), '')" for c in columns)
            return f"MD5(CONCAT_WS('|', {parts}))"
        concat = " || '|' || ".join(f"COALESCE(CAST({c} AS VARCHAR), '')" for c in columns)
        return f"MD5({concat})"

    async def _table_exists(self, table: str) -> bool:
        try:
            await self.executor.extract(f"SELECT 1 FROM {table} LIMIT 0")
            return True
        except Exception:
            return False

    async def _count_rows(self, table: str) -> int:
        try:
            rows = await self.executor.extract(f"SELECT COUNT(*) AS cnt FROM {table}")
            return int(rows[0].get("cnt", rows[0].get("count", 0))) if rows else 0
        except Exception:
            return -1

    async def _get_max_value(self, table: str, column: str) -> str | None:
        rows = await self.executor.extract(f"SELECT MAX({column}) as max_val FROM {table}")
        if rows and rows[0].get("max_val"):
            return str(rows[0]["max_val"])
        return None

    async def _get_columns(self, table: str) -> list[str]:
        """Column names of a table or staging table.

        Connectors that can (postgres, redshift, mysql, clickhouse, duckdb) answer exactly via
        ``get_columns``; otherwise fall back to sampling a row or ``DESCRIBE``.
        """
        get_columns = getattr(self.executor, "get_columns", None)
        if get_columns is not None:
            return await get_columns(table)
        rows = await self.executor.extract(f"SELECT * FROM {table} LIMIT 1")
        if rows:
            return list(rows[0].keys())
        try:
            desc_rows = await self.executor.extract(f"DESCRIBE {table}")
            return [r.get("column_name", r.get("name", list(r.values())[0])) for r in desc_rows]
        except Exception:
            return []

    async def _merge_into(self, table: str, source_query: str, unique_key: list[str],
                          on_schema_change: str = "ignore") -> None:
        if self.dialect == "clickhouse":
            # ClickHouse has no UPDATE ... FROM: replace matching rows instead.
            await self._delete_insert(table, source_query, unique_key)
            return

        staging = await self._stage(table, f"SELECT * FROM ({source_query}) __src", "__merge_staging_")
        try:
            key_join = " AND ".join(f"t.{k} = s.{k}" for k in unique_key)
            cols = await self._get_columns(staging)
            non_key_cols = [c for c in cols if c not in unique_key]

            if non_key_cols:
                await self.executor.execute(self._update_join(
                    table, staging, [(c, f"s.{c}") for c in non_key_cols], key_join, "1 = 1"
                ))

            col_str = ", ".join(cols)
            src_col_str = ", ".join(f"s.{c}" for c in cols)
            await self.executor.execute(f"""
                INSERT INTO {table} ({col_str})
                SELECT {src_col_str} FROM {staging} s
                LEFT JOIN {table} t ON {key_join}
                WHERE t.{unique_key[0]} IS NULL
            """)
        finally:
            await self.executor.execute(self._drop_temp(staging))

    async def _delete_insert(self, table: str, source_query: str, unique_key: list[str]) -> None:
        staging = await self._stage(table, f"SELECT * FROM ({source_query}) __src", "__di_staging_")
        try:
            keys = self._key_tuple(unique_key)
            await self.executor.execute(
                f"DELETE FROM {table} WHERE {keys} IN (SELECT {keys} FROM {staging})"
            )
            cols = ", ".join(await self._get_columns(staging))
            await self.executor.execute(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM {staging}")
        finally:
            await self.executor.execute(self._drop_temp(staging))

    async def _insert_overwrite(self, table: str, source_query: str, partition_col: str) -> None:
        staging = await self._stage(table, f"SELECT * FROM ({source_query}) __src", "__io_staging_")
        try:
            # Delete rows in partitions that appear in source
            await self.executor.execute(f"""
                DELETE FROM {table}
                WHERE {partition_col} IN (SELECT DISTINCT {partition_col} FROM {staging})
            """)
            cols = ", ".join(await self._get_columns(staging))
            await self.executor.execute(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM {staging}")
        finally:
            await self.executor.execute(self._drop_temp(staging))

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

    async def _prune_snapshots(self, table: str, id_col: str, ts_col: str, retain: int) -> None:
        """Keep the newest ``retain`` snapshots (by snapshot id, so equal timestamps are fine)."""
        rows = await self.executor.extract(f"""
            SELECT {id_col} AS sid, MAX({ts_col}) AS ts FROM {table}
            GROUP BY {id_col} ORDER BY ts DESC
        """)
        stale = [r["sid"] for r in rows[retain:]]
        if stale:
            ids = ", ".join("'" + str(s).replace("'", "''") + "'" for s in stale)
            await self.executor.execute(f"DELETE FROM {table} WHERE {id_col} IN ({ids})")

