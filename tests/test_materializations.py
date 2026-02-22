"""Tests for materialization strategies and engine."""

import pytest
from tau.materializations.strategies import (
    MaterializationType,
    MaterializationConfig,
    FullRefreshConfig,
    IncrementalConfig,
    PartitionedConfig,
    SCDType1Config,
    SCDType2Config,
    SnapshotConfig,
)
from tau.materializations.engine import MaterializationEngine


class MockSQLExecutor:
    """In-memory SQL executor for testing materializations."""

    def __init__(self):
        self.tables: dict[str, list[dict]] = {}
        self.views: dict[str, str] = {}
        self.executed: list[str] = []
        self._query_results: dict[str, list[dict]] = {}

    def set_query_result(self, query_substring: str, result: list[dict]):
        """Pre-configure results for queries matching a substring."""
        self._query_results[query_substring] = result

    async def execute(self, query: str, params=None) -> str:
        self.executed.append(query.strip())
        q = query.strip().upper()

        if q.startswith("DROP TABLE IF EXISTS"):
            table = self._extract_table(query, "DROP TABLE IF EXISTS")
            self.tables.pop(table, None)

        elif q.startswith("CREATE TABLE") and " AS " in q:
            table = self._extract_table_create_as(query)
            # Get data from source query
            source_query = query.split(" AS ", 1)[1].strip()
            data = self._resolve_query(source_query)
            self.tables[table] = list(data)

        elif q.startswith("CREATE TEMP TABLE") and " AS " in q:
            table = self._extract_table_create_as(query)
            source_query = query.split(" AS ", 1)[1].strip()
            data = self._resolve_query(source_query)
            self.tables[table] = list(data)

        elif q.startswith("CREATE OR REPLACE VIEW"):
            parts = query.split(" AS ", 1)
            view_name = parts[0].replace("CREATE OR REPLACE VIEW", "").strip()
            self.views[view_name] = parts[1].strip() if len(parts) > 1 else ""

        elif q.startswith("INSERT INTO"):
            table = query.split("INSERT INTO")[1].strip().split()[0]
            if "SELECT" in q:
                source = query.split("SELECT", 1)[1]
                data = self._resolve_query(f"SELECT {source}")
                if table in self.tables:
                    self.tables[table].extend(data)
                else:
                    self.tables[table] = list(data)

        elif q.startswith("DELETE FROM"):
            table = query.split("DELETE FROM")[1].strip().split()[0]
            # Simplified: just track it was called
            pass

        elif q.startswith("UPDATE"):
            # Simplified: track it was called
            pass

        elif q.startswith("TRUNCATE"):
            table = query.split()[-1]
            if table in self.tables:
                self.tables[table] = []

        return "OK"

    async def extract(self, query: str, params=None, **kwargs) -> list[dict]:
        self.executed.append(query.strip())
        q = query.strip().upper()

        # SELECT 1 FROM table LIMIT 0 (existence check) — must come before generic matching
        if "SELECT 1 FROM" in q and "LIMIT 0" in q:
            table = self._extract_from_table(query)
            if table and table in self.tables:
                return []
            raise Exception(f"Table {table} does not exist")

        # Check pre-configured results
        for substring, result in self._query_results.items():
            if substring.upper() in q:
                return result

        # SELECT COUNT(*)
        if "COUNT(*)" in q:
            table = self._extract_from_table(query)
            if table and table in self.tables:
                return [{"cnt": len(self.tables[table])}]
            return [{"cnt": 0}]

        # SELECT MAX(col)
        if "MAX(" in q:
            return [{"max_val": None}]

        # SELECT * FROM table LIMIT 0 (column check)
        if "LIMIT 0" in q:
            table = self._extract_from_table(query)
            if table and table in self.tables and self.tables[table]:
                return [self.tables[table][0]]
            return []

        # DESCRIBE
        if q.startswith("DESCRIBE"):
            return []

        # SELECT column_name FROM information_schema
        if "INFORMATION_SCHEMA" in q:
            table = query.split("'")[-2] if "'" in query else ""
            if table in self.tables and self.tables[table]:
                return [{"column_name": k} for k in self.tables[table][0].keys()]
            return []

        # General SELECT
        table = self._extract_from_table(query)
        if table and table in self.tables:
            return list(self.tables[table])

        return []

    def _extract_table(self, query: str, prefix: str) -> str:
        return query.replace(prefix, "").strip().split()[0].strip(";")

    def _extract_table_create_as(self, query: str) -> str:
        # CREATE [TEMP] TABLE name AS ...
        q = query.strip()
        parts = q.split(" AS ", 1)[0].split()
        return parts[-1]

    def _extract_from_table(self, query: str) -> str | None:
        q = query.upper()
        if "FROM" in q:
            parts = query.split("FROM")[-1].strip().split()
            if parts:
                return parts[0].strip(";").strip(")")
        return None

    def _resolve_query(self, query: str) -> list[dict]:
        """Try to resolve a query to data."""
        # Check pre-configured results first
        for substring, result in self._query_results.items():
            if substring.upper() in query.upper():
                return result

        # Try to find a table reference
        table = self._extract_from_table(query)
        if table and table in self.tables:
            return list(self.tables[table])
        return []


# ─── Strategy Config Tests ───

class TestStrategyConfigs:
    def test_full_refresh_defaults(self):
        config = FullRefreshConfig(target_table="t", source_query="SELECT 1")
        assert config.strategy == MaterializationType.FULL_REFRESH

    def test_incremental_defaults(self):
        config = IncrementalConfig(
            target_table="t", source_query="SELECT 1",
            unique_key="id", incremental_column="updated_at",
        )
        assert config.strategy == MaterializationType.INCREMENTAL
        assert config.incremental_strategy == "merge"
        assert config.on_schema_change == "append_new_columns"

    def test_partitioned_defaults(self):
        config = PartitionedConfig(
            target_table="t", source_query="SELECT 1",
            partition_by="created_date", partition_type="date",
        )
        assert config.partition_granularity == "day"
        assert config.cluster_by == []

    def test_scd_type_1_defaults(self):
        config = SCDType1Config(
            target_table="t", source_query="SELECT 1",
            unique_key="id",
        )
        assert config.updated_at_column == "updated_at"

    def test_scd_type_2_defaults(self):
        config = SCDType2Config(
            target_table="t", source_query="SELECT 1",
            unique_key="customer_id",
        )
        assert config.valid_from_column == "valid_from"
        assert config.valid_to_column == "valid_to"
        assert config.is_current_column == "is_current"
        assert config.hash_column == "row_hash"
        assert config.invalidate_hard_deletes is True

    def test_snapshot_defaults(self):
        config = SnapshotConfig(
            target_table="t", source_query="SELECT 1",
        )
        assert config.snapshot_timestamp_column == "snapshot_at"
        assert config.snapshot_id_column == "snapshot_id"
        assert config.retain_snapshots is None

    def test_unique_key_as_list(self):
        config = IncrementalConfig(
            target_table="t", source_query="SELECT 1",
            unique_key=["id", "date"],
        )
        assert config.unique_key == ["id", "date"]

    def test_unique_key_as_string(self):
        config = IncrementalConfig(
            target_table="t", source_query="SELECT 1",
            unique_key="id",
        )
        assert config.unique_key == "id"


# ─── Engine Tests ───

class TestMaterializationEngine:
    def _make_engine(self, executor=None, dialect="postgres"):
        return MaterializationEngine(executor=executor or MockSQLExecutor(), dialect=dialect)

    @pytest.mark.asyncio
    async def test_full_refresh_first_run(self):
        executor = MockSQLExecutor()
        executor.set_query_result("SELECT 1", [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
        engine = self._make_engine(executor)

        config = FullRefreshConfig(target_table="users", source_query="SELECT 1")
        result = await engine.materialize(config)

        assert result["strategy"] == "full_refresh"
        assert result["table"] == "users"
        assert "users" in executor.tables

    @pytest.mark.asyncio
    async def test_full_refresh_drops_existing(self):
        executor = MockSQLExecutor()
        executor.tables["users"] = [{"id": 1}]
        executor.set_query_result("SELECT 1", [{"id": 2}])
        engine = self._make_engine(executor)

        config = FullRefreshConfig(target_table="users", source_query="SELECT 1")
        await engine.materialize(config)

        # Should have executed DROP TABLE
        assert any("DROP TABLE" in q.upper() for q in executor.executed)

    @pytest.mark.asyncio
    async def test_full_refresh_with_hooks(self):
        executor = MockSQLExecutor()
        executor.set_query_result("SELECT 1", [])
        engine = self._make_engine(executor)

        config = FullRefreshConfig(
            target_table="t", source_query="SELECT 1",
            pre_hook="CREATE INDEX idx ON t(id)",
            post_hook="ANALYZE t",
        )
        await engine.materialize(config)

        assert any("CREATE INDEX" in q for q in executor.executed)
        assert any("ANALYZE" in q for q in executor.executed)

    @pytest.mark.asyncio
    async def test_incremental_first_run_creates_table(self):
        executor = MockSQLExecutor()
        executor.set_query_result("SELECT 1", [{"id": 1}, {"id": 2}])
        engine = self._make_engine(executor)

        config = IncrementalConfig(
            target_table="orders", source_query="SELECT 1",
            unique_key="id", incremental_column="updated_at",
        )
        result = await engine.materialize(config)

        assert result["first_run"] is True
        assert result["strategy"] == "incremental"

    @pytest.mark.asyncio
    async def test_incremental_subsequent_run(self):
        executor = MockSQLExecutor()
        executor.tables["orders"] = [{"id": 1, "amount": 100}]
        executor.set_query_result("SELECT *", [{"id": 2, "amount": 200}])
        engine = self._make_engine(executor)

        config = IncrementalConfig(
            target_table="orders", source_query="SELECT * FROM raw_orders",
            unique_key="id",
        )
        result = await engine.materialize(config)

        assert result["first_run"] is False
        assert result["strategy"] == "incremental"

    @pytest.mark.asyncio
    async def test_append_only_first_run(self):
        executor = MockSQLExecutor()
        executor.set_query_result("SELECT 1", [{"id": 1}])
        engine = self._make_engine(executor)

        config = MaterializationConfig(
            target_table="events", source_query="SELECT 1",
            strategy=MaterializationType.APPEND_ONLY,
        )
        result = await engine.materialize(config)
        assert result["strategy"] == "append_only"

    @pytest.mark.asyncio
    async def test_append_only_subsequent(self):
        executor = MockSQLExecutor()
        executor.tables["events"] = [{"id": 1}]
        executor.set_query_result("SELECT 1", [{"id": 2}])
        engine = self._make_engine(executor)

        config = MaterializationConfig(
            target_table="events", source_query="SELECT 1",
            strategy=MaterializationType.APPEND_ONLY,
        )
        result = await engine.materialize(config)
        assert result["strategy"] == "append_only"

    @pytest.mark.asyncio
    async def test_view(self):
        executor = MockSQLExecutor()
        engine = self._make_engine(executor)

        config = MaterializationConfig(
            target_table="v_active_users",
            source_query="SELECT * FROM users WHERE active = TRUE",
            strategy=MaterializationType.VIEW,
        )
        result = await engine.materialize(config)

        assert result["strategy"] == "view"
        assert "v_active_users" in executor.views

    @pytest.mark.asyncio
    async def test_snapshot_first_run(self):
        executor = MockSQLExecutor()
        executor.set_query_result("SELECT 1", [{"id": 1, "balance": 100}])
        engine = self._make_engine(executor)

        config = SnapshotConfig(
            target_table="balance_snapshots", source_query="SELECT 1",
        )
        result = await engine.materialize(config)

        assert result["strategy"] == "snapshot"
        assert "snapshot_id" in result

    @pytest.mark.asyncio
    async def test_snapshot_appends(self):
        executor = MockSQLExecutor()
        executor.tables["snaps"] = [{"id": 1, "val": 10, "snapshot_at": "t1", "snapshot_id": "s1"}]
        executor.set_query_result("SELECT 1", [{"id": 1, "val": 20}])
        engine = self._make_engine(executor)

        config = SnapshotConfig(target_table="snaps", source_query="SELECT 1")
        result = await engine.materialize(config)

        assert result["strategy"] == "snapshot"

    @pytest.mark.asyncio
    async def test_scd_type_1_first_run(self):
        executor = MockSQLExecutor()
        executor.set_query_result("SELECT 1", [{"id": 1, "name": "Alice", "email": "a@b.com"}])
        engine = self._make_engine(executor)

        config = SCDType1Config(
            target_table="customers", source_query="SELECT 1",
            unique_key="id", tracked_columns=["name", "email"],
        )
        result = await engine.materialize(config)

        assert result["first_run"] is True
        assert result["strategy"] == "scd_type_1"

    @pytest.mark.asyncio
    async def test_scd_type_2_first_run(self):
        executor = MockSQLExecutor()
        executor.set_query_result("SELECT 1", [{"id": 1, "name": "Alice", "tier": "gold"}])
        engine = self._make_engine(executor)

        config = SCDType2Config(
            target_table="dim_customers", source_query="SELECT 1",
            unique_key="id", tracked_columns=["name", "tier"],
        )
        result = await engine.materialize(config)

        assert result["first_run"] is True
        assert result["strategy"] == "scd_type_2"

    @pytest.mark.asyncio
    async def test_partitioned_first_run(self):
        executor = MockSQLExecutor()
        executor.set_query_result("SELECT 1", [{"date": "2026-01-01", "amount": 100}])
        engine = self._make_engine(executor)

        config = PartitionedConfig(
            target_table="daily_sales", source_query="SELECT 1",
            partition_by="date", partition_type="date",
            partition_granularity="day",
        )
        result = await engine.materialize(config)

        assert result["first_run"] is True
        assert result["strategy"] == "partitioned"

    @pytest.mark.asyncio
    async def test_partitioned_subsequent(self):
        executor = MockSQLExecutor()
        executor.tables["daily_sales"] = [{"date": "2026-01-01", "amount": 100}]
        executor.set_query_result("SELECT *", [{"date": "2026-01-02", "amount": 200}])
        engine = self._make_engine(executor)

        config = PartitionedConfig(
            target_table="daily_sales", source_query="SELECT * FROM raw",
            partition_by="date", incremental_column="date",
        )
        result = await engine.materialize(config)

        assert result["first_run"] is False

    @pytest.mark.asyncio
    async def test_unknown_strategy_raises(self):
        engine = self._make_engine()
        config = MaterializationConfig(target_table="t", source_query="SELECT 1")
        config.strategy = "invalid"

        with pytest.raises(ValueError, match="Unknown materialization"):
            await engine.materialize(config)


# ─── Dialect-Specific Tests ───

class TestDialectSQL:
    def test_current_timestamp_postgres(self):
        engine = MaterializationEngine(MockSQLExecutor(), dialect="postgres")
        assert engine._current_timestamp() == "CURRENT_TIMESTAMP"

    def test_current_timestamp_bigquery(self):
        engine = MaterializationEngine(MockSQLExecutor(), dialect="bigquery")
        assert engine._current_timestamp() == "CURRENT_TIMESTAMP()"

    def test_hash_expression_postgres(self):
        engine = MaterializationEngine(MockSQLExecutor(), dialect="postgres")
        expr = engine._build_hash_expression(["name", "email"])
        assert "MD5" in expr
        assert "name" in expr
        assert "email" in expr

    def test_hash_expression_clickhouse(self):
        engine = MaterializationEngine(MockSQLExecutor(), dialect="clickhouse")
        expr = engine._build_hash_expression(["name"])
        assert "concat" in expr
        assert "toString" in expr

    def test_normalize_keys_string(self):
        engine = MaterializationEngine(MockSQLExecutor())
        assert engine._normalize_keys("id") == ["id"]

    def test_normalize_keys_csv(self):
        engine = MaterializationEngine(MockSQLExecutor())
        assert engine._normalize_keys("id, date") == ["id", "date"]

    def test_normalize_keys_list(self):
        engine = MaterializationEngine(MockSQLExecutor())
        assert engine._normalize_keys(["id", "date"]) == ["id", "date"]

    def test_create_partitioned_bigquery(self):
        engine = MaterializationEngine(MockSQLExecutor(), dialect="bigquery")
        config = PartitionedConfig(
            target_table="t", source_query="SELECT 1",
            partition_by="created_date", partition_granularity="day",
            cluster_by=["user_id"],
        )
        sql = engine._create_partitioned_table("t", "SELECT 1", config)
        assert "PARTITION BY" in sql
        assert "CLUSTER BY" in sql

    def test_create_partitioned_clickhouse(self):
        engine = MaterializationEngine(MockSQLExecutor(), dialect="clickhouse")
        config = PartitionedConfig(
            target_table="t", source_query="SELECT 1",
            partition_by="created_date", cluster_by=["user_id"],
        )
        sql = engine._create_partitioned_table("t", "SELECT 1", config)
        assert "MergeTree" in sql
        assert "PARTITION BY" in sql


# ─── Table Existence Tests ───

class TestTableExistence:
    @pytest.mark.asyncio
    async def test_table_exists_true(self):
        executor = MockSQLExecutor()
        executor.tables["users"] = [{"id": 1}]
        engine = MaterializationEngine(executor)
        assert await engine._table_exists("users") is True

    @pytest.mark.asyncio
    async def test_table_exists_false(self):
        executor = MockSQLExecutor()
        engine = MaterializationEngine(executor)
        assert await engine._table_exists("nonexistent") is False

    @pytest.mark.asyncio
    async def test_count_rows(self):
        executor = MockSQLExecutor()
        executor.tables["users"] = [{"id": 1}, {"id": 2}, {"id": 3}]
        engine = MaterializationEngine(executor)
        count = await engine._count_rows("users")
        assert count == 3


# ─── PipelineContext Integration ───

class TestContextMaterialize:
    @pytest.mark.asyncio
    async def test_materialize_no_connector_raises(self):
        from tau.pipeline.context import PipelineContext
        ctx = PipelineContext(pipeline_name="test", run_id="r1")

        config = FullRefreshConfig(target_table="t", source_query="SELECT 1")
        with pytest.raises(ValueError, match="connector is required"):
            await ctx.materialize(config)

    @pytest.mark.asyncio
    async def test_materialize_with_connector(self):
        from tau.pipeline.context import PipelineContext
        ctx = PipelineContext(pipeline_name="test", run_id="r1")

        executor = MockSQLExecutor()
        executor.set_query_result("SELECT 1", [{"id": 1}])

        config = FullRefreshConfig(target_table="output", source_query="SELECT 1")
        result = await ctx.materialize(config, connector=executor, dialect="postgres")

        assert result["strategy"] == "full_refresh"
        assert "Materialized output" in ctx.get_logs()
