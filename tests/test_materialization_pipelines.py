"""Integration tests: pipelines using materialization strategies.

Tests cover:
- Each materialization strategy in a pipeline context
- Multi-step pipelines with multiple materializations
- Edge cases: empty data, schema changes, composite keys, dialect switching
- Error handling and recovery
- Snapshot retention/pruning
- Incremental strategies (merge, delete+insert, insert_overwrite)
- SCD2 hard delete handling
"""

import pytest
from tau import pipeline, PipelineContext
from tau.materializations import (
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


# ─── Enhanced Mock Executor ───

class MockExecutor:
    """SQL executor that tracks all operations for assertion."""

    def __init__(self):
        self.tables: dict[str, list[dict]] = {}
        self.views: dict[str, str] = {}
        self.executed: list[str] = []
        self._query_results: dict[str, list[dict]] = {}
        self._execute_count = 0
        self._fail_on: str | None = None  # Substring — if found in query, raise

    def set_query_result(self, substring: str, result: list[dict]):
        self._query_results[substring] = result

    def fail_on(self, substring: str):
        """Make executor raise when a query contains this substring."""
        self._fail_on = substring

    async def execute(self, query: str, params=None) -> str:
        self._execute_count += 1
        q = query.strip()
        self.executed.append(q)

        if self._fail_on and self._fail_on.upper() in q.upper():
            raise RuntimeError(f"Simulated failure on: {self._fail_on}")

        qu = q.upper()
        if qu.startswith("DROP TABLE IF EXISTS"):
            table = q.split("DROP TABLE IF EXISTS")[-1].strip().split()[0].rstrip(";")
            self.tables.pop(table, None)
        elif qu.startswith(("CREATE TABLE", "CREATE TEMP TABLE")) and " AS " in qu:
            table = q.split(" AS ", 1)[0].split()[-1]
            source = q.split(" AS ", 1)[1].strip()
            self.tables[table] = list(self._resolve(source))
        elif qu.startswith("CREATE OR REPLACE VIEW"):
            name = q.split(" AS ", 1)[0].replace("CREATE OR REPLACE VIEW", "").strip()
            self.views[name] = q.split(" AS ", 1)[1].strip() if " AS " in q else ""
        elif qu.startswith("INSERT INTO"):
            table = q.split("INSERT INTO")[1].strip().split()[0]
            if "SELECT" in qu:
                data = self._resolve(q.split("SELECT", 1)[1])
                self.tables.setdefault(table, []).extend(data)
        elif qu.startswith("DELETE FROM"):
            pass  # Track only
        elif qu.startswith("UPDATE"):
            pass  # Track only
        elif qu.startswith("TRUNCATE"):
            table = q.split()[-1].rstrip(";")
            self.tables[table] = []
        return "OK"

    async def extract(self, query: str, params=None, **kwargs) -> list[dict]:
        self.executed.append(query.strip())
        q = query.strip().upper()

        # Existence check — must come first
        if "SELECT 1 FROM" in q and "LIMIT 0" in q:
            table = self._from_table(query)
            if table and table in self.tables:
                return []
            raise Exception(f"Table {table} does not exist")

        # Configured results
        for sub, result in self._query_results.items():
            if sub.upper() in q:
                return result

        if "COUNT(*)" in q:
            table = self._from_table(query)
            return [{"cnt": len(self.tables.get(table, []))}] if table else [{"cnt": 0}]

        if "MAX(" in q:
            return [{"max_val": None}]

        if "LIMIT 0" in q:
            table = self._from_table(query)
            if table and table in self.tables and self.tables[table]:
                return [self.tables[table][0]]
            return []

        if "INFORMATION_SCHEMA" in q:
            table = query.split("'")[-2] if "'" in query else ""
            if table in self.tables and self.tables[table]:
                return [{"column_name": k} for k in self.tables[table][0].keys()]
            return []

        table = self._from_table(query)
        if table and table in self.tables:
            return list(self.tables[table])
        return []

    def _from_table(self, query: str) -> str | None:
        if "FROM" in query.upper():
            parts = query.split("FROM")[-1].strip().split()
            if parts:
                return parts[0].strip(";").strip(")")
        return None

    def _resolve(self, query: str) -> list[dict]:
        for sub, result in self._query_results.items():
            if sub.upper() in query.upper():
                return result
        table = self._from_table(query)
        if table and table in self.tables:
            return list(self.tables[table])
        return []

    def query_contains(self, substring: str) -> bool:
        return any(substring.upper() in q.upper() for q in self.executed)

    def count_queries_matching(self, substring: str) -> int:
        return sum(1 for q in self.executed if substring.upper() in q.upper())


# ─── Pipeline + Full Refresh ───

class TestFullRefreshPipeline:
    @pytest.mark.asyncio
    async def test_pipeline_full_refresh(self):
        """Full pipeline: full refresh materializes a table."""
        executor = MockExecutor()
        executor.set_query_result("raw.products", [
            {"id": 1, "name": "Widget", "price": 9.99},
            {"id": 2, "name": "Gadget", "price": 19.99},
        ])

        ctx = PipelineContext(pipeline_name="test_fr", run_id="r1")
        config = FullRefreshConfig(
            target_table="analytics.products",
            source_query="SELECT * FROM raw.products",
        )
        result = await ctx.materialize(config, connector=executor, dialect="postgres")

        assert result["strategy"] == "full_refresh"
        assert result["rows"] == 2
        assert executor.query_contains("DROP TABLE")
        assert executor.query_contains("CREATE TABLE")

    @pytest.mark.asyncio
    async def test_full_refresh_replaces_existing_data(self):
        """Second run drops the old table and recreates."""
        executor = MockExecutor()
        executor.tables["t"] = [{"id": 1}]
        executor.set_query_result("SELECT 1 AS", [{"id": 99}])

        engine = MaterializationEngine(executor, dialect="postgres")
        result = await engine.materialize(FullRefreshConfig(
            target_table="t", source_query="SELECT 1 AS id",
        ))
        assert executor.query_contains("DROP TABLE")

    @pytest.mark.asyncio
    async def test_full_refresh_empty_source(self):
        """Full refresh with empty source creates empty table."""
        executor = MockExecutor()
        executor.set_query_result("SELECT 1", [])

        engine = MaterializationEngine(executor)
        result = await engine.materialize(FullRefreshConfig(
            target_table="empty_table", source_query="SELECT 1",
        ))
        assert result["rows"] == 0

    @pytest.mark.asyncio
    async def test_full_refresh_pre_post_hooks_order(self):
        """Hooks execute in correct order: pre → drop → create → post."""
        executor = MockExecutor()
        executor.set_query_result("SELECT 1", [{"id": 1}])

        engine = MaterializationEngine(executor)
        await engine.materialize(FullRefreshConfig(
            target_table="t",
            source_query="SELECT 1",
            pre_hook="SELECT 'pre'",
            post_hook="SELECT 'post'",
        ))

        pre_idx = next(i for i, q in enumerate(executor.executed) if "'pre'" in q)
        drop_idx = next(i for i, q in enumerate(executor.executed) if "DROP TABLE" in q.upper())
        create_idx = next(i for i, q in enumerate(executor.executed) if "CREATE TABLE" in q.upper())
        post_idx = next(i for i, q in enumerate(executor.executed) if "'post'" in q)
        assert pre_idx < drop_idx < create_idx < post_idx


# ─── Pipeline + Incremental ───

class TestIncrementalPipeline:
    @pytest.mark.asyncio
    async def test_incremental_first_run(self):
        executor = MockExecutor()
        executor.set_query_result("raw.orders", [
            {"order_id": 1, "amount": 100, "updated_at": "2026-01-01"},
        ])

        engine = MaterializationEngine(executor)
        result = await engine.materialize(IncrementalConfig(
            target_table="orders",
            source_query="SELECT * FROM raw.orders",
            unique_key="order_id",
            incremental_column="updated_at",
        ))
        assert result["first_run"] is True
        assert "orders" in executor.tables

    @pytest.mark.asyncio
    async def test_incremental_merge_creates_staging(self):
        """Merge strategy uses a staging table for UPDATE + INSERT."""
        executor = MockExecutor()
        executor.tables["orders"] = [{"order_id": 1, "amount": 100}]
        executor.set_query_result("raw.orders", [{"order_id": 2, "amount": 200}])

        engine = MaterializationEngine(executor)
        await engine.materialize(IncrementalConfig(
            target_table="orders",
            source_query="SELECT * FROM raw.orders",
            unique_key="order_id",
            incremental_strategy="merge",
        ))
        assert executor.query_contains("__merge_staging")
        assert executor.query_contains("UPDATE")
        assert executor.query_contains("INSERT INTO orders")

    @pytest.mark.asyncio
    async def test_incremental_delete_insert(self):
        """delete+insert strategy deletes matching keys then inserts."""
        executor = MockExecutor()
        executor.tables["orders"] = [{"order_id": 1, "amount": 100}]
        executor.set_query_result("raw.orders", [{"order_id": 1, "amount": 150}])

        engine = MaterializationEngine(executor)
        await engine.materialize(IncrementalConfig(
            target_table="orders",
            source_query="SELECT * FROM raw.orders",
            unique_key="order_id",
            incremental_strategy="delete+insert",
        ))
        assert executor.query_contains("DELETE FROM")
        assert executor.query_contains("__di_staging")

    @pytest.mark.asyncio
    async def test_incremental_insert_overwrite(self):
        """insert_overwrite deletes affected partitions then inserts."""
        executor = MockExecutor()
        executor.tables["events"] = [{"date": "2026-01-01", "count": 10}]
        executor.set_query_result("raw.events", [{"date": "2026-01-01", "count": 15}])

        engine = MaterializationEngine(executor)
        await engine.materialize(IncrementalConfig(
            target_table="events",
            source_query="SELECT * FROM raw.events",
            unique_key="",
            incremental_column="date",
            incremental_strategy="insert_overwrite",
        ))
        assert executor.query_contains("DELETE FROM events")
        assert executor.query_contains("__io_staging")

    @pytest.mark.asyncio
    async def test_incremental_no_unique_key_appends(self):
        """Without a unique key, incremental just appends."""
        executor = MockExecutor()
        executor.tables["logs"] = [{"msg": "old"}]
        executor.set_query_result("raw.logs", [{"msg": "new"}])

        engine = MaterializationEngine(executor)
        await engine.materialize(IncrementalConfig(
            target_table="logs",
            source_query="SELECT * FROM raw.logs",
            unique_key="",
        ))
        # Should not use staging tables
        assert not executor.query_contains("__merge_staging")

    @pytest.mark.asyncio
    async def test_incremental_composite_key(self):
        """Composite unique key with multiple columns."""
        executor = MockExecutor()
        executor.tables["sales"] = [{"store": "A", "date": "01", "rev": 100}]
        executor.set_query_result("raw.sales", [{"store": "B", "date": "01", "rev": 200}])

        engine = MaterializationEngine(executor)
        await engine.materialize(IncrementalConfig(
            target_table="sales",
            source_query="SELECT * FROM raw.sales",
            unique_key=["store", "date"],
            incremental_strategy="merge",
        ))
        # Should join on both keys
        assert executor.query_contains("t.store = s.store")
        assert executor.query_contains("t.date = s.date")


# ─── Pipeline + SCD Type 1 ───

class TestSCDType1Pipeline:
    @pytest.mark.asyncio
    async def test_scd1_first_run_adds_updated_at(self):
        executor = MockExecutor()
        executor.set_query_result("raw.products", [
            {"id": 1, "name": "Widget", "price": 10},
        ])

        engine = MaterializationEngine(executor)
        result = await engine.materialize(SCDType1Config(
            target_table="dim_products",
            source_query="SELECT * FROM raw.products",
            unique_key="id",
        ))
        assert result["first_run"] is True
        # Should include CURRENT_TIMESTAMP for updated_at
        create_q = [q for q in executor.executed if "CREATE TABLE" in q.upper()]
        assert any("CURRENT_TIMESTAMP" in q for q in create_q)

    @pytest.mark.asyncio
    async def test_scd1_updates_changed_rows(self):
        """SCD1 updates existing rows and inserts new ones."""
        executor = MockExecutor()
        executor.tables["dim_products"] = [
            {"id": 1, "name": "Old Name", "price": 10, "updated_at": "2026-01-01"},
        ]
        executor.set_query_result("raw.products", [
            {"id": 1, "name": "New Name", "price": 10},
            {"id": 2, "name": "Brand New", "price": 20},
        ])

        engine = MaterializationEngine(executor)
        result = await engine.materialize(SCDType1Config(
            target_table="dim_products",
            source_query="SELECT * FROM raw.products",
            unique_key="id",
            tracked_columns=["name", "price"],
        ))
        assert result["first_run"] is False
        assert executor.query_contains("UPDATE")
        assert executor.query_contains("IS DISTINCT FROM")

    @pytest.mark.asyncio
    async def test_scd1_no_tracked_columns_updates_all(self):
        """When tracked_columns is empty, all rows get updated."""
        executor = MockExecutor()
        executor.tables["dim"] = [{"id": 1, "x": "a", "updated_at": "old"}]
        executor.set_query_result("raw", [{"id": 1, "x": "b"}])

        engine = MaterializationEngine(executor)
        await engine.materialize(SCDType1Config(
            target_table="dim",
            source_query="SELECT * FROM raw",
            unique_key="id",
            tracked_columns=[],
        ))
        # Change check should be TRUE (update all)
        update_queries = [q for q in executor.executed if q.upper().startswith("UPDATE")]
        assert any("TRUE" in q for q in update_queries)


# ─── Pipeline + SCD Type 2 ───

class TestSCDType2Pipeline:
    @pytest.mark.asyncio
    async def test_scd2_first_run_creates_history_columns(self):
        executor = MockExecutor()
        executor.set_query_result("raw.customers", [
            {"customer_id": 1, "name": "Alice", "tier": "gold"},
        ])

        engine = MaterializationEngine(executor)
        result = await engine.materialize(SCDType2Config(
            target_table="dim_customers",
            source_query="SELECT * FROM raw.customers",
            unique_key="customer_id",
            tracked_columns=["name", "tier"],
        ))
        assert result["first_run"] is True
        create_q = [q for q in executor.executed if "CREATE TABLE" in q.upper()]
        # Should include SCD2 columns
        assert any("valid_from" in q for q in create_q)
        assert any("valid_to" in q for q in create_q)
        assert any("is_current" in q for q in create_q)
        assert any("row_hash" in q for q in create_q)

    @pytest.mark.asyncio
    async def test_scd2_closes_changed_records(self):
        """Changed records get valid_to set and is_current=FALSE."""
        executor = MockExecutor()
        executor.tables["dim_customers"] = [
            {"customer_id": 1, "name": "Alice", "tier": "silver",
             "valid_from": "2026-01-01", "valid_to": None,
             "is_current": True, "row_hash": "old_hash"},
        ]
        executor.set_query_result("raw.customers", [
            {"customer_id": 1, "name": "Alice", "tier": "gold"},
        ])

        engine = MaterializationEngine(executor)
        await engine.materialize(SCDType2Config(
            target_table="dim_customers",
            source_query="SELECT * FROM raw.customers",
            unique_key="customer_id",
            tracked_columns=["name", "tier"],
        ))
        # Should close old record
        assert executor.query_contains("is_current = FALSE")
        # Should insert new version
        assert executor.query_contains("INSERT INTO dim_customers")

    @pytest.mark.asyncio
    async def test_scd2_hard_deletes_enabled(self):
        """Records that disappear from source get closed."""
        executor = MockExecutor()
        executor.tables["dim"] = [
            {"id": 1, "name": "Gone", "valid_from": "old", "valid_to": None,
             "is_current": True, "row_hash": "h"},
        ]
        executor.set_query_result("raw", [])  # Source is empty

        engine = MaterializationEngine(executor)
        await engine.materialize(SCDType2Config(
            target_table="dim",
            source_query="SELECT * FROM raw",
            unique_key="id",
            tracked_columns=["name"],
            invalidate_hard_deletes=True,
        ))
        # Should close records that don't exist in source
        assert executor.query_contains("NOT EXISTS")
        assert executor.query_contains("is_current = FALSE")

    @pytest.mark.asyncio
    async def test_scd2_hard_deletes_disabled(self):
        """With invalidate_hard_deletes=False, disappeared records stay current."""
        executor = MockExecutor()
        executor.tables["dim"] = [
            {"id": 1, "name": "Still Here", "valid_from": "old", "valid_to": None,
             "is_current": True, "row_hash": "h"},
        ]
        executor.set_query_result("raw", [])

        engine = MaterializationEngine(executor)
        await engine.materialize(SCDType2Config(
            target_table="dim",
            source_query="SELECT * FROM raw",
            unique_key="id",
            tracked_columns=["name"],
            invalidate_hard_deletes=False,
        ))
        # Should NOT have the hard delete query
        not_exists_queries = [q for q in executor.executed if "NOT EXISTS" in q.upper()]
        assert len(not_exists_queries) == 0

    @pytest.mark.asyncio
    async def test_scd2_composite_key(self):
        executor = MockExecutor()
        executor.set_query_result("raw", [
            {"region": "US", "product_id": 1, "price": 100},
        ])

        engine = MaterializationEngine(executor)
        result = await engine.materialize(SCDType2Config(
            target_table="dim",
            source_query="SELECT * FROM raw",
            unique_key=["region", "product_id"],
            tracked_columns=["price"],
        ))
        assert result["first_run"] is True

    @pytest.mark.asyncio
    async def test_scd2_custom_column_names(self):
        executor = MockExecutor()
        executor.set_query_result("src", [{"id": 1, "val": "a"}])

        engine = MaterializationEngine(executor)
        await engine.materialize(SCDType2Config(
            target_table="t",
            source_query="SELECT * FROM src",
            unique_key="id",
            tracked_columns=["val"],
            valid_from_column="eff_start",
            valid_to_column="eff_end",
            is_current_column="active",
            hash_column="chksum",
        ))
        create_q = [q for q in executor.executed if "CREATE TABLE" in q.upper()]
        assert any("eff_start" in q for q in create_q)
        assert any("eff_end" in q for q in create_q)
        assert any("active" in q for q in create_q)
        assert any("chksum" in q for q in create_q)


# ─── Pipeline + Partitioned ───

class TestPartitionedPipeline:
    @pytest.mark.asyncio
    async def test_partitioned_first_run_bigquery(self):
        executor = MockExecutor()
        executor.set_query_result("raw.events", [
            {"event_id": 1, "event_date": "2026-01-01", "user_id": 10},
        ])

        engine = MaterializationEngine(executor, dialect="bigquery")
        result = await engine.materialize(PartitionedConfig(
            target_table="events",
            source_query="SELECT * FROM raw.events",
            partition_by="event_date",
            partition_granularity="day",
            cluster_by=["user_id"],
        ))
        assert result["first_run"] is True
        create_q = [q for q in executor.executed if "CREATE TABLE" in q.upper()]
        assert any("PARTITION BY" in q for q in create_q)
        assert any("CLUSTER BY" in q for q in create_q)

    @pytest.mark.asyncio
    async def test_partitioned_clickhouse_mergetree(self):
        executor = MockExecutor()
        executor.set_query_result("raw.logs", [{"ts": "2026-01-01", "msg": "hi"}])

        engine = MaterializationEngine(executor, dialect="clickhouse")
        result = await engine.materialize(PartitionedConfig(
            target_table="logs",
            source_query="SELECT * FROM raw.logs",
            partition_by="ts",
            cluster_by=["msg"],
        ))
        create_q = [q for q in executor.executed if "CREATE TABLE" in q.upper()]
        assert any("MergeTree" in q for q in create_q)

    @pytest.mark.asyncio
    async def test_partitioned_with_expiration(self):
        """Old partitions get deleted when expiration is set."""
        executor = MockExecutor()
        executor.tables["events"] = [
            {"event_date": "2025-01-01", "data": "old"},
            {"event_date": "2026-01-01", "data": "new"},
        ]
        executor.set_query_result("raw.events", [{"event_date": "2026-02-01", "data": "newest"}])

        engine = MaterializationEngine(executor)
        await engine.materialize(PartitionedConfig(
            target_table="events",
            source_query="SELECT * FROM raw.events",
            partition_by="event_date",
            incremental_column="event_date",
            partition_expiration_days=90,
        ))
        assert executor.query_contains("INTERVAL")

    @pytest.mark.asyncio
    async def test_partitioned_incremental_with_unique_key(self):
        """Partitioned + unique key uses merge."""
        executor = MockExecutor()
        executor.tables["events"] = [{"id": 1, "date": "2026-01-01"}]
        executor.set_query_result("raw", [{"id": 2, "date": "2026-01-02"}])

        engine = MaterializationEngine(executor)
        await engine.materialize(PartitionedConfig(
            target_table="events",
            source_query="SELECT * FROM raw",
            unique_key="id",
            partition_by="date",
        ))
        assert executor.query_contains("__merge_staging")


# ─── Pipeline + Snapshot ───

class TestSnapshotPipeline:
    @pytest.mark.asyncio
    async def test_snapshot_adds_metadata_columns(self):
        executor = MockExecutor()
        executor.set_query_result("raw.accounts", [
            {"account_id": 1, "balance": 1000},
        ])

        engine = MaterializationEngine(executor)
        result = await engine.materialize(SnapshotConfig(
            target_table="snapshots",
            source_query="SELECT * FROM raw.accounts",
        ))
        assert "snapshot_id" in result
        create_q = [q for q in executor.executed if "CREATE TABLE" in q.upper()]
        assert any("snapshot_at" in q for q in create_q)
        assert any("snapshot_id" in q for q in create_q)

    @pytest.mark.asyncio
    async def test_snapshot_appends_to_existing(self):
        executor = MockExecutor()
        executor.tables["snapshots"] = [
            {"id": 1, "val": 100, "snapshot_at": "t1", "snapshot_id": "s1"},
        ]
        executor.set_query_result("raw", [{"id": 1, "val": 200}])

        engine = MaterializationEngine(executor)
        await engine.materialize(SnapshotConfig(
            target_table="snapshots",
            source_query="SELECT * FROM raw",
        ))
        assert executor.query_contains("INSERT INTO snapshots")

    @pytest.mark.asyncio
    async def test_snapshot_retention_pruning(self):
        """With retain_snapshots set, old snapshots get pruned."""
        executor = MockExecutor()
        executor.tables["snaps"] = [{"id": 1, "snapshot_at": "old", "snapshot_id": "s1"}]
        executor.set_query_result("raw", [{"id": 1}])

        engine = MaterializationEngine(executor)
        await engine.materialize(SnapshotConfig(
            target_table="snaps",
            source_query="SELECT * FROM raw",
            retain_snapshots=5,
        ))
        assert executor.query_contains("DELETE FROM snaps")
        assert executor.query_contains("LIMIT 5")

    @pytest.mark.asyncio
    async def test_snapshot_no_retention_no_prune(self):
        """Without retain_snapshots, no pruning occurs."""
        executor = MockExecutor()
        executor.tables["snaps"] = [{"id": 1, "snapshot_at": "old", "snapshot_id": "s1"}]
        executor.set_query_result("raw", [{"id": 1}])

        engine = MaterializationEngine(executor)
        await engine.materialize(SnapshotConfig(
            target_table="snaps",
            source_query="SELECT * FROM raw",
            retain_snapshots=None,
        ))
        delete_queries = [q for q in executor.executed if q.upper().startswith("DELETE")]
        assert len(delete_queries) == 0

    @pytest.mark.asyncio
    async def test_snapshot_custom_column_names(self):
        executor = MockExecutor()
        executor.set_query_result("src", [{"id": 1}])

        engine = MaterializationEngine(executor)
        await engine.materialize(SnapshotConfig(
            target_table="t",
            source_query="SELECT * FROM src",
            snapshot_timestamp_column="taken_at",
            snapshot_id_column="snap_id",
        ))
        create_q = [q for q in executor.executed if "CREATE TABLE" in q.upper()]
        assert any("taken_at" in q for q in create_q)
        assert any("snap_id" in q for q in create_q)


# ─── Append Only + View ───

class TestAppendOnlyAndView:
    @pytest.mark.asyncio
    async def test_append_only_never_updates(self):
        executor = MockExecutor()
        executor.tables["logs"] = [{"id": 1, "msg": "first"}]
        executor.set_query_result("buffer", [{"id": 2, "msg": "second"}])

        engine = MaterializationEngine(executor)
        await engine.materialize(MaterializationConfig(
            target_table="logs",
            source_query="SELECT * FROM buffer",
            strategy=MaterializationType.APPEND_ONLY,
        ))
        assert not executor.query_contains("UPDATE")
        assert not executor.query_contains("DELETE")
        assert executor.query_contains("INSERT INTO logs")

    @pytest.mark.asyncio
    async def test_view_creates_view_not_table(self):
        executor = MockExecutor()

        engine = MaterializationEngine(executor)
        result = await engine.materialize(MaterializationConfig(
            target_table="v_report",
            source_query="SELECT * FROM analytics.users WHERE active",
            strategy=MaterializationType.VIEW,
        ))
        assert result["strategy"] == "view"
        assert "v_report" in executor.views
        assert not executor.query_contains("CREATE TABLE")

    @pytest.mark.asyncio
    async def test_view_replaces_existing(self):
        executor = MockExecutor()
        executor.views["v_old"] = "SELECT 1"

        engine = MaterializationEngine(executor)
        await engine.materialize(MaterializationConfig(
            target_table="v_old",
            source_query="SELECT 2",
            strategy=MaterializationType.VIEW,
        ))
        assert executor.query_contains("CREATE OR REPLACE VIEW")


# ─── Multi-Step Pipeline ───

class TestMultiStepPipeline:
    @pytest.mark.asyncio
    async def test_multi_materialization_pipeline(self):
        """Pipeline with multiple materializations in sequence."""
        executor = MockExecutor()
        executor.set_query_result("raw.products", [{"id": 1, "name": "W"}])
        executor.set_query_result("raw.customers", [{"cid": 1, "name": "A", "tier": "gold"}])
        executor.set_query_result("raw.orders", [{"oid": 1, "amount": 100}])

        ctx = PipelineContext(pipeline_name="warehouse_etl", run_id="r1")

        # Step 1: Full refresh dimension
        r1 = await ctx.materialize(
            FullRefreshConfig(target_table="dim_products", source_query="SELECT * FROM raw.products"),
            connector=executor,
        )
        assert r1["strategy"] == "full_refresh"

        # Step 2: SCD2 dimension
        r2 = await ctx.materialize(
            SCDType2Config(
                target_table="dim_customers",
                source_query="SELECT * FROM raw.customers",
                unique_key="cid",
                tracked_columns=["name", "tier"],
            ),
            connector=executor,
        )
        assert r2["strategy"] == "scd_type_2"

        # Step 3: Incremental fact
        r3 = await ctx.materialize(
            IncrementalConfig(
                target_table="fct_orders",
                source_query="SELECT * FROM raw.orders",
                unique_key="oid",
            ),
            connector=executor,
        )
        assert r3["strategy"] == "incremental"

        # Step 4: View
        r4 = await ctx.materialize(
            MaterializationConfig(
                target_table="v_report",
                source_query="SELECT 1",
                strategy=MaterializationType.VIEW,
            ),
            connector=executor,
        )
        assert r4["strategy"] == "view"

        # All should be logged
        logs = ctx.get_logs()
        assert "dim_products" in logs
        assert "dim_customers" in logs
        assert "fct_orders" in logs
        assert "v_report" in logs


# ─── Dialect Tests ───

class TestDialectSpecificSQL:
    @pytest.mark.asyncio
    async def test_bigquery_full_refresh(self):
        executor = MockExecutor()
        executor.set_query_result("src", [{"id": 1}])

        engine = MaterializationEngine(executor, dialect="bigquery")
        await engine.materialize(FullRefreshConfig(
            target_table="t", source_query="SELECT * FROM src",
        ))
        # BigQuery uses CURRENT_TIMESTAMP()
        # Full refresh doesn't use timestamps directly, but verify no errors
        assert executor.query_contains("CREATE TABLE")

    @pytest.mark.asyncio
    async def test_snowflake_hash(self):
        engine = MaterializationEngine(MockExecutor(), dialect="snowflake")
        expr = engine._build_hash_expression(["name", "email"])
        assert "MD5" in expr

    @pytest.mark.asyncio
    async def test_mysql_dialect(self):
        executor = MockExecutor()
        executor.set_query_result("src", [{"id": 1}])

        engine = MaterializationEngine(executor, dialect="mysql")
        await engine.materialize(FullRefreshConfig(
            target_table="t", source_query="SELECT * FROM src",
        ))
        assert executor.query_contains("CREATE TABLE")

    @pytest.mark.asyncio
    async def test_duckdb_dialect(self):
        executor = MockExecutor()
        executor.set_query_result("src", [{"id": 1}])

        engine = MaterializationEngine(executor, dialect="duckdb")
        result = await engine.materialize(FullRefreshConfig(
            target_table="t", source_query="SELECT * FROM src",
        ))
        assert result["strategy"] == "full_refresh"


# ─── Error Handling ───

class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_executor_failure_propagates(self):
        """If the SQL executor fails, the error propagates."""
        executor = MockExecutor()
        executor.fail_on("DROP TABLE")

        engine = MaterializationEngine(executor)
        with pytest.raises(RuntimeError, match="Simulated failure"):
            await engine.materialize(FullRefreshConfig(
                target_table="t", source_query="SELECT 1",
            ))

    @pytest.mark.asyncio
    async def test_context_materialize_logs_on_success(self):
        executor = MockExecutor()
        executor.set_query_result("src", [{"id": 1}])

        ctx = PipelineContext(pipeline_name="test", run_id="r1")
        await ctx.materialize(
            FullRefreshConfig(target_table="output", source_query="SELECT * FROM src"),
            connector=executor,
        )
        assert "Materialized output" in ctx.get_logs()
        assert "full_refresh" in ctx.get_logs()

    @pytest.mark.asyncio
    async def test_table_exists_handles_exception(self):
        """_table_exists returns False when table doesn't exist."""
        executor = MockExecutor()
        engine = MaterializationEngine(executor)
        assert await engine._table_exists("nonexistent") is False

    @pytest.mark.asyncio
    async def test_count_rows_handles_missing_table(self):
        executor = MockExecutor()
        engine = MaterializationEngine(executor)
        count = await engine._count_rows("nonexistent")
        assert count == 0 or count == -1


# ─── Edge Cases ───

class TestEdgeCases:
    @pytest.mark.asyncio
    async def test_schema_qualified_table_name(self):
        """Tables with schema.table names work correctly."""
        executor = MockExecutor()
        executor.set_query_result("src", [{"id": 1}])

        engine = MaterializationEngine(executor)
        result = await engine.materialize(FullRefreshConfig(
            target_table="analytics.dim_products",
            source_query="SELECT * FROM src",
        ))
        assert result["table"] == "analytics.dim_products"

    @pytest.mark.asyncio
    async def test_csv_unique_key_parsing(self):
        """Comma-separated unique key string gets parsed correctly."""
        engine = MaterializationEngine(MockExecutor())
        assert engine._normalize_keys("id, date, region") == ["id", "date", "region"]
        assert engine._normalize_keys("id") == ["id"]
        assert engine._normalize_keys("") == []
        assert engine._normalize_keys([]) == []

    @pytest.mark.asyncio
    async def test_empty_unique_key_list(self):
        engine = MaterializationEngine(MockExecutor())
        assert engine._normalize_keys([]) == []

    @pytest.mark.asyncio
    async def test_all_strategies_return_required_fields(self):
        """Every strategy returns at minimum 'strategy' and 'table'."""
        executor = MockExecutor()
        executor.set_query_result("src", [{"id": 1, "name": "test"}])
        engine = MaterializationEngine(executor)

        configs = [
            FullRefreshConfig(target_table="t1", source_query="SELECT * FROM src"),
            IncrementalConfig(target_table="t2", source_query="SELECT * FROM src", unique_key="id"),
            PartitionedConfig(target_table="t3", source_query="SELECT * FROM src", partition_by="id"),
            SCDType1Config(target_table="t4", source_query="SELECT * FROM src", unique_key="id"),
            SCDType2Config(target_table="t5", source_query="SELECT * FROM src", unique_key="id", tracked_columns=["name"]),
            SnapshotConfig(target_table="t6", source_query="SELECT * FROM src"),
            MaterializationConfig(target_table="t7", source_query="SELECT * FROM src", strategy=MaterializationType.APPEND_ONLY),
            MaterializationConfig(target_table="t8", source_query="SELECT * FROM src", strategy=MaterializationType.VIEW),
        ]

        for config in configs:
            result = await engine.materialize(config)
            assert "strategy" in result, f"Missing 'strategy' in {config.strategy}"
            assert "table" in result or config.strategy == MaterializationType.VIEW, \
                f"Missing 'table' in {config.strategy}"

    @pytest.mark.asyncio
    async def test_incremental_with_max_value(self):
        """Incremental filters by max value of incremental column."""
        executor = MockExecutor()
        executor.tables["orders"] = [{"id": 1, "ts": "2026-01-01"}]
        # Override max to return a value
        executor._query_results["MAX("] = [{"max_val": "2026-01-01"}]
        executor.set_query_result("raw.orders", [{"id": 2, "ts": "2026-01-02"}])

        engine = MaterializationEngine(executor)
        await engine.materialize(IncrementalConfig(
            target_table="orders",
            source_query="SELECT * FROM raw.orders",
            unique_key="id",
            incremental_column="ts",
        ))
        # Should filter by max value
        assert executor.query_contains("2026-01-01")

    @pytest.mark.asyncio
    async def test_snapshot_unique_ids(self):
        """Each snapshot gets a unique ID."""
        executor = MockExecutor()
        executor.set_query_result("src", [{"id": 1}])
        engine = MaterializationEngine(executor)

        r1 = await engine.materialize(SnapshotConfig(target_table="s1", source_query="SELECT * FROM src"))
        # Reset for second run
        executor.set_query_result("src", [{"id": 1}])
        r2 = await engine.materialize(SnapshotConfig(target_table="s2", source_query="SELECT * FROM src"))

        assert r1["snapshot_id"] != r2["snapshot_id"]
