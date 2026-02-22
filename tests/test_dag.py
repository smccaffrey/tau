"""Tests for DAG dependency resolution and execution."""

import pytest
import asyncio
from tau.dag.resolver import DAGResolver, CycleError
from tau.dag.runner import DAGRunner, DAGRunResult


# ─── Resolver Tests ───

class TestDAGResolver:
    def test_empty_dag(self):
        dag = DAGResolver()
        assert dag.topological_sort() == []
        assert dag.parallel_groups() == []

    def test_single_node(self):
        dag = DAGResolver()
        dag.add_pipeline("a")
        assert dag.topological_sort() == ["a"]
        assert dag.parallel_groups() == [["a"]]

    def test_linear_chain(self):
        dag = DAGResolver()
        dag.add_dependency("extract", "transform")
        dag.add_dependency("transform", "load")
        order = dag.topological_sort()
        assert order.index("extract") < order.index("transform") < order.index("load")

    def test_diamond_dependency(self):
        """A → B, A → C, B → D, C → D"""
        dag = DAGResolver()
        dag.add_dependency("A", "B")
        dag.add_dependency("A", "C")
        dag.add_dependency("B", "D")
        dag.add_dependency("C", "D")
        order = dag.topological_sort()
        assert order[0] == "A"
        assert order[-1] == "D"
        assert order.index("A") < order.index("B")
        assert order.index("A") < order.index("C")

    def test_parallel_groups_diamond(self):
        dag = DAGResolver()
        dag.add_dependency("A", "B")
        dag.add_dependency("A", "C")
        dag.add_dependency("B", "D")
        dag.add_dependency("C", "D")
        groups = dag.parallel_groups()
        assert groups[0] == ["A"]
        assert sorted(groups[1]) == ["B", "C"]
        assert groups[2] == ["D"]

    def test_parallel_groups_independent(self):
        """Independent pipelines all in one group."""
        dag = DAGResolver()
        dag.add_pipeline("x")
        dag.add_pipeline("y")
        dag.add_pipeline("z")
        groups = dag.parallel_groups()
        assert len(groups) == 1
        assert sorted(groups[0]) == ["x", "y", "z"]

    def test_cycle_detection_simple(self):
        dag = DAGResolver()
        dag.add_dependency("A", "B")
        dag.add_dependency("B", "A")
        cycle = dag.detect_cycles()
        assert cycle is not None
        assert "A" in cycle and "B" in cycle

    def test_cycle_detection_three_node(self):
        dag = DAGResolver()
        dag.add_dependency("A", "B")
        dag.add_dependency("B", "C")
        dag.add_dependency("C", "A")
        cycle = dag.detect_cycles()
        assert cycle is not None

    def test_no_cycle(self):
        dag = DAGResolver()
        dag.add_dependency("A", "B")
        dag.add_dependency("B", "C")
        assert dag.detect_cycles() is None

    def test_topological_sort_raises_on_cycle(self):
        dag = DAGResolver()
        dag.add_dependency("A", "B")
        dag.add_dependency("B", "A")
        with pytest.raises(CycleError):
            dag.topological_sort()

    def test_get_upstream(self):
        dag = DAGResolver()
        dag.add_dependency("A", "B")
        dag.add_dependency("B", "C")
        dag.add_dependency("A", "C")
        upstream = dag.get_upstream("C")
        assert upstream == {"A", "B"}

    def test_get_downstream(self):
        dag = DAGResolver()
        dag.add_dependency("A", "B")
        dag.add_dependency("A", "C")
        dag.add_dependency("B", "D")
        downstream = dag.get_downstream("A")
        assert downstream == {"B", "C", "D"}

    def test_get_upstream_root(self):
        dag = DAGResolver()
        dag.add_dependency("A", "B")
        assert dag.get_upstream("A") == set()

    def test_get_downstream_leaf(self):
        dag = DAGResolver()
        dag.add_dependency("A", "B")
        assert dag.get_downstream("B") == set()

    def test_add_dependencies_bulk(self):
        dag = DAGResolver()
        dag.add_dependencies("load", ["extract", "transform"])
        assert dag.get_upstream("load") == {"extract", "transform"}

    def test_subgraph(self):
        dag = DAGResolver()
        dag.add_dependency("A", "B")
        dag.add_dependency("A", "C")
        dag.add_dependency("B", "D")
        dag.add_dependency("C", "D")
        dag.add_pipeline("E")  # Unrelated

        sub = dag.get_subgraph("A")
        assert "E" not in sub.nodes
        assert set(sub.nodes.keys()) == {"A", "B", "C", "D"}

    def test_to_dict(self):
        dag = DAGResolver()
        dag.add_dependency("A", "B")
        d = dag.to_dict()
        assert "nodes" in d
        assert "edges" in d
        assert "groups" in d
        assert "A" in d["nodes"]
        assert "B" in d["nodes"]

    def test_complex_dag(self):
        """
        raw_orders → stg_orders → fct_orders → report_view
        raw_products → stg_products ↗
        raw_customers → dim_customers ↗
        """
        dag = DAGResolver()
        dag.add_dependency("raw_orders", "stg_orders")
        dag.add_dependency("stg_orders", "fct_orders")
        dag.add_dependency("raw_products", "stg_products")
        dag.add_dependency("stg_products", "fct_orders")
        dag.add_dependency("raw_customers", "dim_customers")
        dag.add_dependency("dim_customers", "fct_orders")
        dag.add_dependency("fct_orders", "report_view")

        groups = dag.parallel_groups()
        # First group: all raw + raw_customers
        assert sorted(groups[0]) == ["raw_customers", "raw_orders", "raw_products"]
        # Last group: report_view
        assert groups[-1] == ["report_view"]

        order = dag.topological_sort()
        assert order.index("raw_orders") < order.index("stg_orders")
        assert order.index("stg_orders") < order.index("fct_orders")
        assert order.index("fct_orders") < order.index("report_view")


# ─── Runner Tests ───

class TestDAGRunner:
    @pytest.mark.asyncio
    async def test_run_linear_dag(self):
        dag = DAGResolver()
        dag.add_dependency("A", "B")
        dag.add_dependency("B", "C")

        execution_log = []

        async def mock_run(name: str) -> dict:
            execution_log.append(name)
            return {"status": "success"}

        runner = DAGRunner(dag, run_fn=mock_run, max_parallel=4)
        result = await runner.run()

        assert result.status == "success"
        assert execution_log == ["A", "B", "C"]

    @pytest.mark.asyncio
    async def test_run_parallel_group(self):
        dag = DAGResolver()
        dag.add_dependency("A", "B")
        dag.add_dependency("A", "C")
        dag.add_dependency("B", "D")
        dag.add_dependency("C", "D")

        execution_log = []

        async def mock_run(name: str) -> dict:
            execution_log.append(name)
            return {"status": "success"}

        runner = DAGRunner(dag, run_fn=mock_run, max_parallel=4)
        result = await runner.run()

        assert result.status == "success"
        assert execution_log[0] == "A"
        assert set(execution_log[1:3]) == {"B", "C"}
        assert execution_log[3] == "D"

    @pytest.mark.asyncio
    async def test_failure_skips_downstream(self):
        dag = DAGResolver()
        dag.add_dependency("A", "B")
        dag.add_dependency("B", "C")

        async def mock_run(name: str) -> dict:
            if name == "B":
                return {"status": "failed", "error": "boom"}
            return {"status": "success"}

        runner = DAGRunner(dag, run_fn=mock_run)
        result = await runner.run()

        assert result.status == "partial"
        assert "B" in result.failed
        assert "C" in result.skipped

    @pytest.mark.asyncio
    async def test_fail_fast_mode(self):
        dag = DAGResolver()
        dag.add_pipeline("A")
        dag.add_pipeline("B")
        dag.add_dependency("A", "C")
        dag.add_dependency("B", "C")

        async def mock_run(name: str) -> dict:
            if name == "A":
                raise RuntimeError("fail")
            return {"status": "success"}

        runner = DAGRunner(dag, run_fn=mock_run, fail_fast=True)
        result = await runner.run()

        assert "A" in result.failed

    @pytest.mark.asyncio
    async def test_run_downstream(self):
        dag = DAGResolver()
        dag.add_dependency("A", "B")
        dag.add_dependency("B", "C")
        dag.add_pipeline("D")  # Unrelated

        execution_log = []

        async def mock_run(name: str) -> dict:
            execution_log.append(name)
            return {"status": "success"}

        runner = DAGRunner(dag, run_fn=mock_run)
        result = await runner.run_downstream("A")

        assert "D" not in execution_log
        assert "A" in execution_log

    @pytest.mark.asyncio
    async def test_concurrency_limit(self):
        dag = DAGResolver()
        for i in range(10):
            dag.add_pipeline(f"p{i}")

        max_concurrent = 0
        current = 0
        lock = asyncio.Lock()

        async def mock_run(name: str) -> dict:
            nonlocal max_concurrent, current
            async with lock:
                current += 1
                if current > max_concurrent:
                    max_concurrent = current
            await asyncio.sleep(0.01)
            async with lock:
                current -= 1
            return {"status": "success"}

        runner = DAGRunner(dag, run_fn=mock_run, max_parallel=3)
        result = await runner.run()

        assert result.status == "success"
        assert max_concurrent <= 3

    @pytest.mark.asyncio
    async def test_exception_in_run_fn(self):
        dag = DAGResolver()
        dag.add_pipeline("A")

        async def mock_run(name: str) -> dict:
            raise ValueError("unexpected error")

        runner = DAGRunner(dag, run_fn=mock_run)
        result = await runner.run()

        assert "A" in result.failed
        assert "unexpected error" in result.results["A"]["error"]

    @pytest.mark.asyncio
    async def test_result_has_timing(self):
        dag = DAGResolver()
        dag.add_pipeline("A")

        async def mock_run(name: str) -> dict:
            return {"status": "success"}

        runner = DAGRunner(dag, run_fn=mock_run)
        result = await runner.run()

        assert result.started_at is not None
        assert result.finished_at is not None
        assert result.duration_ms is not None
        assert result.duration_ms >= 0

    @pytest.mark.asyncio
    async def test_result_to_dict(self):
        dag = DAGResolver()
        dag.add_pipeline("A")

        async def mock_run(name: str) -> dict:
            return {"status": "success"}

        runner = DAGRunner(dag, run_fn=mock_run)
        result = await runner.run()
        d = result.to_dict()

        assert "status" in d
        assert "results" in d
        assert "execution_order" in d

    @pytest.mark.asyncio
    async def test_empty_dag_runs_ok(self):
        dag = DAGResolver()
        async def mock_run(name: str) -> dict:
            return {"status": "success"}
        runner = DAGRunner(dag, run_fn=mock_run)
        result = await runner.run()
        assert result.status == "success"
