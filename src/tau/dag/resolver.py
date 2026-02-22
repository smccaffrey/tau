"""DAG dependency resolver — topological sort, cycle detection, parallel groups."""

from __future__ import annotations
from collections import defaultdict, deque
from dataclasses import dataclass, field


@dataclass
class DAGNode:
    """A node in the dependency graph."""
    name: str
    upstream: set[str] = field(default_factory=set)
    downstream: set[str] = field(default_factory=set)


class CycleError(Exception):
    """Raised when a dependency cycle is detected."""
    def __init__(self, cycle: list[str]):
        self.cycle = cycle
        super().__init__(f"Dependency cycle detected: {' → '.join(cycle)}")


class DAGResolver:
    """Resolves pipeline dependency graphs into execution order."""

    def __init__(self):
        self._nodes: dict[str, DAGNode] = {}

    def add_pipeline(self, name: str) -> None:
        """Register a pipeline node."""
        if name not in self._nodes:
            self._nodes[name] = DAGNode(name=name)

    def add_dependency(self, upstream: str, downstream: str) -> None:
        """Add a dependency: downstream depends on upstream."""
        self.add_pipeline(upstream)
        self.add_pipeline(downstream)
        self._nodes[upstream].downstream.add(downstream)
        self._nodes[downstream].upstream.add(upstream)

    def add_dependencies(self, pipeline: str, depends_on: list[str]) -> None:
        """Add multiple dependencies for a pipeline."""
        for dep in depends_on:
            self.add_dependency(upstream=dep, downstream=pipeline)

    @property
    def nodes(self) -> dict[str, DAGNode]:
        return self._nodes

    def get_upstream(self, name: str) -> set[str]:
        """Get all transitive upstream dependencies."""
        visited = set()
        queue = deque([name])
        while queue:
            node = queue.popleft()
            if node in visited or node not in self._nodes:
                continue
            visited.add(node)
            queue.extend(self._nodes[node].upstream)
        visited.discard(name)
        return visited

    def get_downstream(self, name: str) -> set[str]:
        """Get all transitive downstream dependents."""
        visited = set()
        queue = deque([name])
        while queue:
            node = queue.popleft()
            if node in visited or node not in self._nodes:
                continue
            visited.add(node)
            queue.extend(self._nodes[node].downstream)
        visited.discard(name)
        return visited

    def detect_cycles(self) -> list[str] | None:
        """Detect cycles using DFS. Returns cycle path or None."""
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {n: WHITE for n in self._nodes}
        parent = {}

        def dfs(node: str) -> list[str] | None:
            color[node] = GRAY
            for child in self._nodes[node].downstream:
                if child not in color:
                    continue
                if color[child] == GRAY:
                    # Found cycle — reconstruct it
                    cycle = [child, node]
                    current = node
                    while current in parent and parent[current] != child:
                        current = parent[current]
                        cycle.append(current)
                    cycle.reverse()
                    return cycle
                if color[child] == WHITE:
                    parent[child] = node
                    result = dfs(child)
                    if result:
                        return result
            color[node] = BLACK
            return None

        for node in self._nodes:
            if color[node] == WHITE:
                result = dfs(node)
                if result:
                    return result
        return None

    def topological_sort(self) -> list[str]:
        """Return pipelines in dependency order (upstream first).

        Raises CycleError if a cycle is detected.
        """
        cycle = self.detect_cycles()
        if cycle:
            raise CycleError(cycle)

        in_degree = {n: len(self._nodes[n].upstream) for n in self._nodes}
        queue = deque([n for n, d in in_degree.items() if d == 0])
        result = []

        while queue:
            node = queue.popleft()
            result.append(node)
            for child in self._nodes[node].downstream:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(result) != len(self._nodes):
            # Shouldn't happen since we checked for cycles
            raise CycleError(["unknown"])

        return result

    def parallel_groups(self) -> list[list[str]]:
        """Return execution groups — pipelines in the same group can run in parallel.

        Each group only runs after all previous groups have completed.
        """
        cycle = self.detect_cycles()
        if cycle:
            raise CycleError(cycle)

        in_degree = {n: len(self._nodes[n].upstream) for n in self._nodes}
        current_group = [n for n, d in in_degree.items() if d == 0]
        groups = []

        while current_group:
            groups.append(sorted(current_group))  # Sort for deterministic output
            next_group = []
            for node in current_group:
                for child in self._nodes[node].downstream:
                    in_degree[child] -= 1
                    if in_degree[child] == 0:
                        next_group.append(child)
            current_group = next_group

        return groups

    def get_subgraph(self, root: str) -> "DAGResolver":
        """Get a sub-DAG rooted at a specific pipeline (all downstream)."""
        downstream = self.get_downstream(root)
        downstream.add(root)

        sub = DAGResolver()
        for name in downstream:
            sub.add_pipeline(name)
        for name in downstream:
            node = self._nodes[name]
            for child in node.downstream:
                if child in downstream:
                    sub.add_dependency(name, child)
        return sub

    def to_dict(self) -> dict:
        """Serialize the DAG for JSON output."""
        return {
            "nodes": sorted(self._nodes.keys()),
            "edges": [
                {"upstream": name, "downstream": child}
                for name, node in self._nodes.items()
                for child in node.downstream
            ],
            "groups": self.parallel_groups(),
        }
