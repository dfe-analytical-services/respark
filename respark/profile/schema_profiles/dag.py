from dataclasses import dataclass
from collections import deque
from typing import Iterable, Tuple, List, Dict, Set, Self
from .types import FkConstraint

Table = str
Edge = Tuple[Table, Table]  # parent -> child (pk_table -> fk_table)
Layers = List[List[Table]]


class CycleError(ValueError):
    """Raised when a cycle is detected.
    This happens when the passed graph is not a Deterministic Acyclical Graph.
    """

    pass


@dataclass(frozen=True, slots=True)
class DAG:
    """
    DAG Class for planning generation order.

    nodes: A tuple of table names as strings, act as the nodes in the graph
    edges: A tuple of tuples in form (pk_table -> fk_table) to show parent -> child relationship,
           acting as the edges in the graph

    """

    nodes: Tuple[Table, ...]
    edges: Tuple[Edge, ...]

    @classmethod
    def from_fk_constraints(
        cls, table_names: Iterable[Table], constraints: Dict[str, FkConstraint]
    ) -> Self:
        """
        Build a DAG restricted to the given table_names. Any constraint that
        references a table outside table_names is ignored.
        """
        nodes: Set[Table] = set(table_names)
        edges: Set[Edge] = set()
        for c in constraints.values():
            if c.pk_table in nodes and c.fk_table in nodes:
                edges.add((c.pk_table, c.fk_table))

        return cls(
            nodes=tuple(sorted(nodes)),
            edges=tuple(sorted(edges)),
        )

    def _adj_and_indegree(self) -> Tuple[Dict[Table, Set[Table]], Dict[Table, int]]:
        adj: Dict[Table, Set[Table]] = {n: set() for n in self.nodes}
        indeg: Dict[Table, int] = {n: 0 for n in self.nodes}

        # Need to go back and review this set(self.edges) later - will a child table ever have more than
        # 1 dependency on a parent table?
        for u, v in set(self.edges):
            adj[u].add(v)
            indeg[v] += 1

        return adj, indeg

    def compute_layers(self) -> Layers:
        """
        Layered topological sort with Khan's algorithm.

        Each inner list is a 'wave' that can be
        generated in parallel.

        Raises CycleError if the graph is not a DAG.
        """
        adj, indeg = self._adj_and_indegree()

        # Start with all zero-indegree nodes (including isolated).
        # E.g start with tables that have no parents,
        # or tables with both no parents or children
        zero_degrees = deque(sorted([n for n in self.nodes if indeg[n] == 0]))
        layer_results: Layers = []
        visited = 0

        while zero_degrees:

            wave = list(zero_degrees)
            layer_results.append(wave)
            zero_degrees.clear()

            for u in wave:
                visited += 1
                for v in sorted(adj[u]):
                    indeg[v] -= 1
                    if indeg[v] == 0:
                        zero_degrees.append(v)

        if visited != len(self.nodes):
            raise CycleError("Cycle detected—DAG required.")

        return layer_results
