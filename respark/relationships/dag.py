from dataclasses import dataclass
from collections import deque
from typing import Iterable, Tuple, Mapping, List, Dict, Set, Self

# Defining datatypes for DAG module

Node = str
EdgePair = Tuple[Node, Node]
Layers = List[List[Node]]


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

    nodes: Tuple[Node, ...]
    edges: Tuple[EdgePair, ...]

    @classmethod
    def build(
        cls,
        input_nodes: Iterable[Node],
        input_edges: Iterable[Mapping[str, str]],
    ) -> Self:
        """
        Build a DAG from a list of nodes and dict-style edges:
        each edge mapping must have keys "start_node" and "end_node".
        """
        nodes: Set[Node] = set(input_nodes)
        pairs: Set[EdgePair] = set()

        for e in input_edges:
            u = e["start_node"]
            v = e["end_node"]
            if u in nodes and v in nodes:
                pairs.add((u, v))

        return cls(
            nodes=tuple(sorted(nodes)),
            edges=tuple(sorted(pairs)),
        )

    def compute_layers(self) -> Layers:
        """
        Layered topological sort with Khan's algorithm.

        Each inner list is a 'wave' that can be
        generated in parallel.

        Raises CycleError if the graph is not a DAG.
        """

        adj: Dict[Node, Set[Node]] = {n: set() for n in self.nodes}
        indeg: Dict[Node, int] = {n: 0 for n in self.nodes}

        for u, v in set(self.edges):
            adj[u].add(v)
            indeg[v] += 1

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
