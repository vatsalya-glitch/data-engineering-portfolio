"""Dependency graph of a pipeline. No Airflow imports.

The graph is a mapping `node -> the nodes it uses`. Sources use nothing;
models use sources and intermediates.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from graphlib import CycleError, TopologicalSorter

Graph = Mapping[str, Iterable[str]]


class GraphCycleError(ValueError):
    """The graph has a cycle. `cycle` lists it, e.g. ["a", "b", "a"]."""

    def __init__(self, cycle: list[str]):
        self.cycle = cycle
        super().__init__("cycle in uses: " + " -> ".join(cycle))


def _canonical_cycle(cycle: list[str]) -> list[str]:
    """Write a cycle as 'a uses b uses ... uses a', starting from its smallest name.

    graphlib lists a cycle so that each node is a dependency of the next one.
    We reverse it, so each arrow reads "uses", and rotate it so the same
    cycle always gives the same message.
    """
    nodes = list(reversed(cycle))[:-1]  # first and last are the same node
    start = nodes.index(min(nodes))
    nodes = nodes[start:] + nodes[:start]
    return nodes + [nodes[0]]


def topological_order(graph: Graph, priority: Mapping[str, int] | None = None) -> list[str]:
    """Every node after the nodes it uses.

    Among nodes that are ready at the same time, higher `priority` comes
    first, then the name, so the order is stable.
    """
    priority = priority or {}
    sorter = TopologicalSorter({node: sorted(uses) for node, uses in graph.items()})
    try:
        sorter.prepare()
    except CycleError as error:
        raise GraphCycleError(_canonical_cycle(error.args[1])) from None
    order: list[str] = []
    while sorter.is_active():
        ready = sorted(sorter.get_ready(), key=lambda node: (-priority.get(node, 0), node))
        order.extend(ready)
        sorter.done(*ready)
    return order


def build_order(graph: Graph, reports: Iterable[str]) -> list[str]:
    """Topological order in which, at every point, the tables most reports share come first."""
    return topological_order(graph, report_usage_counts(graph, reports))


def upstream(graph: Graph, node: str) -> set[str]:
    """All nodes `node` uses, directly or through other nodes (not including itself)."""
    seen: set[str] = set()
    stack = list(graph.get(node, ()))
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        stack.extend(graph.get(current, ()))
    return seen


def report_usage_counts(graph: Graph, reports: Iterable[str]) -> dict[str, int]:
    """For every node, how many reports depend on it, directly or indirectly.

    A report counts itself, so every report gets at least 1. A node no
    report uses gets 0.
    """
    counts = {node: 0 for node in graph}
    for report in reports:
        for node in upstream(graph, report) | {report}:
            counts[node] = counts.get(node, 0) + 1
    return counts
