"""Read config.yaml and work out which task runs after which.

Every table is a node named "<layer>.<name>", for example "ingestion.orders".
A node depends on the tables listed under its `ingestion:` and
`transformation:` keys. Ingestion tables depend on nothing.

    graph[node] = the nodes it needs first

The order is topological: a node runs only after the nodes it needs. When
several nodes are ready at the same time, the one used by the most reports
goes first.
"""

from __future__ import annotations

from graphlib import CycleError, TopologicalSorter
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
LAYERS = ("ingestion", "transformation", "reports")


def load_config(path: Path = HERE / "config.yaml") -> dict:
    with open(path) as file:
        return yaml.safe_load(file)


def build_graph(config: dict, scripts_dir: Path = HERE) -> dict[str, set[str]]:
    """node -> the nodes it needs. Raises ValueError listing every mistake in the config."""
    graph: dict[str, set[str]] = {}
    errors: list[str] = []

    for name in config.get("ingestion") or []:
        graph[f"ingestion.{name}"] = set()
    for layer in ("transformation", "reports"):
        for name, uses in (config.get(layer) or {}).items():
            uses = uses or {}
            needs = {f"{used_layer}.{used}"
                     for used_layer in ("ingestion", "transformation")
                     for used in uses.get(used_layer, [])}
            if not needs:
                errors.append(f"{layer}.{name} uses no tables")
            graph[f"{layer}.{name}"] = needs

    for node, needs in graph.items():
        for need in needs:
            if need not in graph:
                errors.append(f"{node} uses {need}, which is not in the config")
        layer, name = node.split(".")
        if not (scripts_dir / layer / f"{name}.sql").is_file():
            errors.append(f"{node} has no script {layer}/{name}.sql")

    if not errors:
        try:
            TopologicalSorter(graph).prepare()
        except CycleError as error:
            errors.append("cycle: " + " -> ".join(error.args[1]))
    if errors:
        raise ValueError("config.yaml has problems:\n  - " + "\n  - ".join(errors))
    return graph


def reports_using(graph: dict[str, set[str]]) -> dict[str, int]:
    """node -> how many reports need it, directly or through other tables."""
    counts = {node: 0 for node in graph}
    for report in (node for node in graph if node.startswith("reports.")):
        seen, stack = set(), [report]
        while stack:
            node = stack.pop()
            if node not in seen:
                seen.add(node)
                stack.extend(graph[node])
        for node in seen:
            counts[node] += 1
    return counts


def build_order(graph: dict[str, set[str]]) -> list[str]:
    """Topological order; among nodes ready together, the most-used comes first."""
    counts = reports_using(graph)
    sorter = TopologicalSorter(graph)
    sorter.prepare()
    order: list[str] = []
    while sorter.is_active():
        ready = sorted(sorter.get_ready(), key=lambda node: (-counts[node], node))
        order.extend(ready)
        sorter.done(*ready)
    return order


if __name__ == "__main__":
    graph = build_graph(load_config())
    counts = reports_using(graph)
    for number, node in enumerate(build_order(graph), start=1):
        needs = ", ".join(sorted(graph[node])) or "-"
        print(f"{number:>2}. {node:<45} used by {counts[node]} report(s)   needs: {needs}")
