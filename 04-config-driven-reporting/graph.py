"""Read config.yaml and work out which task runs after which.

Every table in the config becomes a node of a graph, named "<layer>.<name>",
for example "ingestion.orders" or "reports.revenue_report". The graph is a
plain dictionary:

    graph[node] = the set of nodes it needs first

For the finance config this gives, for example:

    graph["ingestion.orders"]             = set()        # raw tables need nothing
    graph["transformation.order_revenue"] = {"ingestion.orders", "ingestion.order_items"}
    graph["reports.revenue_report"]       = {"transformation.net_revenue"}

From the graph we work out:
- the build order (topological: a node always comes after what it needs), and
- for each node, how many reports need it, so shared work can go first.

Run `python graph.py` to print the build order.
"""

from __future__ import annotations

from graphlib import CycleError, TopologicalSorter
from pathlib import Path

import yaml

# The folder this file is in. config.yaml and the script folders
# (ingestion/, transformation/, reports/) live next to it.
HERE = Path(__file__).resolve().parent


def load_config(path: Path = HERE / "config.yaml") -> dict:
    """Read config.yaml into a dictionary."""
    with open(path) as file:
        return yaml.safe_load(file)


def build_graph(config: dict, scripts_dir: Path = HERE) -> dict[str, set[str]]:
    """Turn the config into `node -> nodes it needs`.

    Checks the config on the way. If anything is wrong, raises ValueError
    listing every problem at once, so they can all be fixed in one go.
    """
    graph: dict[str, set[str]] = {}
    errors: list[str] = []

    # 1. Ingestion tables are copied straight from the database,
    #    so they need nothing: they have no incoming arrows.
    for name in config.get("ingestion") or []:
        graph[f"ingestion.{name}"] = set()

    # 2. Transformations and reports need the tables listed under their
    #    `ingestion:` and `transformation:` keys. For example
    #        net_revenue: {ingestion: [refunds], transformation: [order_revenue]}
    #    becomes
    #        "transformation.net_revenue" -> {"ingestion.refunds", "transformation.order_revenue"}
    for layer in ("transformation", "reports"):
        for name, uses in (config.get(layer) or {}).items():
            uses = uses or {}
            needs = {
                f"{used_layer}.{used}"
                for used_layer in ("ingestion", "transformation")
                for used in uses.get(used_layer, [])
            }
            if not needs:
                errors.append(f"{layer}.{name} uses no tables")
            graph[f"{layer}.{name}"] = needs

    # 3. Check every node: each table it needs must be in the config,
    #    and it must have its SQL script at <layer>/<name>.sql.
    for node, needs in graph.items():
        for need in needs:
            if need not in graph:
                errors.append(f"{node} uses {need}, which is not in the config")
        layer, name = node.split(".")
        if not (scripts_dir / layer / f"{name}.sql").is_file():
            errors.append(f"{node} has no script {layer}/{name}.sql")

    # 4. Check there is no cycle (a needs b, b needs a), because then no
    #    order could work. TopologicalSorter.prepare() raises CycleError
    #    and tells us the nodes in the loop.
    if not errors:
        try:
            TopologicalSorter(graph).prepare()
        except CycleError as error:
            errors.append("cycle: " + " -> ".join(error.args[1]))

    if errors:
        raise ValueError("config.yaml has problems:\n  - " + "\n  - ".join(errors))
    return graph


def reports_using(graph: dict[str, set[str]]) -> dict[str, int]:
    """For every node, count how many reports need it, directly or through other tables.

    Example: revenue_report needs net_revenue, which needs order_revenue,
    which needs orders. So orders counts revenue_report too, even though
    the report doesn't list it.
    """
    counts = {node: 0 for node in graph}
    reports = [node for node in graph if node.startswith("reports.")]

    for report in reports:
        # Walk backwards from the report through everything it needs
        # (a depth-first search), collecting each node once in `seen`.
        seen: set[str] = set()
        stack = [report]
        while stack:
            node = stack.pop()
            if node not in seen:
                seen.add(node)
                stack.extend(graph[node])
        # Every node we reached is needed by this report.
        for node in seen:
            counts[node] += 1
    return counts


def build_order(graph: dict[str, set[str]]) -> list[str]:
    """The order to build the tables in.

    Topological: a node always comes after the nodes it needs. At each
    point, several nodes can be ready at once (everything they need is
    done). Among those, the one needed by the most reports comes first;
    ties are broken by name so the order is always the same.
    """
    counts = reports_using(graph)
    sorter = TopologicalSorter(graph)
    sorter.prepare()

    order: list[str] = []
    while sorter.is_active():
        # All nodes whose needs are done, most-used first.
        ready = sorted(sorter.get_ready(), key=lambda node: (-counts[node], node))
        order.extend(ready)
        # Mark them done, which makes the nodes that need them ready next.
        sorter.done(*ready)
    return order


if __name__ == "__main__":
    # `python graph.py`: check the config and print the build order.
    graph = build_graph(load_config())
    counts = reports_using(graph)
    for number, node in enumerate(build_order(graph), start=1):
        needs = ", ".join(sorted(graph[node])) or "-"
        print(f"{number:>2}. {node:<45} used by {counts[node]} report(s)   needs: {needs}")
