"""Read config.yaml and work out which task runs after which.

config.yaml describes each report from the top down: the report, the
transformations it reads, and inside each transformation the ingestion
tables (and other transformations) it is built from.

Every table becomes a node of a graph, named "<layer>.<name>", for example
"ingestion.orders" or "reports.revenue_report". The graph is a plain
dictionary:

    graph[node] = the set of nodes it needs first

For the finance config this gives, for example:

    graph["ingestion.orders"]             = set()        # raw tables need nothing
    graph["transformation.order_revenue"] = {"ingestion.orders", "ingestion.order_items"}
    graph["transformation.net_revenue"]   = {"ingestion.refunds", "transformation.order_revenue"}
    graph["reports.revenue_report"]       = {"transformation.net_revenue"}

A table written under several reports is still one node, so it runs once.

From the graph we work out:
- the build order (topological: a node always comes after what it needs), and
- for each node, how many reports need it, so shared work can go first.

Run `python graph.py` to print the build order.
"""

from __future__ import annotations

from graphlib import TopologicalSorter
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
    first_seen: dict[str, str] = {}   # node -> where in the config it was first described
    errors: list[str] = []

    def add(node: str, needs: set[str], where: str) -> None:
        """Add a node. A node met again (a shared table) must need the same tables."""
        if node not in graph:
            graph[node] = needs
            first_seen[node] = where
        elif graph[node] != needs:
            errors.append(
                f"{node} is described differently in {first_seen[node]} "
                f"({', '.join(sorted(graph[node])) or 'nothing'}) and in {where} "
                f"({', '.join(sorted(needs)) or 'nothing'})"
            )

    def read_needs(body, where: str) -> set[str]:
        """Read the `ingestion:` and `transformation:` keys of a report or transformation.

        Adds every table found to the graph, going down into nested
        transformations, and returns the nodes this report or
        transformation needs directly.
        """
        if body is None:
            body = {}
        if not isinstance(body, dict):
            errors.append(f"{where} must have 'ingestion:' and/or 'transformation:' under it")
            return set()
        for key in body:
            if key not in ("ingestion", "transformation"):
                errors.append(f"{where} has an unknown key {key!r}")

        needs: set[str] = set()

        # `ingestion:` is a list of raw table names. Raw tables need nothing.
        ingestion = body.get("ingestion") or []
        if not isinstance(ingestion, list):
            errors.append(f"{where}.ingestion must be a list of table names")
            ingestion = []
        for name in ingestion:
            add(f"ingestion.{name}", set(), f"{where}.ingestion")
            needs.add(f"ingestion.{name}")

        # `transformation:` maps each transformation name to what it needs,
        # written the same way, so we read it with this same function.
        transformation = body.get("transformation") or {}
        if not isinstance(transformation, dict):
            errors.append(f"{where}.transformation must map each transformation to what it needs")
            transformation = {}
        for name, inner in transformation.items():
            inner_where = f"{where}.transformation.{name}"
            inner_needs = read_needs(inner, inner_where)
            if not inner_needs:
                errors.append(f"{inner_where} uses no tables")
            add(f"transformation.{name}", inner_needs, inner_where)
            needs.add(f"transformation.{name}")

        return needs

    # 1. Walk every report from the top down. Each report needs what is
    #    listed directly under it; nested tables are added on the way.
    reports = config.get("reports") if isinstance(config, dict) else None
    if not isinstance(reports, dict) or not reports:
        errors.append("config.yaml needs a 'reports:' key with at least one report")
        reports = {}
    for name, body in reports.items():
        where = f"reports.{name}"
        needs = read_needs(body, where)
        if not needs:
            errors.append(f"{where} uses no tables")
        add(where, needs, where)

    # 2. Every table needs its SQL script at <layer>/<name>.sql.
    #    (No separate cycle check is needed: nesting can only express
    #    "a needs b, b needs a" by describing a table in two different
    #    ways, which add() already reports.)
    for node in graph:
        layer, name = node.split(".")
        if not (scripts_dir / layer / f"{name}.sql").is_file():
            errors.append(f"{node} has no script {layer}/{name}.sql")

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
