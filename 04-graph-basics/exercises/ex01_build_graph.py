"""ex01 - Build a graph from a hand-written dict.

Run:  python ex01_build_graph.py

The dict says, for each node, which nodes it DEPENDS ON (its upstream).
From that we build two adjacency lists:
    upstream[node]   -> nodes that must run BEFORE node
    downstream[node] -> nodes that run AFTER node (they need node)

    extract_orders --> clean_orders --+
                                      +--> join --+--> report
    extract_users  --> clean_users  --+           +--> dashboard
"""

# node -> list of nodes it depends on
PIPELINE = {
    "extract_orders": [],
    "extract_users": [],
    "clean_orders": ["extract_orders"],
    "clean_users": ["extract_users"],
    "join": ["clean_orders", "clean_users"],
    "report": ["join"],
    "dashboard": ["join"],
}


def build_graph(depends_on):
    """Return (upstream, downstream) adjacency lists."""
    upstream = {node: [] for node in depends_on}
    downstream = {node: [] for node in depends_on}

    for node, parents in depends_on.items():
        for parent in parents:
            if parent not in depends_on:
                raise ValueError(f"'{node}' depends on unknown node '{parent}'")
            # One edge: parent --> node
            upstream[node].append(parent)
            downstream[parent].append(node)

    return upstream, downstream


def main():
    upstream, downstream = build_graph(PIPELINE)

    print(f"{'NODE':<16} {'UPSTREAM (runs before)':<30} DOWNSTREAM (runs after)")
    print("-" * 75)
    for node in PIPELINE:
        up = ", ".join(upstream[node]) or "-"
        down = ", ".join(downstream[node]) or "-"
        print(f"{node:<16} {up:<30} {down}")


if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------
# Try it yourself
#  1. Add a node "archive" that depends on "report". Run again. Which node
#     gained a downstream entry?
#  2. Write edge_list(downstream) that returns a list of (from, to) tuples.
#     Print every edge and the total number of nodes and edges.
# ---------------------------------------------------------------------------
