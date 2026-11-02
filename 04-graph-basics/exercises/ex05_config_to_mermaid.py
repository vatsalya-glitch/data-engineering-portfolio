"""ex05 - Build ONE graph from a config with 3 flows that share nodes.

Run:  python ex05_config_to_mermaid.py

The config lists flows. Each flow says, for each node, what it depends on.
Several flows mention extract_orders and clean_orders, but the graph keeps
each node ONCE. clean_orders then simply gets several downstream edges:

                                 +--> daily_sales          (sales)
    extract_orders -> clean_orders --> customer_360 -> ...  (marketing)
                                 +--> revenue_usd          (finance)

The same config in YAML would look like this (yaml.safe_load gives the
exact same dict, so the code below would not change):

    sales:
      extract_orders: []
      clean_orders: [extract_orders]
      daily_sales: [clean_orders]
    marketing:
      ...

Paste the printed Mermaid text into https://mermaid.live to see the picture.
"""

from collections import deque

CONFIG = {
    "sales": {
        "extract_orders": [],
        "clean_orders": ["extract_orders"],
        "daily_sales": ["clean_orders"],
    },
    "marketing": {
        "extract_orders": [],
        "clean_orders": ["extract_orders"],
        "extract_users": [],
        "clean_users": ["extract_users"],
        "customer_360": ["clean_orders", "clean_users"],
        "campaign_list": ["customer_360"],
    },
    "finance": {
        "extract_orders": [],
        "clean_orders": ["extract_orders"],
        "extract_fx_rates": [],
        "revenue_usd": ["clean_orders", "extract_fx_rates"],
    },
}


def build_graph_from_config(config):
    """Merge every flow into one graph.

    Returns:
        upstream   dict node -> list of parents
        downstream dict node -> list of children
        used_by    dict node -> list of flows that mention it
    """
    upstream, downstream, used_by = {}, {}, {}

    # Pass 1: create every node exactly once.
    for flow, nodes in config.items():
        for node in nodes:
            if node not in upstream:
                upstream[node] = []
                downstream[node] = []
                used_by[node] = []
            used_by[node].append(flow)

    # Pass 2: create edges, skipping ones we already have.
    for flow, nodes in config.items():
        for node, parents in nodes.items():
            for parent in parents:
                if parent not in upstream:
                    raise ValueError(
                        f"flow '{flow}': '{node}' depends on unknown node '{parent}'")
                if parent not in upstream[node]:      # no duplicate edges
                    upstream[node].append(parent)
                    downstream[parent].append(node)

    return upstream, downstream, used_by


def topological_sort(upstream, downstream):
    in_degree = {node: len(parents) for node, parents in upstream.items()}
    ready = deque(sorted(n for n, d in in_degree.items() if d == 0))
    order = []
    while ready:
        node = ready.popleft()
        order.append(node)
        for child in downstream[node]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                ready.append(child)
    if len(order) != len(upstream):
        stuck = sorted(n for n in upstream if n not in order)
        raise ValueError(f"Cycle detected among: {stuck}")
    return order


def to_mermaid(downstream, used_by):
    lines = ["graph LR"]
    for parent, children in downstream.items():
        for child in children:
            lines.append(f"    {parent} --> {child}")
    shared = [n for n, flows in used_by.items() if len(flows) > 1]
    if shared:
        lines.append("    classDef shared fill:#ffe08a,stroke:#b8860b")
        lines.append(f"    class {','.join(shared)} shared")
    return "\n".join(lines)


def main():
    upstream, downstream, used_by = build_graph_from_config(CONFIG)

    mentions = sum(len(nodes) for nodes in CONFIG.values())
    edges = sum(len(children) for children in downstream.values())
    print(f"Config mentions {mentions} nodes in {len(CONFIG)} flows")
    print(f"Graph has {len(upstream)} unique nodes and {edges} edges")
    print()

    print("Shared nodes:")
    for node, flows in used_by.items():
        if len(flows) > 1:
            print(f"  {node:<15} used by {flows}  -> downstream {downstream[node]}")
    print()

    print("Run order:", " -> ".join(topological_sort(upstream, downstream)))
    print()

    print("Mermaid (paste into https://mermaid.live):")
    print(to_mermaid(downstream, used_by))


if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------
# Try it yourself
#  1. Add a 4th flow "quality" with a node "dq_checks" that depends on
#     "clean_orders" and "clean_users". Change ONLY the CONFIG. Run again:
#     which nodes are now shared, and how many downstream edges does
#     clean_orders have?
#  2. Write run_order_for_flow(flow) that returns the run order of ONLY the
#     nodes one flow needs (the flow's nodes plus all their ancestors), in a
#     valid order. Try it on "finance".
# ---------------------------------------------------------------------------
