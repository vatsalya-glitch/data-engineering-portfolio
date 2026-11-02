"""ex02 - Roots, leaves, ancestors and descendants.

Run:  python ex02_roots_leaves_family.py

    extract_orders --> clean_orders --+
                                      +--> join --+--> report
    extract_users  --> clean_users  --+           +--> dashboard

    roots  = nodes with no upstream   (where the pipeline starts)
    leaves = nodes with no downstream (where the pipeline ends)
    ancestors(n)   = everything upstream of n, direct AND indirect
    descendants(n) = everything downstream of n, direct AND indirect
"""

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
    upstream = {node: [] for node in depends_on}
    downstream = {node: [] for node in depends_on}
    for node, parents in depends_on.items():
        for parent in parents:
            if parent not in depends_on:
                raise ValueError(f"'{node}' depends on unknown node '{parent}'")
            upstream[node].append(parent)
            downstream[parent].append(node)
    return upstream, downstream


def roots(upstream):
    return [node for node, parents in upstream.items() if not parents]


def leaves(downstream):
    return [node for node, children in downstream.items() if not children]


def walk(start, neighbours):
    """Collect every node reachable from start by following `neighbours`.

    Pass the upstream dict to get ancestors, the downstream dict to get
    descendants. Uses a simple to-do list (a stack) and a 'seen' set so each
    node is visited only once.
    """
    seen = set()
    todo = list(neighbours[start])
    while todo:
        node = todo.pop()
        if node in seen:
            continue
        seen.add(node)
        todo.extend(neighbours[node])
    return seen


def ancestors(node, upstream):
    return walk(node, upstream)


def descendants(node, downstream):
    return walk(node, downstream)


def main():
    upstream, downstream = build_graph(PIPELINE)

    print("Roots :", roots(upstream))
    print("Leaves:", leaves(downstream))
    print()

    for node in ["clean_orders", "join"]:
        print(f"Node '{node}'")
        print("  direct upstream  :", upstream[node])
        print("  all ancestors    :", sorted(ancestors(node, upstream)))
        print("  direct downstream:", downstream[node])
        print("  all descendants  :", sorted(descendants(node, downstream)))
        print()


if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------
# Try it yourself
#  1. Write is_upstream_of(a, b) that returns True if a must run (directly or
#     indirectly) before b. Test: ("extract_users", "report") -> True,
#     ("report", "join") -> False.
#  2. Print the nodes that are NEITHER ancestors NOR descendants of
#     "clean_orders" (and not clean_orders itself). These nodes don't depend
#     on it in any way, so they could run at the same time as it.
# ---------------------------------------------------------------------------
