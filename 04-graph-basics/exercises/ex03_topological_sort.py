"""ex03 - Topological sort with Kahn's algorithm, written by hand.

Run:  python ex03_topological_sort.py

Idea in one sentence: repeatedly run a node whose upstream is all done.

    1. in_degree[n] = how many upstream nodes n is still waiting for
    2. ready = all nodes with in_degree 0 (the roots)
    3. while ready is not empty:
         take one node from ready, add it to the run order
         for each downstream child: in_degree[child] -= 1
             if it hits 0, the child is ready
    4. if the order is shorter than the node count -> there is a cycle
"""

from collections import deque

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


def topological_sort(upstream, downstream, verbose=False):
    # Step 1: count how many parents each node waits for.
    in_degree = {node: len(parents) for node, parents in upstream.items()}

    # Step 2: nodes waiting for nobody are ready now. (sorted = stable output)
    ready = deque(sorted(n for n, d in in_degree.items() if d == 0))
    order = []
    step = 0

    # Step 3: run ready nodes, and "unlock" their children.
    while ready:
        node = ready.popleft()
        order.append(node)
        step += 1
        unlocked = []
        for child in downstream[node]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                ready.append(child)
                unlocked.append(child)
        if verbose:
            print(f"step {step}: run {node:<15} unlocked {unlocked or '-'}"
                  f"   ready now {list(ready)}")

    # Step 4: anything left was never unlocked -> it sits on (or after) a cycle.
    if len(order) != len(upstream):
        raise ValueError("Graph has a cycle; no valid run order exists")
    return order


def main():
    upstream, downstream = build_graph(PIPELINE)
    order = topological_sort(upstream, downstream, verbose=True)
    print()
    print("Run order:")
    for i, node in enumerate(order, start=1):
        print(f"  {i}. {node}")


if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------
# Try it yourself
#  1. Group the nodes into "levels": level 0 = roots, level 1 = nodes whose
#     parents are all in level 0, and so on. Nodes in one level can run in
#     parallel. Expected: 4 levels for this pipeline.
#  2. Write is_valid_order(order, upstream) that returns True only if every
#     node appears after all of its upstream nodes. Check it on the computed
#     order and on a bad one, e.g. with "report" first.
# ---------------------------------------------------------------------------
