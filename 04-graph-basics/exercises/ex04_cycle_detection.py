"""ex04 - Add a cycle and detect it with a clear error message.

Run:  python ex04_cycle_detection.py

Kahn's algorithm only runs a node when ALL its parents are done. Nodes on a
cycle wait for each other forever, so they are never run. After the loop,
any node left over means "cycle".

To name the actual cycle: every leftover node still waits for at least one
leftover parent. So start at any leftover node and keep stepping to a
leftover parent. Sooner or later you visit a node twice: that loop is the
cycle.

    extract_orders --> clean_orders --> join --> report
                            ^                      |
                            +----------------------+   (bad edge)
"""

from collections import deque

GOOD_PIPELINE = {
    "extract_orders": [],
    "extract_users": [],
    "clean_orders": ["extract_orders"],
    "clean_users": ["extract_users"],
    "join": ["clean_orders", "clean_users"],
    "report": ["join"],
    "dashboard": ["join"],
}

# Same pipeline, but clean_orders now ALSO depends on report -> cycle.
BAD_PIPELINE = dict(GOOD_PIPELINE)
BAD_PIPELINE["clean_orders"] = ["extract_orders", "report"]


class CycleError(Exception):
    pass


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


def find_cycle(leftover, upstream):
    """Walk upstream through leftover nodes until one repeats."""
    path = []
    position = {}                     # node -> index in path
    node = sorted(leftover)[0]
    while node not in position:
        position[node] = len(path)
        path.append(node)
        # Pick a parent that is also stuck (there is always at least one).
        node = next(p for p in upstream[node] if p in leftover)
    cycle = path[position[node]:]     # the loop part only
    cycle.reverse()                   # show it in run direction (parent -> child)
    return cycle + [cycle[0]]         # close the loop for printing


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
        leftover = {n for n in upstream if n not in order}
        cycle = find_cycle(leftover, upstream)
        raise CycleError(
            "Cycle found: " + " -> ".join(cycle) + "\n"
            "Each task in this loop waits for the one before it, so none can start.\n"
            "Fix: remove one of these dependencies in the config."
        )
    return order


def try_pipeline(name, depends_on):
    print(f"== {name} ==")
    upstream, downstream = build_graph(depends_on)
    try:
        order = topological_sort(upstream, downstream)
        print("OK, run order:", " -> ".join(order))
    except CycleError as err:
        print("ERROR:", err)
    print()


def main():
    try_pipeline("good pipeline", GOOD_PIPELINE)
    try_pipeline("bad pipeline", BAD_PIPELINE)


if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------
# Try it yourself
#  1. Make a node depend on itself: "join": ["clean_orders", "clean_users",
#     "join"]. Is it detected? What does the message say?
#  2. In the bad pipeline, some nodes are stuck only because they come AFTER
#     the cycle (they are not part of it). Add a line to the error message
#     listing these "blocked" nodes. Expected for BAD_PIPELINE: dashboard.
# ---------------------------------------------------------------------------
