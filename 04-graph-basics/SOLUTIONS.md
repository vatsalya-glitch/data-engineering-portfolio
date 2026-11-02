# Solutions: "try it yourself"

Try the tasks first. To test a solution, paste the code at the bottom of the exercise file.
Put it in place of the `if __name__ == "__main__":` block, or after it.

---

## ex01: build a graph

**1. Add `archive` after `report`.** Add one line to `PIPELINE`:

```python
"archive": ["report"],
```

`report` now shows `archive` as its downstream. `archive` shows `report` as its upstream.
You changed only the data, not the code.

**2. Edge list and counts.**

```python
def edge_list(downstream):
    return [(parent, child) for parent, children in downstream.items() for child in children]

upstream, downstream = build_graph(PIPELINE)
edges = edge_list(downstream)
for a, b in edges:
    print(f"{a} --> {b}")
print(f"{len(downstream)} nodes, {len(edges)} edges")
```

Output without `archive`: `7 nodes, 6 edges`. With `archive`: `8 nodes, 7 edges`.

---

## ex02: roots, leaves, family

**1. `is_upstream_of(a, b)`.** A runs before b if a is one of b's ancestors.

```python
upstream, downstream = build_graph(PIPELINE)

def is_upstream_of(a, b):
    return a in ancestors(b, upstream)

print(is_upstream_of("extract_users", "report"))   # True
print(is_upstream_of("report", "join"))            # False
```

**2. Nodes independent of `clean_orders`.**

```python
node = "clean_orders"
related = ancestors(node, upstream) | descendants(node, downstream) | {node}
print(sorted(set(PIPELINE) - related))   # ['clean_users', 'extract_users']
```

The users branch doesn't depend on the orders branch in any way, so they can run side by side.

---

## ex03: topological sort

**1. Levels (batches that can run in parallel).** Go through the nodes in topological order.
A node's level is 1 more than the highest level among its parents. A root is level 0.

```python
upstream, downstream = build_graph(PIPELINE)

def levels(upstream):
    level = {}
    for node in topological_sort(upstream, downstream):
        level[node] = max((level[p] + 1 for p in upstream[node]), default=0)
    groups = {}
    for node, lvl in level.items():
        groups.setdefault(lvl, []).append(node)
    return groups

for lvl, nodes in levels(upstream).items():
    print(f"level {lvl}: {nodes}")
```

```
level 0: ['extract_orders', 'extract_users']
level 1: ['clean_orders', 'clean_users']
level 2: ['join']
level 3: ['report', 'dashboard']
```

**2. `is_valid_order`.** Every parent must sit at an earlier position than its child.

```python
def is_valid_order(order, upstream):
    position = {node: i for i, node in enumerate(order)}
    return all(position[p] < position[node]
               for node in upstream for p in upstream[node])

good = topological_sort(upstream, downstream)
bad = ["report"] + [n for n in good if n != "report"]
print(is_valid_order(good, upstream), is_valid_order(bad, upstream))  # True False
```

---

## ex04: cycle detection

**1. Self-loop.** Yes, it's detected. A node that depends on itself is the smallest possible cycle.

```python
SELF_LOOP = dict(GOOD_PIPELINE)
SELF_LOOP["join"] = ["clean_orders", "clean_users", "join"]
try_pipeline("self loop", SELF_LOOP)
```

```
ERROR: Cycle found: join -> join
```

**2. List blocked nodes.** Blocked nodes are the ones left over that aren't on the cycle.
Change the end of `topological_sort`:

```python
    if len(order) != len(upstream):
        leftover = {n for n in upstream if n not in order}
        cycle = find_cycle(leftover, upstream)
        blocked = sorted(leftover - set(cycle))
        raise CycleError(
            "Cycle found: " + " -> ".join(cycle) + "\n"
            "Each task in this loop waits for the one before it, so none can start.\n"
            f"Blocked (after the cycle): {', '.join(blocked) or '-'}\n"
            "Fix: remove one of these dependencies in the config."
        )
```

```
ERROR: Cycle found: join -> report -> clean_orders -> join
Each task in this loop waits for the one before it, so none can start.
Blocked (after the cycle): dashboard
Fix: remove one of these dependencies in the config.
```

---

## ex05: config with shared nodes

**1. Add a 4th flow, config only.**

```python
CONFIG["quality"] = {
    "clean_orders": ["extract_orders"],
    "clean_users": ["extract_users"],
    "dq_checks": ["clean_orders", "clean_users"],
}
```

You could also just write `{"dq_checks": ["clean_orders", "clean_users"]}`.
The builder creates every node before it adds any edge, so a flow can point at nodes that another flow defines.

Result:
- The graph now has 10 unique nodes and 10 edges.
- `clean_users` is now shared too.
- `clean_orders` has **4** downstream edges: `daily_sales`, `customer_360`, `revenue_usd` and `dq_checks`.

**2. Run order for one flow only.** Collect the flow's nodes plus all their ancestors.
Then keep the global topological order, but filter it down to those nodes.

```python
upstream, downstream, used_by = build_graph_from_config(CONFIG)

def run_order_for_flow(flow):
    needed = set()
    for node in CONFIG[flow]:
        needed.add(node)
        stack = list(upstream[node])
        while stack:
            parent = stack.pop()
            if parent not in needed:
                needed.add(parent)
                stack.extend(upstream[parent])
    full_order = topological_sort(upstream, downstream)
    return [n for n in full_order if n in needed]

print("finance:", " -> ".join(run_order_for_flow("finance")))
# finance: extract_fx_rates -> extract_orders -> clean_orders -> revenue_usd
```

A filtered topological order is still valid. Removing nodes never puts a child before its parent.
