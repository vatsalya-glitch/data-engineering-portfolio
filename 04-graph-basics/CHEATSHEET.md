# Graph Cheat Sheet for Pipelines

## Terms

| Term | Meaning | Picture |
|------|---------|---------|
| Node | One task / step | `[clean]` |
| Edge | "A must run before B". It points from first to after | `A --> B` |
| Upstream (parent) | Direct "before me" | **A** --> me |
| Downstream (child) | Direct "after me" | me --> **B** |
| Ancestors | All upstream, direct + indirect | A --> X --> me |
| Descendants | All downstream, direct + indirect | me --> X --> B |
| Root | No upstream: the pipeline starts here | `(start) R -->` |
| Leaf | No downstream: a final product | `--> L (end)` |
| In-degree | How many parents a node still waits for | `join` = 2 |
| Cycle | Following the arrows brings you back to the start | `A -> B -> A` |
| DAG | Directed (arrows) + Acyclic (no cycles) = runnable | |
| Topological order | Every node comes after all its parents | |
| Shared node | Used by several flows: one node, many downstream edges | `X -->{B, C}` |

## Data structures

```python
# Config: node -> what it depends on (this is what you write)
depends_on = {"a": [], "b": ["a"], "c": ["a", "b"]}

# Adjacency lists (built by code)
upstream   = {"a": [],         "b": ["a"], "c": ["a", "b"]}   # what do I need?
downstream = {"a": ["b", "c"], "b": ["c"], "c": []}           # who needs me?

# Edge list
edges = [("a", "b"), ("a", "c"), ("b", "c")]
```

## Config → graph → run order

```
config ──► 1. create nodes ──► 2. add edges ──► 3. validate ──► 4. Kahn ──► run order
            (once each)      (parent -> node,   (unknown        (or a
                              no duplicates)     parents?)       cycle error)
```

1. **Create nodes:** one entry per unique name. A shared node is created once.
2. **Add edges:** for each `node: [parents]` and each parent:
   `upstream[node].append(parent)` and `downstream[parent].append(node)`. Skip duplicate edges.
3. **Validate:** every parent must exist as a node.
4. **Kahn's algorithm:**
   ```
   in_degree = {n: len(upstream[n])}
   ready     = nodes with in_degree 0
   while ready:
       n = ready.pop(); order.append(n)
       for c in downstream[n]:
           in_degree[c] -= 1
           if in_degree[c] == 0: ready.append(c)
   if len(order) < number of nodes: CYCLE
   ```
5. **Cycle error:** start at a leftover node and keep stepping to a leftover parent until you repeat a node.
   Print that loop: `join -> report -> clean_orders -> join`.

## Rules of thumb

- An arrow points in the direction the data flows: from the producer to the consumer.
- Nodes that are in `ready` at the same time can run in parallel.
- Something broke? Its **descendants** are affected. A wrong result? Look at its **ancestors**.
- To add a step, add one line to the config. Don't touch the code.
- Check for cycles before you run anything.
