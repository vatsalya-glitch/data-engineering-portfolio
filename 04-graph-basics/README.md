# 04 - Graph Basics for Data Pipelines

A from-zero guide to the graph ideas behind every pipeline scheduler.
You need plain Python and nothing else.

```
04-graph-basics/
├── README.md        # This tutorial (10 concepts)
├── exercises/       # ex01 ... ex05, each runs on its own
├── SOLUTIONS.md     # Answers to the "try it yourself" tasks
└── CHEATSHEET.md    # One page to keep next to you
```

Run any exercise:

```bash
cd 04-graph-basics/exercises
python ex01_build_graph.py
```

All the examples use this one small pipeline:

```
extract_orders --> clean_orders --+
                                  +--> join --+--> report
extract_users  --> clean_users  --+           +--> dashboard
```

Each concept below has a **definition**, a **tiny example** and a **why it matters** line.

---

## 1. Node

**Definition:** A node is one step of work in the pipeline: one task.
It could load a file, clean a table or send a report.

```
[ clean_orders ]
```

**Example:** `extract_orders`, `clean_orders` and `report` are three nodes.
In Python a node is usually just a string name.

**Why it matters:** The node is the unit you run, retry and monitor.
If it fails, this is the thing you rerun.

---

## 2. Edge

**Definition:** An edge links two nodes. It means **"A must run before B"**.
An edge has a direction. It points from the step that runs first to the step that runs after it.

```
[ extract_orders ] ---> [ clean_orders ]
      A runs first          B runs after A
```

**Example:** `extract_orders --> clean_orders`: you can't clean orders you haven't loaded yet.
Turning the arrow around changes the meaning. `clean_orders --> extract_orders` would mean
"clean before you extract", which makes no sense.

**Why it matters:** Edges are the rules of your pipeline.
A missing edge lets a task run too early and read data that isn't ready yet.

---

## 3. Upstream and downstream

**Definition:** Upstream means "before me" (against the arrows).
Downstream means "after me" (with the arrows).

- **Direct** upstream (parents) are one edge away. Direct downstream (children) are also one edge away.
- **Indirect**: also counts nodes several edges away.
  All upstream nodes are the **ancestors**. All downstream nodes are the **descendants**.

```
          ancestors of join                   descendants of join
   +-----------------------------+          +---------------------+
   extract_orders -> clean_orders -+         +-> report
                                   +-> join -+
   extract_users  -> clean_users  -+         +-> dashboard
                     \___________/
                   direct upstream (parents)
```

**Example:** For `join`, the direct upstream is `clean_orders` and `clean_users`.
Its ancestors are those two plus `extract_orders` and `extract_users`.
Its descendants are `report` and `dashboard`.

**Why it matters:**
- If `extract_orders` breaks, every descendant gets bad or missing data. That tells you what to rerun.
- If `report` looks wrong, the cause is one of its ancestors. That tells you where to look.

---

## 4. Roots and leaves

**Definition:**
- A **root** has no upstream. Nothing runs before it.
- A **leaf** has no downstream. Nothing waits for it.

```
 ROOTS                                         LEAVES
 extract_orders -> clean_orders -+          +-> report
                                 +-> join --+
 extract_users  -> clean_users  -+          +-> dashboard
```

**Example:** The roots are `extract_orders` and `extract_users`. The leaves are `report` and `dashboard`.

**Why it matters:** Roots are where a run starts. They usually read from outside sources.
Leaves are the final products people actually use. A leaf that nobody uses is often dead code.

---

## 5. DAG: Directed Acyclic Graph

**Definition:**
- **Graph** = nodes + edges.
- **Directed** = every edge has an arrow (A before B, not just "A and B are related").
- **Acyclic** = no cycles. You can never follow the arrows and come back to where you started.

```
 OK (a DAG)                     NOT OK (a cycle)

 A --> B --> C                  A --> B --> C
                                ^           |
                                +-----------+
```

**Why a cycle makes running impossible:** In the right-hand picture:
- A waits for C,
- C waits for B,
- B waits for A.

Every node in the loop waits for another node in the loop, so **no one can go first**. The pipeline can't start.
It isn't a slow pipeline. It's an impossible one.

**Example:** In our pipeline, suppose `clean_orders` also depends on `report`.
Then you get `clean_orders -> join -> report -> clean_orders`. That's a cycle.

**Why it matters:** Every scheduler needs a DAG.
Checking for cycles before you run anything turns a hung pipeline into a clear error message.

---

## 6. Storing a graph in code

There are two common shapes. They both hold the same information.

**Adjacency list:** a dict of lists. Each node maps to its neighbours.

```python
downstream = {                       # node -> who runs AFTER it
    "extract_orders": ["clean_orders"],
    "extract_users":  ["clean_users"],
    "clean_orders":   ["join"],
    "clean_users":    ["join"],
    "join":           ["report", "dashboard"],
    "report":         [],
    "dashboard":      [],
}
```

You can keep it in either direction, or both:
- `upstream[node]` answers "what does this node need?"
- `downstream[node]` answers "who needs this node?"

The ex01 code builds both from one config.

**Edge list:** a flat list of `(from, to)` pairs.

```python
edges = [
    ("extract_orders", "clean_orders"),
    ("extract_users",  "clean_users"),
    ("clean_orders",   "join"),
    ("clean_users",    "join"),
    ("join",           "report"),
    ("join",           "dashboard"),
]
```

```
 adjacency list                     edge list
 join: [report, dashboard]   <==>   (join, report), (join, dashboard)
```

**Why it matters:**
- The adjacency list makes "who are my neighbours?" instant. Algorithms need exactly that.
- The edge list is easy to write, save, count and print, for example as Mermaid text.

---

## 7. Run order: topological sort (Kahn's algorithm)

**Definition:** A **topological order** is a list of all nodes where every node comes after all of its upstream nodes.
If you run the tasks in that order, nothing runs too early.
A graph often has more than one valid order.

**Kahn's algorithm** in one sentence: keep running any node whose upstream is all done.

1. For every node, count how many parents it is still waiting for. This count is its **in-degree**.
2. Put every node with in-degree 0 into a `ready` queue. At the start these are the roots.
3. Repeat until `ready` is empty:
   - Take a node from `ready` and append it to `order`.
   - For each child of that node, subtract 1 from the child's in-degree.
   - If a child's in-degree reaches 0, add it to `ready`.
4. If `order` has every node, you're done. If not, there is a cycle (see concept 8).

**By hand, on our pipeline:**

```
extract_orders --> clean_orders --+
                                  +--> join --+--> report
extract_users  --> clean_users  --+           +--> dashboard
```

Starting in-degrees: `extract_orders 0, extract_users 0, clean_orders 1, clean_users 1, join 2, report 1, dashboard 1`

| Step | Take from ready  | In-degree changes                  | ready afterwards              | order so far |
|------|------------------|------------------------------------|-------------------------------|--------------|
| 0    | -                | -                                  | extract_orders, extract_users | -            |
| 1    | extract_orders   | clean_orders 1→**0**               | extract_users, clean_orders   | eo           |
| 2    | extract_users    | clean_users 1→**0**                | clean_orders, clean_users     | eo, eu       |
| 3    | clean_orders     | join 2→1 (still waiting)           | clean_users                   | …, co        |
| 4    | clean_users      | join 1→**0**                       | join                          | …, cu        |
| 5    | join             | report 1→**0**, dashboard 1→**0**  | report, dashboard             | …, join      |
| 6    | report           | -                                  | dashboard                     | …, report    |
| 7    | dashboard        | -                                  | (empty)                       | all 7 ✔      |

At step 3, `join` is still waiting for `clean_users`, so it isn't ready yet. This step shows the whole idea in small.

**Why it matters:** This is exactly how a scheduler decides what to start next.
Every node that is in `ready` at the same moment could run **in parallel**.

---

## 8. Cycle detection

**Definition:** Kahn's algorithm finds cycles without extra effort.
A node on a cycle always waits for another node on the same cycle, so its in-degree never reaches 0.
It never enters `ready`. When the loop ends, some nodes are left over.

```
extract_orders --> clean_orders --> join --> report     dashboard
                       ^                       |           ^
                       +-----------------------+           |
                                                    (also stuck: its parent
                                                     join never runs)
```

- `order` = `[extract_orders, extract_users, clean_users]`: 3 of 7 nodes.
- The leftover nodes are `clean_orders, join, report, dashboard`. That means there is a cycle.

**Naming the actual cycle:** The leftover set can also hold nodes that are only *blocked*.
`dashboard` is blocked because it comes after the cycle, but it isn't part of it. To find the real loop:
1. Start at a leftover node.
2. Step to one of its parents that is also leftover. There is always at least one.
3. Keep stepping until you reach a node you have already visited.
4. The part of the path between the two visits is the cycle.

Error you want to show:

```
Cycle found: join -> report -> clean_orders -> join
```

**Why it matters:** "Pipeline stuck, no idea why" turns into
"these three dependencies form a loop, remove one". You catch it before anything runs.

---

## 9. Shared nodes

**Definition:** Sometimes two or more flows need the same step, for example `clean_orders`.
That step exists **only once** in the graph. It just gets **more than one downstream edge**.

```
 WRONG: two copies                    RIGHT: one node, two edges

 extract_orders -> clean_orders -> daily_sales        +--> daily_sales
 extract_orders -> clean_orders -> revenue    clean_orders
                                                      +--> revenue
```

**Example:** The sales and finance flows both list `clean_orders`.
The graph keeps one `clean_orders` with two edges: one to `daily_sales` and one to `revenue`.

**Why it matters:** One node runs once, so you get no wasted compute.
You also get no two copies of the "same" table that drift apart.
And if that node fails, you can see clearly that both flows are affected.

---

## 10. Building the graph from a config

**Definition:** You don't write the edges by hand in code. A config says, for each node, what it depends on.
A small builder reads the config and creates the nodes and edges.

```yaml
# pipeline.yaml
extract_orders: []
clean_orders:   [extract_orders]
report:         [clean_orders]
```

```
 config (data)  --->  builder (code, never changes)  --->  graph  --->  run order
```

The builder does three things:
1. Create every node once. A dict key can't appear twice, so shared nodes are handled for free.
2. For each `node: [parents]`, add an edge `parent --> node` for each parent.
   Skip the edge if it already exists.
3. Check the result: no unknown parents and no cycles.

**Example:** Say you want a new `email_report` step after `report`. Add one line:

```yaml
email_report: [report]
```

Rerun, and the graph and the run order now include it. You changed **no Python code**.

**Why it matters:** Pipelines grow every week.
With config-driven graphs, adding a step is a one-line review, not a code change.

---

## Hands-on

| File | What you do |
|------|-------------|
| `exercises/ex01_build_graph.py` | Build upstream/downstream dicts from a hand-written dict and print them |
| `exercises/ex02_roots_leaves_family.py` | Find roots, leaves, ancestors and descendants |
| `exercises/ex03_topological_sort.py` | Kahn's algorithm by hand, with a step-by-step trace |
| `exercises/ex04_cycle_detection.py` | Add a cycle and get a clear error that names it |
| `exercises/ex05_config_to_mermaid.py` | 3 flows with shared nodes become one graph, printed as Mermaid |

Each file ends with two **try it yourself** tasks. The answers are in [SOLUTIONS.md](./SOLUTIONS.md).
When you're done, keep [CHEATSHEET.md](./CHEATSHEET.md) open.
