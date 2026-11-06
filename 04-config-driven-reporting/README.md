# Config-Driven Finance Reports on Airflow

> 🧭 **This is a situation-based project.** Start any explanation from the situation below.

## The Situation

A finance team needs four daily reports: revenue, refunds, shipping cost and invoice reconciliation. They all come from the same five tables in the shop database: `orders`, `order_items`, `invoices`, `refunds` and `shipping`.

Every report needs three kinds of work:

1. **Ingestion:** copy the raw tables from the database.
2. **Transformation:** build shared tables, such as revenue per order.
3. **Report:** build the final report table.

Many reports share the same raw and shared tables. Each should be built **once**, **before** the reports that need it, and the work shared by **most** reports should go **first**.

**The question this project answers:** *How do you describe what each report needs in one config, and let a graph work out the DAG?*

## The Idea

Each report lists the tables it reads. That turns the config into a graph, and a topological sort of the graph gives the order of the DAG's tasks.

```
 layer            table                           built from
 ──────────────   ─────────────────────────────   ─────────────────────────────
 ingestion        orders, order_items, invoices,  the shop database
                  refunds, shipping
 transformation   order_revenue                   orders + order_items
                  net_revenue                     order_revenue + refunds
 reports          revenue_report                  net_revenue
                  refunds_report                  order_revenue + refunds
                  shipping_cost_report            order_revenue + shipping
                  invoice_reconciliation_report   net_revenue + invoices
```

- **Edges come from the config.** A task runs after the tables listed under it in the config: a report after its transformations and ingestion tables, a transformation after the tables nested under it.
- **Shared tables run once.** Each table is one task, however many reports use it.
- **Most-shared first.** Each task's `priority_weight` is the number of reports that need it, directly or through other tables. When several tasks are ready, the one more reports wait on runs first.

## The Files

```
config.yaml        the reports and their steps: what each transformation and report reads
ingestion/         one script per raw table: copy it from the shop database into raw.<name>
transformation/    one script per shared table: build mart.<name>
reports/           one script per report: build reports.<name>
graph.py           read the config, build the graph, check it, order it, count usage
main.py            the Airflow DAG: its settings, one BigQuery task per script, arrows from graph.py
tests/             graph tests (no Airflow needed)
```

## The Config

`config.yaml` has one key, `reports:`. Each report is described from the top down: the report, then the transformations it reads, then inside each transformation the ingestion tables it is built from.

```yaml
reports:

  revenue_report:
    transformation:
      net_revenue:
        ingestion:
          - refunds
        transformation:
          order_revenue:
            ingestion:
              - orders
              - order_items

  refunds_report:
    ingestion:
      - refunds
    transformation:
      order_revenue:
        ingestion:
          - orders
          - order_items

  shipping_cost_report:
    ingestion:
      - shipping
    transformation:
      order_revenue:
        ingestion:
          - orders
          - order_items

  invoice_reconciliation_report:
    ingestion:
      - invoices
    transformation:
      net_revenue:
        ingestion:
          - refunds
        transformation:
          order_revenue:
            ingestion:
              - orders
              - order_items
```

How to read it:

- **`ingestion:`** lists the raw tables read directly at that level: by the report, or by the transformation it sits under.
- **`transformation:`** lists the shared tables read at that level. Under each one, its own `ingestion:` and `transformation:` say what it is built from. A transformation built from another transformation nests it, as `net_revenue` does with `order_revenue`.
- **Each name has a script** at `<layer>/<name>.sql`.
- **A shared table is written under every report that needs it, but runs once.** `order_revenue` appears under all four reports and is one task. `graph.py` checks that it is described the same way everywhere, and stops with an error naming both places if not.

`config.yaml` holds only the reports and their steps. The DAG's own settings are constants at the top of `main.py`:

```python
DAG_ID = "finance_reports"
SCHEDULE = "0 6 * * *"                       # every day at 06:00, for the day that just ended
START_DATE = pendulum.datetime(2026, 11, 1, tz="UTC")
GCP_CONN_ID = "google_cloud_default"         # Airflow connection to Google Cloud
LOCATION = "US"                              # BigQuery location of the datasets
PARAMS = {"source_connection": "my-gcp-project.us.shop_db"}   # used in SQL as {{ params.<name> }}
```

**Adding a report** means writing `reports/<name>.sql` and adding its block under `reports:`, with the transformations and ingestion tables it needs. Nothing else changes.

## The Flow

When Airflow loads `main.py`:

```
config.yaml ──► load_config() ──► build_graph() ──► reports_using() ──► build_order() ──► Airflow tasks + arrows
                 graph.py           graph.py          graph.py            graph.py           main.py
```

1. `load_config()` reads `config.yaml`.
2. `build_graph()` walks each report from the top down and turns the config into `node -> nodes it needs`, with nodes named `<layer>.<name>`. It refuses to load the DAG, listing every mistake, if a script is missing, a table uses nothing, or a shared table is described differently in two places.
3. `reports_using()` counts, for each node, how many reports need it.
4. `build_order()` sorts the graph topologically. Among nodes ready at the same time, the most-used comes first.
5. `main.py` creates one `BigQueryInsertJobOperator` per node, with that count as its `priority_weight`, and adds an arrow for every dependency.

When the DAG runs, at 06:00 every day:

1. The five ingestion tasks start; nothing comes before them. `orders` and `order_items` start first, because all four reports need them.
2. `order_revenue` starts as soon as `orders` and `order_items` are done; `net_revenue` once `order_revenue` and `refunds` are.
3. Each report starts as soon as the tables it needs are done. It doesn't wait for the other reports.
4. If a task fails, only the tasks that need it stop. For example, a failed `shipping` copy stops `shipping_cost_report` only.

## How the Code Works

### `graph.py`

**`build_graph(config)`** builds a dictionary from each node to the nodes it needs:

```python
graph["ingestion.orders"]             = set()                          # needs nothing
graph["transformation.order_revenue"] = {"ingestion.orders", "ingestion.order_items"}
graph["transformation.net_revenue"]   = {"ingestion.refunds", "transformation.order_revenue"}
graph["reports.revenue_report"]       = {"transformation.net_revenue"}
```

It walks each report from the top down, with one small function, `read_needs()`, used at every level:

1. Each name under `ingestion:` becomes a node that needs nothing.
2. Each name under `transformation:` becomes a node. `read_needs()` is called again on what is nested under it, so it needs the tables found there, however deep.
3. The report becomes a node that needs what is listed directly under it.
4. When a table is met again under another report, it must need the same tables as the first time. Otherwise that is an error naming both places.
5. Every node must have its script at `<layer>/<name>.sql`.

No separate cycle check is needed. Nesting can only say "`a` needs `b`, `b` needs `a`" by describing a table two different ways, and step 4 reports that.

All mistakes are collected and raised together, so they can be fixed in one go.

**`reports_using(graph)`** starts from each report and walks back through everything it needs (a depth-first search). Every node it reaches gets +1. That is how `orders` counts for `revenue_report`, even though the report only lists `net_revenue`:

```
revenue_report ─► net_revenue ─► order_revenue ─► orders
```

**`build_order(graph)`** uses `TopologicalSorter` step by step:

```python
while sorter.is_active():
    ready = sorted(sorter.get_ready(), key=lambda node: (-counts[node], node))  # most-used first
    order.extend(ready)
    sorter.done(*ready)   # this makes the nodes that need them ready next
```

`get_ready()` returns the nodes whose needs are all done. Sorting them by usage count puts the shared work first.

### `main.py`

`main.py` is the only file that uses Airflow. It:

1. Sets the DAG's constants (name, schedule, start date, connection, location, params) and calls the three functions above.
2. Creates one `BigQueryInsertJobOperator` per node, in build order. The task id is the node name, the query is the node's SQL script, and the `priority_weight` is its usage count.
3. Adds an arrow `tasks[need] >> tasks[node]` for every dependency in the graph.

### The SQL scripts

Each script creates its own table with `CREATE OR REPLACE TABLE`, so rerunning a day gives the same result.

- **`ingestion/`** copies a table from the shop database into `raw.<name>` with `EXTERNAL_QUERY`.
- **`transformation/`** builds a shared table in `mart.<name>` from `raw` and `mart` tables.
- **`reports/`** builds `reports.<name>` for the completed day, filtering on `{{ data_interval_start | ds }}`.

See the order without Airflow:

```bash
python graph.py
```

```
 1. ingestion.order_items                         used by 4 report(s)   needs: -
 2. ingestion.orders                              used by 4 report(s)   needs: -
 3. ingestion.refunds                             used by 3 report(s)   needs: -
 4. ingestion.invoices                            used by 1 report(s)   needs: -
 5. ingestion.shipping                            used by 1 report(s)   needs: -
 6. transformation.order_revenue                  used by 4 report(s)   needs: ingestion.order_items, ingestion.orders
 7. transformation.net_revenue                    used by 2 report(s)   needs: ingestion.refunds, transformation.order_revenue
 8. reports.refunds_report                        used by 1 report(s)   needs: ingestion.refunds, transformation.order_revenue
 9. reports.shipping_cost_report                  used by 1 report(s)   needs: ingestion.shipping, transformation.order_revenue
10. reports.invoice_reconciliation_report         used by 1 report(s)   needs: ingestion.invoices, transformation.net_revenue
11. reports.revenue_report                        used by 1 report(s)   needs: transformation.net_revenue
```

## Notes

- **Ingestion from the database** uses BigQuery's `EXTERNAL_QUERY` through a BigQuery connection to the database (for example Cloud SQL). Each script does a full copy with `CREATE OR REPLACE`, so rerunning is safe.
- **The completed day.** The schedule is a `CronDataIntervalTimetable`, so the run at 06:00 on 6 Nov has `{{ data_interval_start | ds }}` = `2026-11-05`, and the report scripts filter on that date. A manual run gets the last completed day too.
- **Airflow parses `main.py`** only because it contains the words "airflow" and "dag". Keep the comment at its top.
- **Shared first, in Airflow terms.** The arrows decide what *can* run, and `priority_weight` decides what runs first when worker slots are scarce. With enough slots, independent tasks simply run in parallel.

## Run

```bash
pip install -r requirements.txt \
  -c https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.13.txt
pytest tests
```

To deploy, copy this folder into Airflow's `dags/` folder. Airflow picks up `main.py`, and `main.py` imports `graph.py` from the same folder.
