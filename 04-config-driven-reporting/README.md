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

- **Edges come from the config.** A task runs after the tables listed under its `ingestion:` and `transformation:` keys.
- **Shared tables run once.** Each table is one task, however many reports use it.
- **Most-shared first.** Each task's `priority_weight` is the number of reports that need it, directly or through other tables. When several tasks are ready, the one more reports wait on runs first.

## The Files

```
config.yaml        what each transformation and report reads
ingestion/         one script per raw table: copy it from the shop database into raw.<name>
transformation/    one script per shared table: build mart.<name>
reports/           one script per report: build reports.<name>
graph.py           read the config, build the graph, check it, order it, count usage
main.py            the Airflow DAG: one BigQuery task per script, arrows from graph.py
tests/             graph tests (no Airflow needed)
```

## The Config

```yaml
dag_id: finance_reports
schedule: "0 6 * * *"            # every day at 06:00, for the day that just ended
start_date: 2026-11-01
gcp_conn_id: google_cloud_default
location: US
params:
  source_connection: my-gcp-project.us.shop_db

ingestion: [orders, order_items, invoices, refunds, shipping]

transformation:
  order_revenue:
    ingestion: [orders, order_items]
  net_revenue:
    ingestion: [refunds]
    transformation: [order_revenue]

reports:
  revenue_report:
    transformation: [net_revenue]
  refunds_report:
    ingestion: [refunds]
    transformation: [order_revenue]
  shipping_cost_report:
    ingestion: [shipping]
    transformation: [order_revenue]
  invoice_reconciliation_report:
    ingestion: [invoices]
    transformation: [net_revenue]
```

Each name has a script at `<layer>/<name>.sql`. A transformation or report lists only the tables its own script reads. Tables needed through a transformation come with it: `revenue_report` waits for `orders` because `net_revenue` needs `order_revenue`, which needs `orders`.

**Adding a report** means writing `reports/<name>.sql` and adding a few lines under `reports:`. Nothing else changes.

## The Flow

When Airflow loads `main.py`:

1. `load_config()` reads `config.yaml`.
2. `build_graph()` turns it into `node -> nodes it needs`, with nodes named `<layer>.<name>`. It refuses to load the DAG, listing every mistake, if a table is unknown, a script is missing, a table uses nothing, or there is a cycle.
3. `reports_using()` counts, for each node, how many reports need it.
4. `build_order()` sorts the graph topologically. Among nodes ready at the same time, the most-used comes first.
5. `main.py` creates one `BigQueryInsertJobOperator` per node, with that count as its `priority_weight`, and adds an arrow for every dependency.

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
