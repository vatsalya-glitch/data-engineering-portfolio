# Config-Driven Reporting Pipelines on Airflow

> 🧭 **This is a situation-based project.** Start any explanation from the situation below.

## The Situation

A data team builds reports on BigQuery: a daily delivery report, a daily sales report, a monthly finance report, and more each quarter. Every report used to be its own hand-written Airflow DAG, and over time that went wrong in predictable ways:

| What happens on the team (the situation) | The problem behind it |
|------------------------------------------|-----------------------|
| `raw.orders` is loaded three times a morning, once by every DAG that needs it | Shared tables are **built once per report**, not once per run |
| Someone adds a join to a report's SQL but forgets the task arrow, and the report runs before its table is ready | The order lives in **hand-written arrows** that drift from the SQL |
| The 6 Nov run reports on 6 Nov, a day that has only just started | The DAG covers the **current, incomplete period** |
| A stream is late, and the whole report DAG fails at 06:00 | A table loaded **elsewhere, on another interval** isn't waited for properly |
| One bad null check fails every report, even the ones that never read that table | A failed check **stops everything**, not only what depends on it |
| A typo in one DAG file stops other teams' DAGs from loading | Pipelines **aren't isolated** |

**The question this project answers:** *How do you describe a reporting pipeline once, as data, and get a correct Airflow DAG every time?*

## The Answer: Describe the Reports, Get the DAG

A pipeline has three layers, and the config is written the same way:

```
 ingestion                transformation              reports                  steps
 raw tables       ──►     shared SQL tables   ──►     final SQL tables   ──►   approval, export, email
 raw.<name>               mart.<name>                 reports.<name>
 (load, or wait_for)
```

Each report says which ingestion and transformation tables it uses, and which SQL file builds it. Each transformation says the same about itself. From that, the framework:

1. **Builds each shared table once**, however many reports use it.
2. **Orders everything topologically**: a table runs only after every table it uses has been loaded or built and has passed its checks.
3. **Runs the most-shared tables first**: when several tasks are ready at the same time, the one more reports depend on gets the higher `priority_weight`.
4. **Validates the whole config before anything runs**, listing every problem at once.

One config = one pipeline = one DAG, with one `schedule` and one `period`.

```
configs/daily_operations.yaml ─────────┐                     ┌─► DAG daily_operations
sql/daily_operations/transformation/ ──┤                     │
sql/daily_operations/reports/ ─────────┼─► report_framework ─┤
configs/monthly_finance.yaml ──────────┤   validate, sort,   └─► DAG monthly_finance
sql/monthly_finance/... ───────────────┘   build tasks
```

```bash
pip install -r requirements.txt \
  -c https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.13.txt
pytest tests                    # without Airflow installed, the DAG tests are skipped
```

## A Pipeline Config

```yaml
pipeline:
  dag_id: daily_operations
  schedule: "0 6 * * *"          # when the run starts
  period: day                    # day | month: the completed period the run covers
  project: my-gcp-project
  export_bucket: my-reports-bucket
  alert_email: [data-team@example.com]   # gets an email when any task fails

# 1. Raw tables: loaded by this DAG (load), or by something else (wait_for).
ingestion:
  orders:
    load:
      uri: "gs://my-landing-bucket/orders/{{ period_start }}/*.parquet"
      mode: period               # replace only this period's partition (safe to rerun)
      partition_field: order_date
    checks:
      - "SELECT COUNT(*) = 0 FROM raw.orders WHERE order_id IS NULL"
  customers:
    load:
      uri: "gs://my-landing-bucket/customers/{{ period_start }}/*.parquet"
      mode: replace              # the file is a full copy
  shipments:
    wait_for:
      fresh_when: "SELECT MAX(event_time) >= TIMESTAMP('{{ period_end }}') FROM raw.shipments"
      timeout_hours: 4

# 2. Shared tables built with SQL.
transformation:
  order_shipments:
    sql: transformation/order_shipments.sql
    ingestion: [orders, shipments]
    checks:
      - "SELECT COUNT(*) = 0 FROM mart.order_shipments WHERE order_id IS NULL"

# 3. Reports: what they use, how they are built, what happens after.
reports:
  delivery_report:
    sql: reports/delivery_report.sql
    ingestion: [customers]
    transformation: [order_shipments]
    steps:
      - approval: {timeout_hours: 8}
      - export
      - email: {to: [logistics@example.com]}
```

`sql:` paths are relative to the pipeline's SQL folder, `sql/<dag_id>/`. Each SQL file writes its own table (`CREATE OR REPLACE TABLE reports.delivery_report AS ...`). The tables a SQL file reads must match the tables its config lists (the drift check).

A report lists only the tables its own SQL reads. Tables it needs through a transformation come with that transformation: `delivery_report` above waits for `orders` and `shipments` because `order_shipments` uses them.

The DAG file is one line plus a comment. Keep the comment: Airflow only parses files that contain the words "airflow" and "dag".

```python
# airflow DAG: built from configs/daily_operations.yaml by the report framework.
from report_framework import build_dag

dag = build_dag("configs/daily_operations.yaml")
```

### Framework defaults

Everything that isn't specific to one pipeline is in YAML too: [`dags/report_framework/defaults.yaml`](./dags/report_framework/defaults.yaml). Change a value there and it reaches every pipeline that doesn't set it. A pipeline overrides a default by setting the same key in the same place.

```yaml
datasets: {ingestion: raw, transformation: mart, reports: reports}
pipeline: {timezone: UTC, start_date: 2026-01-01, gcp_conn_id: google_cloud_default,
           smtp_conn_id: smtp_default, retries: 1, retry_delay_minutes: 5, max_active_runs: 1}
wait_for: {timeout_hours: 4, poke_interval_minutes: 10}
steps:
  approval: {timeout_hours: 8}
```

The defaults file is validated like a pipeline config, listing every problem at once. It is shared, so a mistake in it stops every pipeline from loading, the same as a bug in the framework code.

### Config reference

| Key | Where | Required | Meaning |
|---|---|---|---|
| `dag_id`, `schedule`, `period`, `project`, `alert_email` | `pipeline` | yes | `period` is `day` or `month` |
| `timezone`, `start_date`, `gcp_conn_id`, `smtp_conn_id`, `retries`, `retry_delay_minutes`, `max_active_runs` | `pipeline` | no | defaults in `defaults.yaml` |
| `location`, `description`, `export_bucket` | `pipeline` | no | `export_bucket` is the default bucket of the `export` step |
| `load.uri`, `load.mode` | ingestion table | one of `load` / `wait_for` | `uri` starts with `gs://`; `mode` is `replace` or `period` |
| `load.format`, `load.partition_field` | ingestion table | no | format is guessed from the URI extension |
| `wait_for.fresh_when` | ingestion table | one of `load` / `wait_for` | SQL that returns true once the data covers the period |
| `wait_for.timeout_hours`, `wait_for.poke_interval_minutes` | ingestion table | no | defaults in `defaults.yaml` (4 hours, 10 minutes) |
| `sql` | transformation, report | yes | path of the SQL file that builds the table |
| `ingestion`, `transformation` | transformation, report | at least one | the tables its SQL reads, per layer |
| `checks` | any table | no | SQL that must return true; run in parallel after the table is loaded or built |
| `steps` | report | no | run in order; each is a name, or `name: {settings}` |
| `description` | any table, `pipeline` | no | |

| Step | Settings | Does |
|---|---|---|
| `approval` | `timeout_hours` (default 8) | Waits for a person to approve in the Airflow UI. A rejection, or no answer in time, skips the remaining steps without failing the run. |
| `export` | `bucket` (default `pipeline.export_bucket`) | Writes the report as CSV to `gs://<bucket>/<report>/<period_start>/`, emptying that folder first. |
| `email` | `to` (required) | Emails that the report is ready, with the export location. |

Template variables available in every SQL file, check, freshness query and URI:

- `{{ period_start }}`: first date of the completed period (`2026-11-05`, or `2026-10-01` for a month)
- `{{ period_end }}`: first date after it (`2026-11-06`, or `2026-11-01`)

## What the DAG Looks Like

One task group per layer, and inside it one group per table:

```
ingestion                         transformation                          reports
├─ orders     load ─► check_1,2 ─┬─► order_shipments  build ─► check_1,2 ─┬─► delivery_report  build ─► approval ─► export_clear ─► export ─► email
├─ shipments  wait_for ─► check_1┘                                        │
└─ customers  load ─► check_1 ────────────────────────────────────────────┘
```

Task ids follow the layers: `ingestion.orders.load`, `transformation.order_shipments.build`, `reports.delivery_report.email`. The DAG's documentation page lists each report with every table it depends on, and the build order with how many reports use each table.

## How Each Situation Is Handled

| Situation | How the framework handles it | Where |
|---|---|---|
| Reports share tables | One task group per table, however many reports use it. Each report waits only for what it uses, directly or through transformations. | `builder.DagBuilder` |
| Shared tables should go first | `priority_weight` = number of reports that depend on the task, with `weight_rule="absolute"`. The build order breaks ties the same way. | `graph.report_usage_counts`, `graph.build_order` |
| Order comes from config | Edges come only from the `ingestion` / `transformation` lists. `graphlib.TopologicalSorter` orders them and finds cycles: `cycle in uses: a -> b -> a`. | `graph.topological_order` |
| Layers stay clean | A table must be listed under the layer it is defined in. Reports can't be used by other tables, only reports have steps, and a table no report uses is an error. | `config._references`, `config._unused` |
| Config and SQL must match | sqlglot (BigQuery dialect) reads each SQL file, with `{{ ... }}` hidden first. The tables it reads must equal what the config lists. Written tables and CTEs don't count. | `sql_parse.tables_read`, `config._drift` |
| Raw table loaded here | `load → checks`. A BigQuery load job with `WRITE_TRUNCATE`: the whole table (`replace`), or one partition through a decorator such as `orders$20261105` (`period`). Reruns give the same rows. | `builder.load_task` |
| Raw table loaded elsewhere | `wait_for → checks`. A SQL sensor in `mode="reschedule"`, so it doesn't hold a worker slot, failing after `timeout_hours`. Only what depends on it waits. | `builder.BigQueryFreshnessSensor` |
| Quality checks | One `BigQueryCheckOperator` per check, in parallel after the load or build. Downstream waits for all of them; a failure only stops dependents. | `builder.add_checks` |
| Steps after a report | Run in the order listed, each with its own settings. | `steps.py` |
| The completed period | `CronDataIntervalTimetable`. The period is the last full day or month before the run's data interval ends, so the 6 Nov run covers 5 Nov and the 1 Nov monthly run covers October. | `builder.make_period_macro`, `config.period_bounds` |
| Isolation | Each DAG file builds one config. A broken config raises inside that file, so only that DAG shows an import error. | `tests/test_dag_load.py::test_broken_config_only_breaks_its_own_dag` |
| Failure alerts | Every task gets an `on_failure_callback` that emails `alert_email` with the DAG, task, period, error and log link. | `builder.failure_alert` |
| Mistakes caught early | Every problem is collected and raised at once as a `ConfigError`, including unknown keys and duplicate YAML keys. | `config.py` |

### Adding a step type

Write one function and register it with its settings. Existing pipelines don't change, and config validation picks up the new name and settings automatically:

```python
@register("notify_dashboard", settings={"dashboard": TEXT})
def notify_dashboard(ctx: StepContext) -> list[BaseOperator]:
    return [SomeOperator(task_id="notify_dashboard", dashboard=ctx.settings["dashboard"],
                         **ctx.operator_args())]
```

```yaml
    steps:
      - export
      - notify_dashboard: {dashboard: ops}
```

## Project Structure

```
dags/
  report_framework/
    defaults.yaml   # framework defaults shared by every pipeline
    config.py       # load YAML, attach SQL, validate everything (no Airflow import)
    graph.py        # topological order, cycles, usage counts (no Airflow import)
    sql_parse.py    # tables a SQL query reads (no Airflow import)
    rules.py        # value checks shared by config and steps (no Airflow import)
    steps.py        # one function per step type + registry (Airflow imported lazily)
    builder.py      # config -> Airflow DAG
  configs/          # one YAML per pipeline
  sql/<dag_id>/
    transformation/ # one SQL file per transformation table
    reports/        # one SQL file per report
  daily_operations_dag.py
  monthly_finance_dag.py
  .airflowignore    # keeps Airflow from parsing the framework package as DAG files
tests/
  fixtures/         # a small valid pipeline (YAML + SQL) that tests break one rule at a time
  test_config.py    # every validation rule, periods
  test_defaults.py  # defaults.yaml: values, overrides, validation
  test_graph.py     # order, shared-first tie-breaking, cycle messages, usage counts
  test_sql_parse.py # drift-check parsing: CTEs, CREATE ... AS, Jinja
  test_dag_load.py  # DagBag load, task dependencies, periods, alerts, isolation
```

## Design Notes

- **What "shared first" means in Airflow.** Dependencies decide what *can* run; `priority_weight` decides what runs first when several tasks can. With enough worker slots, independent tables run in parallel anyway. When slots are scarce, the tables most reports wait on go first.
- **Reports list direct reads only.** Listing every table a report depends on, through every transformation, would repeat the transformations' config and drift as it changes. The framework works out the rest from the graph and shows it on the DAG's documentation page.
- **How the period variables get into templates.** Airflow has no per-run custom variables, so the framework puts one Jinja line in front of every templated string: `{% set period_start, period_end, period_partition = report_period(data_interval_end, dag_run) %}`. `report_period` is a `user_defined_macros` function.
- **Standard SQL everywhere.** Query jobs set `useLegacySql: False`, checks set `use_legacy_sql=False`, and the freshness sensor builds its own `BigQueryHook(use_legacy_sql=False)`, because the usual `google_cloud_platform` connection doesn't give `SqlSensor` a SQL hook.
- **Out of scope for now:** backfills (`catchup=False`), different schedules within one pipeline, alerts other than email, and ingestion from anything other than GCS files.
