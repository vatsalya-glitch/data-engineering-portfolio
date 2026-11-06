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

## The Answer: One Framework, Many Configs

Engineers describe **what** each table or report needs. The framework works out the order, validates the config, and builds the Airflow tasks.

```
configs/daily_operations.yaml ─┐                   ┌─► DAG daily_operations
sql/daily_operations/*.sql ────┼─► report_framework ┤
configs/monthly_finance.yaml ──┤   (validate, sort, └─► DAG monthly_finance
sql/monthly_finance/*.sql ─────┘    build tasks)
```

One config = one pipeline = one DAG, with one `schedule` and one `period`.

```bash
pip install -r requirements.txt \
  -c https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.13.txt
pytest tests                    # without Airflow installed, the DAG tests are skipped
```

## A Pipeline Config

```yaml
dag_id: daily_operations
schedule: "0 6 * * *"        # when the run starts
period: day                  # day | month: the completed period the run covers
project: my-gcp-project
export_bucket: my-reports-bucket
alert_email: [data-team@example.com]   # gets an email when any task fails

sources:
  orders:                    # loaded by THIS pipeline: ingest -> checks
    ingest:
      uri: "gs://my-landing-bucket/orders/{{ period_start }}/*.parquet"
      mode: period           # replace only this period's partition (safe to rerun)
      partition_field: order_date
    checks:
      - "SELECT COUNT(*) = 0 FROM raw.orders WHERE order_id IS NULL"
  shipments:                 # loaded ELSEWHERE: wait_fresh -> checks
    freshness: "SELECT MAX(event_time) >= TIMESTAMP('{{ period_end }}') FROM raw.shipments"

models:
  order_shipments:
    type: intermediate       # mart.order_shipments, used by other models
    uses: [orders, shipments]
  delivery_report:
    type: report             # reports.delivery_report, final
    uses: [order_shipments, customers]
    steps: [approval, export, email]
    email_to: [logistics@example.com]
```

Each model has one SQL file at `sql/<dag_id>/<model>.sql` that writes its own table (`CREATE OR REPLACE TABLE reports.delivery_report AS ...`). Sources live in `raw`, intermediates in `mart`, reports in `reports`.

The DAG file is one line plus a comment. Keep the comment: Airflow only parses files that contain the words "airflow" and "dag".

```python
# airflow DAG: built from configs/daily_operations.yaml by the report framework.
from report_framework import build_dag

dag = build_dag("configs/daily_operations.yaml")
```

### Framework defaults

Everything that isn't specific to one pipeline is in YAML too: [`dags/report_framework/defaults.yaml`](./dags/report_framework/defaults.yaml) holds the dataset names, connection ids, retries, start date, freshness timeout and approval timeout. Change a value there and it reaches every pipeline that doesn't set it. A pipeline overrides a default by setting the same key at the same level: top level, under a source, or under a report.

```yaml
datasets: {source: raw, intermediate: mart, report: reports}
pipeline: {timezone: UTC, start_date: 2026-01-01, gcp_conn_id: google_cloud_default,
           smtp_conn_id: smtp_default, retries: 1, retry_delay_minutes: 5, max_active_runs: 1}
source:   {freshness_timeout_hours: 4, poke_interval_minutes: 10}
report:   {approval_timeout_hours: 8}
```

The defaults file is validated like a pipeline config, listing every problem at once. It is shared, so a mistake in it stops every pipeline from loading, the same as a bug in the framework code.

### Config reference

| Key | Where | Required | Meaning |
|---|---|---|---|
| `dag_id`, `schedule`, `period`, `project`, `alert_email` | pipeline | yes | `period` is `day` or `month` |
| `timezone`, `start_date`, `gcp_conn_id`, `smtp_conn_id`, `retries`, `retry_delay_minutes`, `max_active_runs` | pipeline | no | defaults in `defaults.yaml` |
| `location`, `description` | pipeline | no | |
| `export_bucket` | pipeline | if any report uses `export` | |
| `ingest.uri`, `ingest.mode` | source | one of `ingest` / `freshness` | `uri` starts with `gs://`; `mode` is `replace` or `period` |
| `ingest.format`, `ingest.partition_field` | source | no | format is guessed from the URI extension |
| `freshness` | source | one of `ingest` / `freshness` | SQL that returns true once the data covers the period |
| `freshness_timeout_hours`, `poke_interval_minutes` | source | no | defaults in `defaults.yaml` (4 hours, 10 minutes) |
| `type`, `uses` | model | yes | `type` is `intermediate` or `report` |
| `checks` | source, model | no | SQL that must return true |
| `steps`, `email_to` | report | no | `email_to` is required by the `email` step |
| `approval_timeout_hours` | report | no | default in `defaults.yaml` (8 hours) |

Template variables available in every SQL file, check, freshness query and URI:

- `{{ period_start }}`: first date of the completed period (`2026-11-05`, or `2026-10-01` for a month)
- `{{ period_end }}`: first date after it (`2026-11-06`, or `2026-11-01`)

## What the DAG Looks Like

Each source, intermediate and report becomes a task group named after it:

```
orders     [ingest] ──► [check_1, check_2] ──┐
shipments  [wait_fresh] ──► [check_1] ───────┼─► order_shipments [build] ──► [check_1] ──┐
customers  [ingest] ──► [check_1] ───────────┼──────────────────────────────────────────┼─► delivery_report [build] ─► approval ─► export_clear ─► export ─► email
                                             └─► (other models that use these tables)   ┘
```

## How Each Situation Is Handled

| Situation | How the framework handles it | Where |
|---|---|---|
| **UC1** Reports share tables | One task group per table, however many reports use it. Each report waits only for what it uses. `priority_weight` = number of reports that depend on the task, with `weight_rule="absolute"`. | `graph.report_usage_counts`, `builder.DagBuilder.weight` |
| **UC2** Order from config | Edges come only from `uses`. `graphlib.TopologicalSorter` gives the build order and finds cycles. | `graph.topological_order` |
| **UC3** Intermediates | `type` is required. Intermediates can't have steps; reports can't be used. A cycle shows the loop: `cycle in uses: a -> b -> a`. | `config._model_rules`, `graph.GraphCycleError` |
| **UC4** Drift check | sqlglot (BigQuery dialect) reads each model's SQL, with `{{ ... }}` hidden first. The tables it reads must equal `uses`. Written tables and CTEs don't count. | `sql_parse.tables_read`, `config._drift` |
| **UC5** Source loaded here | `ingest → checks`. A BigQuery load job with `WRITE_TRUNCATE`: the whole table (`replace`), or one partition through a decorator such as `orders$20261105` (`period`). Reruns give the same rows. | `builder.ingest_task` |
| **UC6** Source loaded elsewhere | `wait_fresh → checks`. A SQL sensor in `mode="reschedule"`, so it doesn't hold a worker slot. It fails after `freshness_timeout_hours` (default 4). Only what depends on the source waits. | `builder.BigQueryFreshnessSensor` |
| **UC7** Quality checks | One `BigQueryCheckOperator` per check, run in parallel after the load. Downstream waits for all of them. A failure only stops dependents. | `builder.add_checks` |
| **UC8** Steps | Run in the order listed. `approval` uses Airflow 3.1's `ApprovalOperator`: a reject, or no answer before the timeout (default `Reject`), skips the remaining steps without failing the run. `export` writes CSV to `gs://<bucket>/<report>/<period_start>/`. `email` says where the export is. | `steps.py` |
| **UC9** Completed period | `CronDataIntervalTimetable`. The period is the last full day or month before the run's data interval ends, so the 6 Nov run covers 5 Nov and the 1 Nov monthly run covers October. | `builder.make_period_macro`, `config.period_bounds` |
| **UC10** Isolation | Each DAG file builds one config. A broken config raises inside that file, so only that DAG shows an import error. | `tests/test_dag_load.py::test_broken_config_only_breaks_its_own_dag` |
| **UC11** Alerts | Every task gets an `on_failure_callback` that emails `alert_email` with the DAG, task, period, error and log link. | `builder.failure_alert` |
| **UC12** Validation | Every problem is collected and raised at once as a `ConfigError`. Unknown keys (such as `use:` for `uses:`) and duplicate YAML keys are reported too. | `config.py` |

### Adding a step type

Write one function and register it. Existing pipelines don't change, and config validation picks up the new name and keys automatically:

```python
@register("notify_dashboard", required_report_keys=("dashboard_id",))
def notify_dashboard(ctx: StepContext) -> list[BaseOperator]:
    return [SomeOperator(task_id="notify_dashboard", ..., **ctx.operator_args())]
```

## Project Structure

```
dags/
  report_framework/
    defaults.yaml   # framework defaults shared by every pipeline
    config.py       # load YAML, attach SQL, validate everything (no Airflow import)
    graph.py        # topological order, cycles, usage counts (no Airflow import)
    sql_parse.py    # tables a SQL query reads (no Airflow import)
    steps.py        # one function per step type + registry (Airflow imported lazily)
    builder.py      # config -> Airflow DAG
  configs/          # one YAML per pipeline
  sql/<dag_id>/     # one SQL file per model
  daily_operations_dag.py
  monthly_finance_dag.py
  .airflowignore    # keeps Airflow from parsing the framework package as DAG files
tests/
  fixtures/         # a small valid pipeline (YAML + SQL) that tests break one rule at a time
  test_config.py    # every validation rule, periods
  test_defaults.py  # defaults.yaml: values, overrides, validation
  test_graph.py     # order, cycle messages, usage counts
  test_sql_parse.py # drift-check parsing: CTEs, CREATE ... AS, Jinja
  test_dag_load.py  # DagBag load, task dependencies, periods, alerts, isolation
```

## Design Notes

- **SQL lives in `sql/<dag_id>/`, not one flat `sql/` folder.** The rule "a SQL file with no model is an error" only works per pipeline. With one shared folder, every pipeline would see the others' files as orphans, or would need to read the other configs, which breaks isolation.
- **How the period variables get into templates.** Airflow has no per-run custom variables, so the framework puts one Jinja line in front of every templated string: `{% set period_start, period_end, period_partition = report_period(data_interval_end, dag_run) %}`. `report_period` is a `user_defined_macros` function.
- **Standard SQL everywhere.** Query jobs set `useLegacySql: False`, checks set `use_legacy_sql=False`, and the freshness sensor builds its own `BigQueryHook(use_legacy_sql=False)`, because the usual `google_cloud_platform` connection doesn't give `SqlSensor` a SQL hook.
- **Exports are rerun-safe.** `export_clear` deletes the period's export folder before `export` writes it again.
- **Out of scope for now:** backfills (`catchup=False`), different schedules within one pipeline, alerts other than email, and ingestion from anything other than GCS files.
