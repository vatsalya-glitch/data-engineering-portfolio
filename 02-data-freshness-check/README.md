# Data Freshness Check

**A beginner project about one small but important pipeline step: before you use a table, check that its data is actually fresh. If it isn't, ingest it yourself.**

## The Problem

`raw_orders` is used by two DAGs. Neither DAG knows about the other.

```
  external source ──► raw_orders ──┬──► sales_report_dag
                                   └──► finance_dag
```

Things that go wrong in immature pipelines:
- A DAG runs before the table is loaded and **silently uses yesterday's data**
- The load fails halfway and a DAG reads a **partial table**
- Someone **reruns** a DAG and the ingestion **duplicates rows**

## The Fix: `ensure_fresh(table, run_date)`

Every DAG calls this step for each table it needs, before its own logic:

```
1. CHECK     is the table fresh for run_date?   ── yes ──► continue
2. INGEST    no → run ingestion/<table>.sql for run_date
3. RE-CHECK  still not fresh? → FAIL loudly (the source is not ready)
4. LOG       write SUCCESS / FAILED to _freshness_log (control table)
```

**Convention:** table name = file name. `raw_orders` is loaded by `ingestion/raw_orders.sql`.

## Three Ways to Check Freshness

Each table picks one in `config/freshness_rules.yaml`:

| Check | Question it asks | Strength | Weakness |
|-------|------------------|----------|----------|
| `recency` | Was the table loaded in the last N hours? | Cheap, works on any table | "Recently touched" ≠ "has *my* data" |
| `partition` | Are there rows for `run_date`? | Date-aware, safe for reruns and backfills | Can't tell a finished load from a half-done one |
| `control_table` | Did ingestion log `SUCCESS` for `run_date`? | Explicit "data is ready" signal | Every ingestion must write the log |

## Two Rules That Make It Safe

1. **Ingestion SQL must be idempotent.** Delete the day, then insert it (or replace the whole table). Running it twice gives the same result. This matters because two DAGs, or a rerun, can trigger the same ingestion.
2. **Re-check after ingesting.** If the source has no data yet, re-ingesting won't fix it. Fail instead of building reports on empty data.

## Run It

```bash
cd 02-data-freshness-check
pip install -r requirements.txt
python run_demo.py
```

The demo plays out five situations:

| # | Situation | What happens |
|---|-----------|--------------|
| 1 | Sales DAG runs on an empty warehouse | Its inputs are stale, so they get ingested |
| 2 | Finance DAG runs next | `raw_orders` is already fresh, so only `raw_payments` is ingested |
| 3 | Sales DAG is rerun | Everything is fresh, nothing is re-ingested |
| 4 | Ingestion is forced twice | Row count stays the same (idempotent) |
| 5 | Finance DAG for a day with no payments in the source | Ingestion runs, re-check fails, the DAG stops |

## Structure

```
02-data-freshness-check/
├── config/freshness_rules.yaml   # per table: check type + auto_ingest
├── ingestion/                    # one idempotent SQL per table
├── common/ensure_fresh.py        # the reusable step
├── dags/                         # two DAGs sharing raw_orders
├── data/source/                  # CSVs playing the "external database"
└── run_demo.py
```

## In Real Life: BigQuery + Airflow

The local pieces map one-to-one:

| Local | Real life |
|-------|-----------|
| DuckDB | BigQuery |
| `data/source/*.csv` | External tables, federated queries (Cloud SQL), GCS files, or Datastream / Fivetran landing tables |
| `dags/*.py` functions | Airflow DAGs (Cloud Composer) |
| `ensure_fresh()` | A Python task or `@task.branch` per input table, built by a small factory |
| `$run_date` | `@run_date` query parameter, filled from Airflow's `{{ ds }}` |
| `_freshness_log` | A small BigQuery audit table shared by all pipelines |

**The checks in BigQuery:**

```sql
-- recency: table metadata, scans no data (free)
SELECT TIMESTAMP_MILLIS(last_modified_time) AS last_modified
FROM `project.dataset.__TABLES__` WHERE table_id = 'raw_orders';

-- partition: partition metadata, also free
SELECT total_rows FROM `project.dataset.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'raw_orders' AND partition_id = FORMAT_DATE('%Y%m%d', @run_date);
```

**Idempotent ingestion in BigQuery:** use `MERGE`, or `DELETE` + `INSERT` inside one multi-statement transaction, or write with `WRITE_TRUNCATE` to the `raw_orders$20261020` partition.

**A sketch of the Airflow version:**

```python
@task
def ensure_fresh(table: str, ds=None):
    if not is_fresh(table, ds):                      # one of the 3 checks
        run_sql(f"ingestion/{table}.sql", run_date=ds)
        if not is_fresh(table, ds):
            raise AirflowFailException(f"{table} still stale for {ds}")

with DAG("finance_dag", schedule="@daily", ...):
    checks = [ensure_fresh.override(task_id=f"fresh_{t}", pool=f"ingest_{t}")(t)
              for t in ["raw_orders", "raw_payments"]]
    checks >> build_finance_reconciliation()
```

**Things real life adds that this demo doesn't:**
- **Two DAGs ingesting at the same moment.** Locally the DAGs run one after the other. In Airflow they can run in parallel and both decide the table is stale. Idempotent SQL keeps the data correct. A **1-slot Airflow pool per table** (as in the sketch), or a `RUNNING` row in the control table, stops the duplicate work.
- **Late source data.** Instead of failing at once, retry the task (`retries`, `retry_delay`) or use a sensor with a timeout.
- **Ownership.** Some teams don't want a downstream DAG loading tables it doesn't own. Set `auto_ingest: false` for those tables, so the step only checks and fails.
- **Alternatives you'll hear about:** `ExternalTaskSensor` (ties DAG schedules together and breaks easily), Airflow **Datasets / Assets** (data-aware scheduling), `BigQueryTablePartitionExistenceSensor`, and dbt `source freshness`. They are all variations of the same idea: never assume, check.

## License

MIT
