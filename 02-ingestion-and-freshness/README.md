# Ingestion and Freshness

**A beginner project about the two ends of one handshake:**
1. **Ingestion** loads data from a source, and when it's done, **announces** it.
2. **Freshness checks** read that announcement before a DAG uses a table. If the table is stale, the check runs the ingestion itself.

```
 external sources ──► INGESTION ──► raw tables ──────────────┬──► sales_report_dag
 (CSV, vendor files,  write → audit → publish                └──► finance_dag
  change logs)              │                                        ▲
                            └──► _freshness_log ──► FRESHNESS CHECK ─┘
                                 ("raw_orders is ready")  (ensure_fresh)
```

## The Problem

`raw_orders` is used by two DAGs that don't know about each other. In immature pipelines:
- A DAG runs before the table is loaded and **silently uses yesterday's data**
- A load fails halfway and a DAG reads a **partial table**
- A vendor file is read **while it's still being written**
- A rerun **duplicates rows**

## Part 1: Ingestion, four types

| Type | Table | How it loads | Typical real use |
|------|-------|--------------|------------------|
| **Snapshot** | `raw_customers` | Reads the whole source, swaps the table | Small reference tables |
| **Incremental** | `raw_orders` | Reads rows with `updated_at` newer than the last **watermark**, then `MERGE` | Big, growing tables |
| **File drop** | `raw_payments` | Waits for the vendor's `_SUCCESS` marker, then replaces that day | Files from partners or other teams |
| **CDC** | `raw_order_status` | Applies the next change-log file: latest change per key wins, deletes are applied | Mirroring a production database |

### CDC, with sample change files
There's no live database here. Two sample files stand in for one hour of change log, arriving in 30-minute batches:

```
data/source/cdc/changes_2026-10-20_10-00_to_10-30.csv
data/source/cdc/changes_2026-10-20_10-30_to_11-00.csv

op,order_id,status,changed_at
INSERT,1001,created,2026-10-20 10:02:00
UPDATE,1001,paid,2026-10-20 10:15:00
UPDATE,1001,shipped,2026-10-20 10:41:00   ← latest wins: 1001 ends as "shipped"
DELETE,1002,,2026-10-20 10:50:00          ← 1002 is removed
```

After each file, the watermark says **"complete up to 10:30"**, then **"up to 11:00"**. Rerunning skips files that are already applied.

### One pattern for all four: Write → Audit → Publish

```
WRITE    ingestion/sql/<table>.sql loads into a staging table (readers never see it)
AUDIT    rows exist? no duplicate keys?
PUBLISH  move into the real table AND write SUCCESS to _freshness_log, in one transaction
```
If the audit fails, nothing is published and the log says `FAILED`. Every published row also carries three **audit columns**: `_loaded_at`, `_batch_id`, `_source`.

## Part 2: Freshness, three checks

Before using a table, each DAG calls `ensure_fresh(table, run_date)`:

```
1. CHECK     is the table fresh for run_date?   ── yes ──► continue
2. INGEST    no → run the table's ingestion
3. RE-CHECK  still not fresh? → FAIL loudly (the source is not ready)
```

Each table picks a check in `config/tables.yaml`:

| Check | Question it asks | Strength | Weakness |
|-------|------------------|----------|----------|
| `recency` | Was the table loaded in the last N hours? | Cheap, works on any table | "Recently touched" ≠ "has *my* data" |
| `partition` | Are there rows for `run_date`? | Date-aware, safe for reruns | Can't tell a finished load from a half-done one |
| `control_table` | Did ingestion log `SUCCESS` for `run_date`? | Explicit "data is ready" signal | Every ingestion must write the log |

## The Rules That Make It Safe

1. **Every load is safe to rerun**: swap, replace-the-day, or `MERGE`. Never a plain `INSERT`.
2. **Never read a file before its `_SUCCESS` marker exists.**
3. **Audit before publishing**, and publish the data and its signal together.
4. **Re-check after ingesting.** If the source has no data, fail instead of building reports on nothing.

## Run It

```bash
cd 02-ingestion-and-freshness
pip install -r requirements.txt
python run_demo.py
```

| # | Situation | What happens |
|---|-----------|--------------|
| 1 | Sales DAG on an empty warehouse | Its inputs are stale, so they're ingested (snapshot + incremental) |
| 2 | Finance DAG next | `raw_orders` is already fresh; payments are loaded from the file drop |
| 3 | Sales DAG rerun | Everything is fresh, nothing is re-ingested |
| 4 | Finance DAG, next day | Vendor file has no `_SUCCESS` yet, so the DAG fails |
| 5 | CDC, two change files | Latest change wins, deletes apply, watermark moves 10:30 → 11:00 |
| 6 | CDC rerun | No new files after the watermark, nothing changes |
| 7 | Vendor file with duplicate rows | Audit rejects it, nothing is published |

## Structure

```
02-ingestion-and-freshness/
├── config/tables.yaml        # per table: ingestion type + freshness check
├── data/source/              # the "external sources": CSV, vendor drops, CDC change logs
├── ingestion/
│   ├── sql/                  # WRITE step: one SQL per table
│   ├── wap.py                # write → audit → publish
│   └── ingest.py             # the four ingestion types
├── common/control_log.py     # _freshness_log: written by ingestion, read by freshness
├── freshness/ensure_fresh.py # check → ingest → re-check
├── dags/                     # two DAGs sharing raw_orders
└── run_demo.py
```

## In Real Life: BigQuery + Airflow

| Local | Real life |
|-------|-----------|
| DuckDB | BigQuery |
| `customers.csv`, `orders.csv` | Source databases, read via federated queries (Cloud SQL) or extract jobs |
| `vendor_drops/` + `_SUCCESS` | A GCS bucket; vendors upload the marker last; a GCS sensor or Pub/Sub notification triggers the load |
| `cdc/changes_*.csv` | Datastream, Debezium or Fivetran writing change events continuously |
| Staging temp table + swap | A `_staging` table, then `MERGE`, or `WRITE_TRUNCATE` to one partition (`raw_payments$20261020`) |
| `dags/*.py` functions | Airflow DAGs (Cloud Composer) |
| `run_date` | Airflow's `{{ ds }}`, passed as a `@run_date` query parameter |
| `_freshness_log` | One shared BigQuery audit table for all pipelines |

**The freshness checks in BigQuery** read metadata, so they scan no data and are free:
```sql
-- recency
SELECT TIMESTAMP_MILLIS(last_modified_time) FROM `project.dataset.__TABLES__`
WHERE table_id = 'raw_orders';

-- partition
SELECT total_rows FROM `project.dataset.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'raw_orders' AND partition_id = FORMAT_DATE('%Y%m%d', @run_date);
```

**A sketch of the Airflow version:**
```python
@task
def ensure_fresh(table: str, ds=None):
    if not is_fresh(table, ds):
        ingest(table, ds)                      # write → audit → publish
        if not is_fresh(table, ds):
            raise AirflowFailException(f"{table} still stale for {ds}")

with DAG("finance_dag", schedule="@daily", ...):
    checks = [ensure_fresh.override(task_id=f"fresh_{t}", pool=f"ingest_{t}")(t)
              for t in ["raw_orders", "raw_payments"]]
    checks >> build_finance_reconciliation()
```

**What real life adds that this demo leaves out:**
- **Two DAGs ingesting at the same moment.** Safe-to-rerun loads keep the data correct. A **1-slot Airflow pool per table** (as above) stops the duplicate work.
- **Late data.** Rows that arrive after the watermark has moved past them. Real incremental loads re-read a small overlap window (for example, the last 2 hours).
- **Waiting instead of failing.** Use task `retries` or a sensor with a timeout while a vendor file is late.
- **Deeper audits.** Today's row count compared with the usual (±30%), null checks on important columns.
- **A separate monitor.** A scheduled job that checks every table against its deadline, to catch a pipeline that **never started** (inline checks can't see that).
- **Ownership.** Set `auto_ingest: false` for tables a DAG doesn't own, so the check only fails.
- **Tools you'll hear about:** Airflow Datasets/Assets, dbt `source freshness`, Great Expectations / Soda for audits, Monte Carlo / Elementary for monitoring.

## License

MIT
