# Ingestion and Freshness

> 🧭 **This is a situation-based project.**
> It doesn't start from a concept and invent an example for it. It starts from a **situation that really happens in data teams**, and every concept here is something that situation forces you to learn.
>
> **When explaining this project to someone, always start with the situation below.**

## The Situation

You work on a small data team at an online shop. Every day, data lands in the warehouse in four different ways:
- **Orders** are copied from the shop's database, only the rows that changed.
- **Customers** are copied in full, because the table is small.
- **Payments** arrive as files that a payment vendor uploads to a shared folder.
- **Order status changes** (created → paid → shipped) stream in from the database every 30 minutes.

Two pipelines (DAGs), owned by different people, use this data:
- `sales_report_dag` reads orders + customers to build the daily sales report.
- `finance_dag` reads orders + payments to check that every order was paid.

Nobody guarantees the data is loaded before these pipelines run. So, sooner or later, things go wrong. Each one has two sides: what people **see** at the shop, and the data engineering **problem** behind it:

| What happens at the shop (the situation) | The problem behind it |
|------------------------------------------|-----------------------|
| Finance runs **before payments arrive**, and paid orders show up as unpaid | A pipeline **silently uses stale data**, because nothing told it the table wasn't loaded yet |
| A vendor file is read **while it's still being written**, so half the payments are missing | A file is read **before it's complete** |
| A rerun **loads the same orders twice**, and revenue is double-counted | The load **isn't safe to rerun**, so it duplicates rows |
| A load **fails halfway**, and a report is built on a partial table | A pipeline reads a **partial table** |

**The question this project answers:** *How does a pipeline know that the data it needs is really ready?*

## What This Situation Teaches

| The situation raises… | So you learn… | Where |
|-----------------------|---------------|-------|
| Data arrives in four different ways | Four ingestion types: snapshot, incremental, file drop, CDC | [Part 1](#part-1-ingestion-four-types) |
| A load can fail halfway or bring bad data | Write → Audit → Publish | [Part 1](#one-pattern-for-all-four-write--audit--publish) |
| Reruns must not double-count | Safe-to-rerun loads, watermarks, load history | [The Rules](#the-rules-that-make-it-safe) |
| Pipelines need to know the data is ready | Announce, don't trigger: the control log | [The Expert Rule](#the-expert-rule-announce-dont-trigger) |
| A pipeline must check before using a table | Three freshness checks | [Part 2](#part-2-freshness-three-checks) |
| How this looks at a real job | The BigQuery + Airflow mapping | [In Real Life](#in-real-life-bigquery--airflow) |

## How This Project Answers It

Two ends of one handshake:
1. **Ingestion** loads data from a source, and when it's done, **announces** it.
2. **Freshness checks** read that announcement before a DAG uses a table. If the table is stale, the check runs the ingestion itself.

```
 external sources ──► INGESTION ──► raw tables ──────────────┬──► sales_report_dag
 (CSV, vendor files,  write → audit → publish                └──► finance_dag
  change logs)              │                                        ▲
                            └──► _freshness_log ──► FRESHNESS CHECK ─┘
                                 ("raw_orders is ready")  (ensure_fresh)
```

## The Expert Rule: Announce, Don't Trigger

> **A producer should announce, not trigger.** Ingestion says *"raw_orders is loaded, complete up to T"*. Each consumer checks for itself and decides whether it has everything it needs.

That's the whole design of this project: ingestion writes the announcement to `_freshness_log`, and every DAG checks it before running.

**Why not have the ingestion DAG simply trigger the next DAG?** It's the obvious option, and it's fine for one source feeding one or two consumers. It breaks once ingestion is split by source. `finance_dag` needs orders (database) **and** payments (vendor file), loaded by two separate pipelines:

```
05:10  orders ingestion finishes   → triggers finance_dag (run #1): new orders, OLD payments → wrong report
05:40  payments ingestion finishes → triggers finance_dag (run #2): now correct
```

| Push: producer triggers consumers | Pull: producer announces, consumer checks |
|-----------------------------------|-------------------------------------------|
| A consumer with two inputs runs **twice**, the first time with half-fresh data | Runs **once**, when all its inputs are ready |
| CDC batches every 5 min → the consumer runs **288 times a day** | The consumer asks one question: "complete up to midnight?" |
| The producer must list every consumer, so every new pipeline means editing it | The producer never changes; new consumers check for themselves |
| If the producer never runs, consumers **silently** never start | The consumer fails **loudly**: "raw_orders is stale" |

**Other options you'll meet:**
- **Airflow Datasets / Assets** (`schedule=[raw_orders, raw_payments]`) run a DAG once, when *all* its inputs have been updated. That's the same rule, built into the scheduler.
- Teams that use them still keep a freshness check inside the DAG as a gate, because "the task succeeded" doesn't guarantee "the data is right".

## Part 1: Ingestion, four types

| Type | Table | How it loads | Typical real use |
|------|-------|--------------|------------------|
| **Snapshot** | `raw_customers` | Reads the whole source, swaps the table | Small reference tables |
| **Incremental** | `raw_orders` | Reads rows with `updated_at` newer than the last **watermark**, then `MERGE` | Big, growing tables |
| **File drop** | `raw_payments` | Waits for the vendor's `_SUCCESS` marker, then replaces that day | Files from partners or other teams |
| **CDC** | `raw_order_status` | Applies the next Debezium change file: latest change per key wins, deletes are applied | Mirroring a production database |

### CDC, with sample Debezium change files
There's no live database here. Two sample files stand in for one hour of change events, in the shape **Debezium** writes them once its `ExtractNewRecordState` transform flattens each event into a row:

```
data/source/cdc/order_status/
└── dt=2026-10-20/hour=10/                 ← Hive-style date/hour folders
    ├── 20261020T100000Z_000001.csv        ← UTC timestamp (ISO 8601) + sequence number
    └── 20261020T103000Z_000002.csv

order_id,status,__op,__source_ts_ms,__lsn
1001,created,c,1792490520000,24001101
1001,paid,u,1792491300000,24001104
1001,shipped,u,1792492860000,24001106     ← latest __lsn wins: 1001 ends as "shipped"
1002,paid,d,1792493400000,24001108        ← d = delete: 1002 is removed
```

| Field | Meaning |
|-------|---------|
| `__op` | `c` create, `u` update, `d` delete (`r` = initial snapshot read) |
| `__source_ts_ms` | When the change was committed in the source database (epoch millis, UTC) |
| `__lsn` | Position in the database's change log: the true order of changes |

Two things make it safe to rerun, as in real CDC pipelines:
- **Load history:** every applied file is recorded in `_freshness_log`, so a file is never applied twice.
- **Watermark from the events, not the file name:** after each file the table is "complete up to" its newest `__source_ts_ms` (10:24, then 10:58).

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
| 5 | CDC, two Debezium files | Latest change wins, deletes apply, watermark moves 10:24 → 10:58 |
| 6 | CDC rerun | Both files are in the load history, nothing changes |
| 7 | Vendor file with duplicate rows | Audit rejects it, nothing is published |

## Structure

```
02-ingestion-and-freshness/
├── config/tables.yaml        # per table: ingestion type + freshness check
├── data/source/              # the "external sources": CSV, vendor drops, Debezium change files
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
| `cdc/order_status/dt=…/hour=…/` | Debezium → Kafka → a GCS sink connector writing these folders (often JSON or Avro, not CSV); Datastream is the managed Google option |
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
