"""
INGESTION + FRESHNESS DEMO

Part 1 - the chain: two DAGs share raw_orders and check freshness before using it
  1. Sales DAG runs first      -> its stale inputs are ingested (snapshot + incremental)
  2. Finance DAG runs next     -> raw_orders already fresh, payments loaded from a file drop
  3. Sales DAG is rerun        -> everything fresh, nothing re-ingested
  4. Finance DAG, next day     -> vendor file has no _SUCCESS marker yet, the DAG fails loudly

Part 2 - ingestion types up close
  5. CDC, two Debezium files   -> latest change wins, deletes apply, watermark moves forward
  6. CDC rerun                 -> both files are in the load history, nothing changes
  7. Bad vendor file           -> audit finds duplicate keys, nothing is published

Usage:
  python run_demo.py
"""

import os
from pathlib import Path

os.chdir(Path(__file__).parent)

import duckdb

from dags import finance_dag, sales_report_dag
from freshness.ensure_fresh import StaleDataError
from ingestion.ingest import ingest

DB_FILE = "warehouse.duckdb"


def step(title):
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def show_order_status(con):
    rows = con.execute("SELECT order_id, status FROM raw_order_status ORDER BY order_id").fetchall()
    print("    raw_order_status now: " + ", ".join(f"{oid}={status}" for oid, status in rows))


def show_log(con):
    print("\n  _freshness_log (the signal ingestion writes and the freshness check reads):")
    rows = con.execute("""
        SELECT table_name, run_date, status, row_count, watermark
        FROM _freshness_log ORDER BY completed_at
    """).fetchall()
    for table, run_date, status, row_count, watermark in rows:
        mark = f"complete up to {watermark}" if watermark else ""
        print(f"    {table:<17} {run_date}  {status:<8} {row_count} rows  {mark}")


def main():
    Path(DB_FILE).unlink(missing_ok=True)  # fresh warehouse every demo run
    con = duckdb.connect(DB_FILE)

    print("\nPART 1 - THE CHAIN: ingestion announces, consumers check")

    step("1. Sales DAG, run_date=2026-10-20 (warehouse is empty)")
    sales_report_dag.run(con, "2026-10-20")

    step("2. Finance DAG, run_date=2026-10-20 (shares raw_orders)")
    finance_dag.run(con, "2026-10-20")

    step("3. Sales DAG rerun, run_date=2026-10-20")
    sales_report_dag.run(con, "2026-10-20")

    step("4. Finance DAG, run_date=2026-10-21 (vendor still writing the file)")
    try:
        finance_dag.run(con, "2026-10-21")
    except StaleDataError as e:
        print(f"    ✗ DAG FAILED: {e}")
        print("    -> finance_reconciliation was NOT rebuilt with incomplete data")

    print("\n\nPART 2 - INGESTION TYPES UP CLOSE")

    step("5. CDC: apply the Debezium change files, one file per run")
    for _ in range(2):
        ingest(con, "raw_order_status", "2026-10-20")
        show_order_status(con)
    print("    -> 1001 went created→paid→shipped, 1002 was deleted, 1004 is new")

    step("6. CDC rerun: is it safe to run again?")
    ingest(con, "raw_order_status", "2026-10-20")
    show_order_status(con)

    step("7. Vendor file for 2026-10-22 has a _SUCCESS marker but duplicate rows")
    ingest(con, "raw_payments", "2026-10-22")
    published = con.execute(
        "SELECT count(*) FROM raw_payments WHERE payment_date = '2026-10-22'"
    ).fetchone()[0]
    print(f"    raw_payments rows for 2026-10-22: {published} -> bad data never reached consumers")

    show_log(con)
    con.close()


if __name__ == "__main__":
    main()
