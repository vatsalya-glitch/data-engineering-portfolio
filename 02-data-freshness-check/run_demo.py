"""
DATA FRESHNESS DEMO

Plays out the situations from the README with two DAGs that share raw_orders:
  1. Sales DAG runs first      -> its stale inputs get ingested
  2. Finance DAG runs next     -> raw_orders is already fresh, only payments ingested
  3. Sales DAG is rerun        -> everything fresh, nothing re-ingested
  4. Ingestion forced twice    -> same row count (idempotent SQL)
  5. Finance DAG, next day     -> payments source has no data yet, the DAG fails loudly

Usage:
  python run_demo.py
"""

import os
from pathlib import Path

os.chdir(Path(__file__).parent)

import duckdb

from common.ensure_fresh import StaleDataError, run_ingestion
from dags import finance_dag, sales_report_dag

DB_FILE = "warehouse.duckdb"


def step(title):
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def show_log(con):
    print("\n  _freshness_log (control table):")
    rows = con.execute(
        "SELECT table_name, run_date, status, row_count FROM _freshness_log ORDER BY completed_at"
    ).fetchall()
    for table, run_date, status, row_count in rows:
        print(f"    {table:<14} {run_date}  {status:<8} {row_count} rows")


def main():
    Path(DB_FILE).unlink(missing_ok=True)  # fresh warehouse every demo run
    con = duckdb.connect(DB_FILE)

    step("1. Sales DAG, run_date=2026-10-20 (warehouse is empty)")
    sales_report_dag.run(con, "2026-10-20")

    step("2. Finance DAG, run_date=2026-10-20 (shares raw_orders)")
    finance_dag.run(con, "2026-10-20")

    step("3. Sales DAG rerun, run_date=2026-10-20")
    sales_report_dag.run(con, "2026-10-20")

    step("4. Force raw_orders ingestion twice: does it duplicate rows?")
    first = run_ingestion(con, "raw_orders", "2026-10-20")
    second = run_ingestion(con, "raw_orders", "2026-10-20")
    print(f"    rows after 1st run: {first}, after 2nd run: {second} "
          f"-> {'idempotent ✓' if first == second else 'DUPLICATED ✗'}")

    step("5. Finance DAG, run_date=2026-10-21 (payments not in source yet)")
    try:
        finance_dag.run(con, "2026-10-21")
    except StaleDataError as e:
        print(f"    ✗ DAG FAILED: {e}")
        print("    -> finance_reconciliation was NOT rebuilt with incomplete data")

    show_log(con)
    con.close()


if __name__ == "__main__":
    main()
