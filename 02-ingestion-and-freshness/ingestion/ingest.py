"""
INGEST - the four ingestion types.

Each type only decides two things, then hands over to write_audit_publish():
  - which parameters the WRITE SQL gets (source file, watermark)
  - how audited rows are PUBLISHED (swap, replace one day, or MERGE)

Entry point: ingest(con, table, run_date)
"""

from datetime import datetime
from pathlib import Path

from common.config import TABLES
from common.control_log import ensure_log_table, last_watermark, loaded_files
from ingestion.wap import write_audit_publish

# --- Publish strategies ---------------------------------------------------

def publish_swap(table):
    """Replace the whole table in one statement: readers see old OR new, never half."""
    def publish(con, pub):
        con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {pub}")
    return publish


def publish_replace_day(table, date_column, run_date):
    """Delete one day, insert it again: a rerun never duplicates rows."""
    def publish(con, pub):
        con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM {pub} LIMIT 0")
        con.execute(f"DELETE FROM {table} WHERE {date_column} = CAST(? AS DATE)", [run_date])
        con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM {pub}")
    return publish


def publish_merge(table, key, cdc=False):
    """Update rows that exist, insert new ones. For CDC, also apply deletes (__op = 'd')."""
    def publish(con, pub):
        drop_op = "EXCLUDE (__op)" if cdc else ""
        con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * {drop_op} FROM {pub} LIMIT 0")
        cols = [c for c, *_ in con.execute(f"DESCRIBE {table}").fetchall()]
        set_cols = ", ".join(f"{c} = s.{c}" for c in cols if c != key)
        insert_cols = ", ".join(cols)
        insert_vals = ", ".join(f"s.{c}" for c in cols)
        delete_clause = "WHEN MATCHED AND s.__op = 'd' THEN DELETE" if cdc else ""
        not_deleted = "AND s.__op <> 'd'" if cdc else ""
        con.execute(f"""
            MERGE INTO {table} t USING {pub} s ON t.{key} = s.{key}
            {delete_clause}
            WHEN MATCHED THEN UPDATE SET {set_cols}
            WHEN NOT MATCHED {not_deleted} THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)
    return publish


# --- The four ingestion types ---------------------------------------------

def ingest_snapshot(con, table, run_date):
    rule = TABLES[table]
    return write_audit_publish(
        con, table, run_date,
        params={"source_file": rule["source"]},
        publish=publish_swap(table),
    )


def ingest_incremental(con, table, run_date):
    rule = TABLES[table]
    watermark = last_watermark(con, table) or datetime(1900, 1, 1)
    print(f"    … {table}: reading rows with {rule['watermark_column']} > {watermark}")
    return write_audit_publish(
        con, table, run_date,
        params={"source_file": rule["source"], "watermark": watermark},
        publish=publish_merge(table, rule["key"]),
        watermark_column=rule["watermark_column"],
    )


def ingest_file_drop(con, table, run_date):
    rule = TABLES[table]
    folder = Path(rule["source"].format(run_date=run_date))
    if not (folder / "_SUCCESS").exists():
        print(f"    ⏸ {table}: no _SUCCESS marker in {folder}, vendor still writing -> not loading")
        return "NOT_READY"
    return write_audit_publish(
        con, table, run_date,
        params={"source_file": str(folder / "*.csv")},
        publish=publish_replace_day(table, rule["date_column"], run_date),
    )


def ingest_cdc(con, table, run_date):
    """
    Each run applies the NEXT change file not yet in the load history (one batch per run).
    Files live in dt=YYYY-MM-DD/hour=HH/ folders and are named <UTC timestamp>_<sequence>.csv,
    so sorting the paths as text gives the order they were written in.
    """
    rule = TABLES[table]
    done = loaded_files(con, table)
    pending = [f for f in sorted(Path(rule["source"]).rglob("*.csv")) if str(f) not in done]
    if not pending:
        print(f"    ✓ {table}: every change file is already applied, nothing to do")
        return "NO_NEW_DATA"

    next_file = pending[0]
    print(f"    … {table}: applying {next_file.relative_to(rule['source'])}")
    return write_audit_publish(
        con, table, run_date,
        params={"source_file": str(next_file)},
        publish=publish_merge(table, rule["key"], cdc=True),
        watermark_column="__source_ts",
    )


INGESTION_TYPES = {
    "snapshot": ingest_snapshot,
    "incremental": ingest_incremental,
    "file_drop": ingest_file_drop,
    "cdc": ingest_cdc,
}


def ingest(con, table, run_date):
    ensure_log_table(con)
    return INGESTION_TYPES[TABLES[table]["ingestion"]](con, table, run_date)
