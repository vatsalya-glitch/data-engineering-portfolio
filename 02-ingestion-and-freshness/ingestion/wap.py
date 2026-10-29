"""
WRITE - AUDIT - PUBLISH: the one pattern every ingestion type goes through.

  WRITE    run ingestion/sql/<table>.sql into a staging table (readers never see it)
  AUDIT    two simple checks on staging: rows exist, no duplicate keys
  PUBLISH  copy staging into the real table AND write the SUCCESS signal,
           in one transaction: either both happen or neither does

If the audit fails, nothing is published and the signal says FAILED.
"""

import uuid
from pathlib import Path

from common.config import TABLES
from common.control_log import ensure_log_table, write_log

SQL_DIR = Path("ingestion/sql")


def write(con, table, params):
    sql = (SQL_DIR / f"{table}.sql").read_text()
    used = {name: value for name, value in params.items() if f"${name}" in sql}
    con.execute(sql, used)


def audit(con, table):
    """Return a list of problems (empty list = audit passed)."""
    staging, key = f"stg_{table}", TABLES[table]["key"]
    problems = []

    rows = con.execute(f"SELECT count(*) FROM {staging}").fetchone()[0]
    if rows == 0:
        problems.append("staging is empty")

    dupes = con.execute(
        f"SELECT count(*) FROM (SELECT {key} FROM {staging} GROUP BY {key} HAVING count(*) > 1)"
    ).fetchone()[0]
    if dupes:
        problems.append(f"{dupes} duplicate {key} value(s)")

    return rows, problems


def write_audit_publish(con, table, run_date, params, publish,
                        watermark=None, watermark_column=None):
    """
    Run one load. `publish(con, pub_table)` moves the audited rows into the real table.
    The watermark is either given (CDC: the change file's end time) or read from
    staging (incremental: the newest watermark_column value).
    """
    ensure_log_table(con)
    batch_id = f"{table}-{uuid.uuid4().hex[:8]}"

    write(con, table, params)
    rows, problems = audit(con, table)

    if rows == 0 and watermark_column:
        # Incremental load with nothing newer than the watermark: normal, not a failure
        print(f"    ✓ {table}: no new rows after the watermark, nothing to do")
        return "NO_NEW_DATA"

    if problems:
        write_log(con, table, run_date, "FAILED", rows, batch_id)
        print(f"    ✗ audit failed for {table}: {', '.join(problems)} -> NOT published")
        return "FAILED"

    if watermark_column:
        watermark = con.execute(f"SELECT max({watermark_column}) FROM stg_{table}").fetchone()[0]

    # Audit columns on every published row: when, which batch, from where
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE pub_{table} AS
            SELECT *, now() AS _loaded_at, ? AS _batch_id, ? AS _source FROM stg_{table}""",
        [batch_id, params["source_file"]],
    )

    con.begin()
    try:
        publish(con, f"pub_{table}")
        write_log(con, table, run_date, "SUCCESS", rows, batch_id, watermark)
        con.commit()
    except Exception:
        con.rollback()
        raise
    print(f"    ✓ {table}: {rows} rows published (batch {batch_id})")
    return "SUCCESS"
