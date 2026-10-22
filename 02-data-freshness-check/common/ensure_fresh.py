"""
ENSURE FRESH - the reusable "check, then ingest if needed" step.

Any DAG that needs a table calls ensure_fresh(con, table, run_date) first:

  1. CHECK    - is the table fresh for run_date? (strategy comes from the YAML)
  2. INGEST   - if not, run ingestion/<table>.sql for run_date
  3. RE-CHECK - still not fresh? fail loudly: the source itself is not ready
  4. LOG      - every ingestion writes a row to _freshness_log (the control table)

Paths are relative to the project root (run_demo.py changes into it).
"""

from datetime import date
from pathlib import Path

import yaml

RULES = yaml.safe_load(Path("config/freshness_rules.yaml").read_text())
INGESTION_DIR = Path("ingestion")


class StaleDataError(Exception):
    """Raised when a table cannot be made fresh."""


def _table_exists(con, table):
    return con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [table]
    ).fetchone()[0] > 0


def _ensure_control_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS _freshness_log (
            table_name VARCHAR, run_date DATE, status VARCHAR,
            row_count INTEGER, completed_at TIMESTAMP
        )
    """)


def _row_count(con, table, rule, run_date):
    """Rows for run_date (daily tables) or all rows (snapshot tables)."""
    if "date_column" in rule:
        sql = f"SELECT count(*) FROM {table} WHERE {rule['date_column']} = CAST(? AS DATE)"
        return con.execute(sql, [run_date]).fetchone()[0]
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


# --- The three check strategies -------------------------------------------

def check_recency(con, table, rule, run_date):
    """Scenario 1: was the table loaded recently? Ignores run_date entirely."""
    hours = con.execute(
        f"SELECT date_diff('hour', max(_loaded_at), now()) FROM {table}"
    ).fetchone()[0]
    return hours is not None and hours < rule["max_age_hours"]


def check_partition(con, table, rule, run_date):
    """Scenario 2: is there data for MY date?"""
    return _row_count(con, table, rule, run_date) >= rule["min_rows"]


def check_control_table(con, table, rule, run_date):
    """Scenario 3: did the producer record a SUCCESS for this date?"""
    return con.execute(
        """
        SELECT count(*) FROM _freshness_log
        WHERE table_name = ? AND run_date = CAST(? AS DATE) AND status = 'SUCCESS'
        """,
        [table, run_date],
    ).fetchone()[0] > 0


CHECKS = {
    "recency": check_recency,
    "partition": check_partition,
    "control_table": check_control_table,
}


# --- Ingestion ------------------------------------------------------------

def run_ingestion(con, table, run_date):
    """Run ingestion/<table>.sql for run_date and log the result."""
    rule = RULES[table]
    sql_file = INGESTION_DIR / f"{table}.sql"
    for statement in sql_file.read_text().split(";"):
        if not statement.strip():
            continue
        params = {"run_date": run_date} if "$run_date" in statement else None
        con.execute(statement, params)

    rows = _row_count(con, table, rule, run_date)
    status = "SUCCESS" if rows >= rule.get("min_rows", 1) else "FAILED"
    con.execute(
        "INSERT INTO _freshness_log VALUES (?, CAST(? AS DATE), ?, ?, now())",
        [table, run_date, status, rows],
    )
    return rows


def is_fresh(con, table, run_date):
    rule = RULES[table]
    return _table_exists(con, table) and CHECKS[rule["check"]](con, table, rule, run_date)


def ensure_fresh(con, table, run_date):
    """Make sure `table` is fresh for `run_date`, ingesting it if needed."""
    date.fromisoformat(run_date)  # fail early on a bad date
    _ensure_control_table(con)
    rule = RULES[table]
    label = f"{table} [{rule['check']}]"

    if is_fresh(con, table, run_date):
        print(f"    ✓ {label} is fresh, skipping ingestion")
        return

    if not rule.get("auto_ingest", False):
        raise StaleDataError(f"{label} is stale and auto_ingest is off")

    print(f"    … {label} is stale, running {INGESTION_DIR / (table + '.sql')}")
    rows = run_ingestion(con, table, run_date)

    if not is_fresh(con, table, run_date):
        raise StaleDataError(
            f"{label} is still stale after ingestion ({rows} rows for {run_date}). "
            "The external source is not ready."
        )
    print(f"    ✓ {label} ingested, {rows} rows")
