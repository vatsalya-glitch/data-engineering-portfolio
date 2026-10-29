"""
ENSURE FRESH - the reusable "check, then ingest if needed" step.

Any DAG that needs a table calls ensure_fresh(con, table, run_date) first:

  1. CHECK    - is the table fresh for run_date? (strategy comes from the YAML)
  2. INGEST   - if not, run the table's ingestion (ingestion/ingest.py)
  3. RE-CHECK - still not fresh? fail loudly: the source itself is not ready

The ingestion half writes _freshness_log; this half reads it.

Paths are relative to the project root (run_demo.py changes into it).
"""

from datetime import date

from common.config import TABLES as RULES
from common.control_log import ensure_log_table
from ingestion.ingest import ingest


class StaleDataError(Exception):
    """Raised when a table cannot be made fresh."""


def _table_exists(con, table):
    return con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [table]
    ).fetchone()[0] > 0


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


def is_fresh(con, table, run_date):
    rule = RULES[table]
    return _table_exists(con, table) and CHECKS[rule["check"]](con, table, rule, run_date)


def ensure_fresh(con, table, run_date):
    """Make sure `table` is fresh for `run_date`, ingesting it if needed."""
    date.fromisoformat(run_date)  # fail early on a bad date
    ensure_log_table(con)
    rule = RULES[table]
    label = f"{table} [{rule['check']}]"

    if is_fresh(con, table, run_date):
        print(f"    ✓ {label} is fresh, skipping ingestion")
        return

    if not rule.get("auto_ingest", False):
        raise StaleDataError(f"{label} is stale and auto_ingest is off")

    print(f"    … {label} is stale, running {rule['ingestion']} ingestion")
    status = ingest(con, table, run_date)

    if not is_fresh(con, table, run_date):
        raise StaleDataError(
            f"{label} is still stale after ingestion (status {status}) for {run_date}. "
            "The external source is not ready."
        )
    print(f"    ✓ {label} is fresh now")
