"""
CONTROL LOG - the "this table is loaded" signal.

Ingestion WRITES one row here after every load attempt.
The freshness check READS it before a DAG uses a table.
This one table is the contract between the two halves of the project.
"""


def ensure_log_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS _freshness_log (
            table_name VARCHAR, run_date DATE, status VARCHAR, row_count INTEGER,
            watermark TIMESTAMP, batch_id VARCHAR, source_file VARCHAR,
            completed_at TIMESTAMP
        )
    """)


def write_log(con, table, run_date, status, row_count, batch_id, source_file, watermark=None):
    con.execute(
        "INSERT INTO _freshness_log VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, ?, now())",
        [table, run_date, status, row_count, watermark, batch_id, source_file],
    )


def last_watermark(con, table):
    """How far the table is complete: the newest watermark of a successful load."""
    return con.execute(
        "SELECT max(watermark) FROM _freshness_log WHERE table_name = ? AND status = 'SUCCESS'",
        [table],
    ).fetchone()[0]


def loaded_files(con, table):
    """Load history: source files already applied successfully (so they are never applied twice)."""
    rows = con.execute(
        "SELECT source_file FROM _freshness_log WHERE table_name = ? AND status = 'SUCCESS'",
        [table],
    ).fetchall()
    return {source_file for (source_file,) in rows}
