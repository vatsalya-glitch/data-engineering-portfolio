"""Print a summary of what landed in DuckDB. Run with: python -m pipeline report"""

import time

import duckdb

from pipeline.config import PROJECT_ROOT, load_pipeline_config


def database_path():
    return PROJECT_ROOT / load_pipeline_config()["sink"]["database"]


def show(con, title: str, sql: str, limit: int | None = None) -> None:
    print(f"\n── {title} " + "─" * max(0, 66 - len(title)))
    rel = con.sql(sql)
    if limit:
        rel = rel.limit(limit)
    rel.show(max_width=120)


def connect_read_only(path, attempts: int = 10):
    """The sink holds the file lock for a moment during each flush - wait it out."""
    for attempt in range(attempts):
        try:
            return duckdb.connect(str(path), read_only=True)
        except duckdb.IOException:
            if attempt == attempts - 1:
                raise
            time.sleep(0.5)


def print_report(path=None) -> None:
    path = path or database_path()
    if not path.exists():
        print(f"No database at {path} yet - run the pipeline first.")
        return

    with connect_read_only(path) as con:
        show(con, "Pipeline health (rows per dataset)", "SELECT * FROM ops.pipeline_health")
        show(con, "Conversion funnel", "SELECT * FROM gold.conversion_funnel")
        show(con, "Revenue by category (paid orders)", "SELECT * FROM gold.revenue_by_category")
        show(con, "Channel mix (v1 events upcast to channel='unknown')",
             "SELECT * FROM gold.channel_mix")
        show(con, "Real-time windows (latest 8)",
             """SELECT strftime(window_start, '%H:%M:%S') AS window, events, page_views,
                       add_to_carts, orders, revenue, payment_failures
                FROM gold.window_metrics ORDER BY window_start DESC LIMIT 8""")
        show(con, "Stream vs batch reconciliation (totals)",
             """SELECT SUM(stream_events) AS stream_events, SUM(batch_events) AS batch_events,
                       SUM(late_events) AS late_events, SUM(unexplained) AS unexplained
                FROM gold.window_reconciliation""")
        show(con, "Ingestion latency: event_time -> DuckDB", "SELECT * FROM gold.ingestion_latency")
        show(con, "Alerts", """SELECT alert_type, severity, COUNT(*) AS alerts
                               FROM ops.alerts GROUP BY ALL ORDER BY alerts DESC""")
        show(con, "Quarantine: why events were rejected",
             """SELECT regexp_replace(error, '''[^'']*''|-?\\d[\\d.:TZ+-]*', '…', 'g') AS reason,
                       COUNT(*) AS events
                FROM quarantine.invalid_events GROUP BY 1 ORDER BY 2 DESC""")
    print(f"\nExplore it yourself:  duckdb {path.relative_to(PROJECT_ROOT)}")
