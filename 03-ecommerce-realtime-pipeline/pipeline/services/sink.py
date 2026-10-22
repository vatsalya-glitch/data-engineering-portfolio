"""
SINK: topics -> DuckDB (the only service that writes to the database)

Three ideas worth studying here:

1. MICRO-BATCHING
   Writing one row per message is slow (one transaction each). The sink
   buffers rows and flushes when the batch is big enough OR old enough -
   trading a couple of seconds of latency for 100x throughput.

2. ACK AFTER COMMIT
   Messages are acked only after the DuckDB transaction commits. If the sink
   crashes mid-batch, the messages are redelivered and written again.
   Writes use INSERT ... ON CONFLICT on natural keys, so "written again" is
   harmless. At-least-once delivery + idempotent writes = effectively-once.

3. SINGLE WRITER
   DuckDB allows one writing process per database file. Instead of fighting
   that, the architecture embraces it: every service publishes to topics and
   only the sink touches the file. It opens the database per flush and closes
   it right after, so you can query it between flushes.
"""

import json
import time
from collections import defaultdict
from pathlib import Path

import duckdb

from pipeline.config import PROJECT_ROOT
from pipeline.events import decode, parse_ts, to_naive_utc, utc_now
from pipeline.services.base import Service

SQL_DIR = PROJECT_ROOT / "sql"

INSERTS = {
    "bronze.events": """
        INSERT INTO bronze.events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING""",
    "quarantine.invalid_events": """
        INSERT INTO quarantine.invalid_events VALUES (?, ?, ?, ?)
        ON CONFLICT DO NOTHING""",
    "quarantine.dead_letters": """
        INSERT INTO quarantine.dead_letters VALUES (?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING""",
    "gold.window_metrics": """
        INSERT OR REPLACE INTO gold.window_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
    "gold.late_events": """
        INSERT INTO gold.late_events VALUES (?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING""",
    "ops.alerts": """
        INSERT INTO ops.alerts VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING""",
}


def ts(value: str):
    return to_naive_utc(parse_ts(value))


def init_database(path: Path, window_seconds: int) -> None:
    """Create tables and (re)create the analytical views."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(path)) as con:
        con.execute((SQL_DIR / "tables.sql").read_text())
        models = (SQL_DIR / "models.sql").read_text()
        con.execute(models.replace("{window_seconds}", str(window_seconds)))


class Sink(Service):
    name = "sink"
    subscriptions = [
        "sink-events-sub",
        "sink-invalid-sub",
        "sink-windows-sub",
        "sink-alerts-sub",
        "sink-dlq-sub",
    ]

    def __init__(self, broker, database: str | Path, flush_max_rows: int,
                 flush_interval_seconds: float, window_seconds: int):
        super().__init__(broker)
        self.db_path = Path(database)
        self.flush_max_rows = flush_max_rows
        self.flush_interval = flush_interval_seconds
        init_database(self.db_path, window_seconds)

        self.rows: dict[str, list[tuple]] = defaultdict(list)
        self.pending_acks: dict[str, list[str]] = defaultdict(list)
        self.buffered = 0
        self.batch_started: float | None = None
        self.flushes = 0
        self.rows_written = 0

    # ── buffering ───────────────────────────────────────────────────
    def process(self, subscription, messages) -> None:
        nacks = []
        for msg in messages:
            try:
                table, row = self.to_row(subscription, msg)
            except Exception:
                # Can't even map it to a row: let the broker retry, then dead-letter it.
                self.log.exception("cannot map message %s from %s", msg.message_id, subscription)
                nacks.append(msg.ack_id)
                self.failed += 1
                continue
            self.rows[table].append(row)
            self.pending_acks[subscription].append(msg.ack_id)
            self.buffered += 1
        self.broker.nack(subscription, nacks)
        if self.batch_started is None and self.buffered:
            self.batch_started = time.monotonic()
        if self.buffered >= self.flush_max_rows:
            self.flush()

    def tick(self) -> None:
        if self.batch_started and time.monotonic() - self.batch_started >= self.flush_interval:
            self.flush()

    def close(self) -> None:
        self.flush()
        self.log.info("flushes=%d rows written=%d", self.flushes, self.rows_written)

    # ── message -> row mapping ──────────────────────────────────────
    def to_row(self, subscription: str, msg) -> tuple[str, tuple]:
        now = to_naive_utc(utc_now())

        if subscription == "sink-events-sub":
            e = decode(msg.data)
            published = to_naive_utc(msg.publish_time) if msg.publish_time else None
            return "bronze.events", (
                e["event_id"], e["event_type"], e["schema_version"], e["source_schema_version"],
                ts(e["event_time"]), e["session_id"], e.get("user_id"), e.get("channel"),
                json.dumps(e["payload"]), published, now,
            )

        if subscription == "sink-invalid-sub":
            return "quarantine.invalid_events", (
                msg.attributes.get("source_message_id", msg.message_id),
                msg.attributes.get("error", "unknown"),
                msg.data.decode("utf-8", errors="replace"),
                now,
            )

        if subscription == "sink-dlq-sub":
            return "quarantine.dead_letters", (
                msg.message_id,
                msg.attributes.get("source_subscription"),
                int(msg.attributes.get("delivery_attempts", msg.delivery_attempt)),
                msg.data.decode("utf-8", errors="replace"),
                now,
            )

        if subscription == "sink-windows-sub":
            r = decode(msg.data)
            if msg.attributes.get("record_type") == "late_event":
                return "gold.late_events", (
                    r["event_id"], r["event_type"], ts(r["event_time"]),
                    ts(r["watermark"]), ts(r["detected_at"]),
                )
            m = r["metrics"]
            return "gold.window_metrics", (
                ts(r["window_start"]), ts(r["window_end"]),
                m.get("events", 0), m.get("page_views", 0), m.get("product_views", 0),
                m.get("add_to_carts", 0), m.get("checkouts", 0), m.get("orders", 0),
                round(m.get("revenue", 0.0), 2), m.get("payment_failures", 0),
                ts(r["emitted_at"]),
            )

        if subscription == "sink-alerts-sub":
            a = decode(msg.data)
            return "ops.alerts", (
                a["alert_id"], a["alert_type"], a["severity"], a["event_id"],
                a.get("user_id"), json.dumps(a.get("details")), ts(a["created_at"]),
            )

        raise ValueError(f"no mapping for subscription {subscription}")

    # ── flush: one transaction, then ack ────────────────────────────
    def flush(self) -> None:
        if not self.buffered:
            return
        try:
            con = duckdb.connect(str(self.db_path))
        except duckdb.IOException:
            # Someone (e.g. a report) holds the file lock. Keep the batch, retry next tick.
            self.log.warning("database busy - will retry flush")
            return

        try:
            con.execute("BEGIN TRANSACTION")
            for table, rows in self.rows.items():
                con.executemany(INSERTS[table], rows)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            self.log.exception("flush failed - nacking %d messages for redelivery", self.buffered)
            for subscription, ack_ids in self.pending_acks.items():
                self.broker.nack(subscription, ack_ids)
            self.failed += self.buffered
            self._reset()
            return
        finally:
            con.close()

        # Only now - after the data is durable - do we tell the broker we're done.
        for subscription, ack_ids in self.pending_acks.items():
            self.broker.ack(subscription, ack_ids)
        summary = ", ".join(f"{t}={len(r)}" for t, r in sorted(self.rows.items()))
        self.log.info("flushed %d rows (%s)", self.buffered, summary)
        self.flushes += 1
        self.rows_written += self.buffered
        self.processed += self.buffered
        self._reset()

    def _reset(self) -> None:
        self.rows.clear()
        self.pending_acks.clear()
        self.buffered = 0
        self.batch_started = None
