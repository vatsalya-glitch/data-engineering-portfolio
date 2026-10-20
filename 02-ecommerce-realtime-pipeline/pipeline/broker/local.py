"""
LOCAL BROKER: a Pub/Sub clone on top of SQLite

Why build one? Because reading it is the fastest way to understand what a
message broker actually does. Everything Pub/Sub promises is implemented here
with two tables:

  messages    - the topic log. One row per published message.
  deliveries  - one row per (subscription, message) that is not yet acked.

  publish  -> insert into messages + one delivery row per subscription (fan-out)
  pull     -> lease visible deliveries: bump attempt, hide until ack deadline
  ack      -> delete the delivery row (done forever)
  nack     -> make the delivery visible again immediately
  timeout  -> a leased delivery that is never acked becomes visible again when
              its ack deadline passes (at-least-once delivery)
  DLQ      -> a delivery that already failed max_delivery_attempts times is
              forwarded to the dead-letter topic instead of being redelivered

SQLite in WAL mode lets several processes (producer, validator, sink, ...)
share one broker file safely, so you can run each service in its own terminal.
"""

import json
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from pipeline.broker.base import ReceivedMessage

SCHEMA = """
CREATE TABLE IF NOT EXISTS topics (
    name TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS subscriptions (
    name                  TEXT PRIMARY KEY,
    topic                 TEXT NOT NULL REFERENCES topics(name),
    ack_deadline_seconds  INTEGER NOT NULL,
    dead_letter_topic     TEXT,
    max_delivery_attempts INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
    message_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    topic        TEXT NOT NULL,
    data         BLOB NOT NULL,
    attributes   TEXT NOT NULL,
    publish_time REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS deliveries (
    subscription     TEXT NOT NULL,
    message_id       INTEGER NOT NULL,
    delivery_attempt INTEGER NOT NULL DEFAULT 0,
    visible_at       REAL NOT NULL,
    PRIMARY KEY (subscription, message_id)
);
CREATE INDEX IF NOT EXISTS idx_deliveries_visible
    ON deliveries (subscription, visible_at);
"""


class LocalBroker:
    def __init__(self, path: str | Path, clock=time.time):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock          # injectable so tests can fast-forward time
        self._local = threading.local()
        with self._conn() as conn:
            conn.executescript(SCHEMA)

    # ── connection handling ─────────────────────────────────────────
    def _conn(self) -> sqlite3.Connection:
        """One connection per thread (sqlite3 connections are not thread-safe)."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            # isolation_level=None: we issue BEGIN/COMMIT ourselves.
            conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return conn

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    # ── admin ───────────────────────────────────────────────────────
    def create_topic(self, topic: str) -> None:
        self._conn().execute("INSERT OR IGNORE INTO topics VALUES (?)", (topic,))

    def create_subscription(self, subscription, topic, ack_deadline_seconds=10,
                            dead_letter_topic=None, max_delivery_attempts=5) -> None:
        conn = self._conn()
        if not conn.execute("SELECT 1 FROM topics WHERE name = ?", (topic,)).fetchone():
            raise ValueError(f"Topic does not exist: {topic}")
        conn.execute(
            "INSERT OR IGNORE INTO subscriptions VALUES (?, ?, ?, ?, ?)",
            (subscription, topic, ack_deadline_seconds, dead_letter_topic, max_delivery_attempts),
        )

    # ── publish ─────────────────────────────────────────────────────
    def publish(self, topic: str, data: bytes, **attributes: str) -> str:
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            message_id = self._publish_in_txn(conn, topic, data, attributes)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        return message_id

    def _publish_in_txn(self, conn, topic, data, attributes) -> str:
        if not conn.execute("SELECT 1 FROM topics WHERE name = ?", (topic,)).fetchone():
            raise ValueError(f"Topic does not exist: {topic}")
        now = self.clock()
        cur = conn.execute(
            "INSERT INTO messages (topic, data, attributes, publish_time) VALUES (?, ?, ?, ?)",
            (topic, data, json.dumps(attributes), now),
        )
        message_id = cur.lastrowid
        # Fan-out: every subscription on the topic gets its own copy.
        conn.execute(
            """INSERT INTO deliveries (subscription, message_id, visible_at)
               SELECT name, ?, ? FROM subscriptions WHERE topic = ?""",
            (message_id, now, topic),
        )
        return str(message_id)

    # ── pull ────────────────────────────────────────────────────────
    def pull(self, subscription: str, max_messages: int = 100) -> list[ReceivedMessage]:
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")   # take the write lock: no double leasing
        try:
            sub = conn.execute(
                """SELECT ack_deadline_seconds, dead_letter_topic, max_delivery_attempts
                   FROM subscriptions WHERE name = ?""",
                (subscription,),
            ).fetchone()
            if sub is None:
                raise ValueError(f"Subscription does not exist: {subscription}")
            ack_deadline, dlq_topic, max_attempts = sub

            now = self.clock()
            rows = conn.execute(
                """SELECT d.message_id, d.delivery_attempt, m.data, m.attributes, m.publish_time
                   FROM deliveries d JOIN messages m USING (message_id)
                   WHERE d.subscription = ? AND d.visible_at <= ?
                   ORDER BY d.message_id
                   LIMIT ?""",
                (subscription, now, max_messages),
            ).fetchall()

            received = []
            for message_id, attempts, data, attrs, publish_time in rows:
                attributes = json.loads(attrs)
                if dlq_topic and attempts >= max_attempts:
                    # Poison message: stop retrying, park it on the dead-letter topic.
                    attributes.update(
                        source_subscription=subscription,
                        delivery_attempts=str(attempts),
                    )
                    self._publish_in_txn(conn, dlq_topic, data, attributes)
                    self._delete(conn, subscription, [message_id])
                    continue

                conn.execute(
                    """UPDATE deliveries SET delivery_attempt = delivery_attempt + 1,
                                             visible_at = ?
                       WHERE subscription = ? AND message_id = ?""",
                    (now + ack_deadline, subscription, message_id),
                )
                received.append(ReceivedMessage(
                    ack_id=f"{subscription}:{message_id}",
                    message_id=str(message_id),
                    data=data,
                    attributes=attributes,
                    publish_time=datetime.fromtimestamp(publish_time, tz=timezone.utc),
                    delivery_attempt=attempts + 1,
                ))
            conn.execute("COMMIT")
            return received
        except Exception:
            conn.execute("ROLLBACK")
            raise

    # ── ack / nack ──────────────────────────────────────────────────
    def ack(self, subscription: str, ack_ids: list[str]) -> None:
        if ack_ids:
            self._delete(self._conn(), subscription, [_message_id(a) for a in ack_ids])

    def nack(self, subscription: str, ack_ids: list[str]) -> None:
        if not ack_ids:
            return
        self._conn().executemany(
            "UPDATE deliveries SET visible_at = ? WHERE subscription = ? AND message_id = ?",
            [(self.clock(), subscription, _message_id(a)) for a in ack_ids],
        )

    def _delete(self, conn, subscription, message_ids) -> None:
        conn.executemany(
            "DELETE FROM deliveries WHERE subscription = ? AND message_id = ?",
            [(subscription, m) for m in message_ids],
        )

    # ── observability (not part of the Broker interface) ────────────
    def stats(self) -> list[dict]:
        """Backlog per subscription - the #1 health metric of any stream."""
        rows = self._conn().execute(
            """SELECT s.name, s.topic,
                      COUNT(d.message_id),
                      COALESCE(SUM(d.delivery_attempt > 1), 0),
                      MIN(m.publish_time)
               FROM subscriptions s
               LEFT JOIN deliveries d ON d.subscription = s.name
               LEFT JOIN messages m ON m.message_id = d.message_id
               GROUP BY s.name, s.topic ORDER BY s.topic, s.name"""
        ).fetchall()
        now = self.clock()
        return [
            {
                "subscription": name,
                "topic": topic,
                "backlog": backlog,
                "redelivered": redelivered,
                "oldest_unacked_age_s": round(now - oldest, 1) if oldest else 0.0,
            }
            for name, topic, backlog, redelivered, oldest in rows
        ]

    def topic_counts(self) -> dict[str, int]:
        rows = self._conn().execute(
            "SELECT topic, COUNT(*) FROM messages GROUP BY topic"
        ).fetchall()
        return dict(rows)


def _message_id(ack_id: str) -> int:
    return int(ack_id.rsplit(":", 1)[1])
