"""
Event serialisation helpers.

Wire format: UTF-8 JSON. Every timestamp is UTC, ISO-8601, with a 'Z' suffix.
Brokers only move bytes - the meaning of those bytes is a contract between
producers and consumers (see config/event_contracts.yaml).
"""

import json
from datetime import datetime, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_ts(value: str) -> datetime:
    """Parse an ISO-8601 timestamp into an aware UTC datetime."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_naive_utc(dt: datetime) -> datetime:
    """DuckDB TIMESTAMP columns hold naive UTC values."""
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def encode(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


def decode(data: bytes) -> dict:
    return json.loads(data.decode("utf-8"))
