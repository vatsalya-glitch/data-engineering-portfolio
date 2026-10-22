"""Validator routing and sink idempotency, using the real local broker + DuckDB."""

import duckdb
import pytest

from pipeline.broker import setup_topology
from pipeline.events import encode, to_iso, utc_now
from pipeline.services.sink import Sink
from pipeline.services.validator import Validator


def page_view(event_id="e-1", schema_version=2):
    event = {
        "event_id": event_id, "event_type": "page_viewed", "schema_version": schema_version,
        "event_time": to_iso(utc_now()), "session_id": "s-1", "user_id": None,
        "payload": {"page": "home"},
    }
    if schema_version == 2:
        event["channel"] = "ios"
    return event


@pytest.fixture
def topology(broker):
    setup_topology(broker)
    return broker


@pytest.fixture
def sink(topology, tmp_path):
    return Sink(topology, database=tmp_path / "test.duckdb", flush_max_rows=10_000,
                flush_interval_seconds=60, window_seconds=10)


def drain(broker, service):
    for sub in service.subscriptions:
        while msgs := broker.pull(sub):
            service.process(sub, msgs)


def query(sink, sql):
    with duckdb.connect(str(sink.db_path), read_only=True) as con:
        return con.execute(sql).fetchall()


def test_validator_routes_good_and_bad_events(topology):
    topology.publish("raw-events", encode(page_view("good", schema_version=1)))
    topology.publish("raw-events", encode({**page_view("bad"), "event_type": "nope"}))
    topology.publish("raw-events", b"{not json")

    drain(topology, Validator(topology))

    [clean] = topology.pull("sink-events-sub")
    assert clean.attributes == {"event_type": "page_viewed", "event_id": "good"}
    invalid = topology.pull("sink-invalid-sub")
    assert sorted(m.attributes["error"].split(":")[0] for m in invalid) == [
        "unknown event_type 'nope'", "unparseable"]


def test_sink_acks_only_after_flush(topology, sink):
    topology.publish("clean-events", encode({**page_view(), "source_schema_version": 2}))

    drain(topology, sink)
    assert sink.buffered == 1
    backlog = {r["subscription"]: r["backlog"] for r in topology.stats()}
    assert backlog["sink-events-sub"] == 1, "not acked before the data is durable"

    sink.flush()
    backlog = {r["subscription"]: r["backlog"] for r in topology.stats()}
    assert backlog["sink-events-sub"] == 0
    assert query(sink, "SELECT channel FROM bronze.events") == [("ios",)]


def test_sink_is_idempotent_for_duplicates(topology, sink):
    event = encode({**page_view("dup"), "source_schema_version": 2})
    topology.publish("clean-events", event)
    topology.publish("clean-events", event)       # duplicate in the same batch
    drain(topology, sink)
    sink.flush()

    topology.publish("clean-events", event)       # redelivery in a later batch
    drain(topology, sink)
    sink.flush()

    assert query(sink, "SELECT COUNT(*) FROM bronze.events") == [(1,)]


def test_window_results_are_upserted(topology, sink):
    def window(events):
        return encode({"window_start": "2026-01-01T12:00:00Z", "window_end": "2026-01-01T12:00:10Z",
                       "metrics": {"events": events}, "emitted_at": "2026-01-01T12:00:20Z"})

    topology.publish("window-results", window(5), record_type="window")
    drain(topology, sink)
    sink.flush()
    topology.publish("window-results", window(7), record_type="window")
    drain(topology, sink)
    sink.flush()

    assert query(sink, "SELECT events FROM gold.window_metrics") == [(7,)]
