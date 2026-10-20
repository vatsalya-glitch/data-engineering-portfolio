"""Run the whole pipeline on a small, seeded stream and check every number adds up."""

import threading

import duckdb

from pipeline.broker import setup_topology
from pipeline.config import load_pipeline_config
from pipeline.services.aggregator import Aggregator
from pipeline.services.alerts import Alerts
from pipeline.services.base import run_service
from pipeline.services.producer import run_producer
from pipeline.services.sink import Sink
from pipeline.services.validator import Validator
from run_pipeline import wait_until_drained


def test_pipeline_end_to_end(tmp_path):
    from pipeline.broker.local import LocalBroker

    cfg = load_pipeline_config()
    broker = LocalBroker(tmp_path / "broker.sqlite3")
    setup_topology(broker)

    services = {
        "validator": Validator(broker),
        "aggregator": Aggregator(broker, **cfg["aggregator"]),
        "alerts": Alerts(broker, **cfg["alerts"]),
        "sink": Sink(broker, database=tmp_path / "e2e.duckdb", flush_max_rows=200,
                     flush_interval_seconds=0.5, window_seconds=cfg["aggregator"]["window_seconds"]),
    }
    stops = {name: threading.Event() for name in services}
    threads = {name: threading.Thread(target=run_service, args=(svc, stops[name]), daemon=True)
               for name, svc in services.items()}
    for t in threads.values():
        t.start()

    producer_cfg = {**cfg["producer"], "events_per_second": 2000}
    stats = run_producer(broker, threading.Event(), max_events=600, **producer_cfg)

    for stage in (["validator"], ["aggregator", "alerts"], ["sink"]):
        for name in stage:
            wait_until_drained(broker, services[name], quiet_seconds=1.0, timeout=30)
        for name in stage:
            stops[name].set()
            threads[name].join(timeout=30)

    with duckdb.connect(str(tmp_path / "e2e.duckdb"), read_only=True) as con:
        def one(sql):
            return con.execute(sql).fetchone()[0]

        # Duplicates were published but each event landed exactly once.
        assert stats["duplicates"] > 0
        assert one("SELECT COUNT(*) FROM bronze.events") == stats["valid_unique"]
        # Every malformed event is quarantined, none silently lost.
        assert one("SELECT COUNT(*) FROM quarantine.invalid_events") == stats["malformed"]
        assert one("SELECT COUNT(*) FROM quarantine.dead_letters") == 0
        # Streaming windows + late events explain the batch numbers exactly.
        assert one("SELECT SUM(unexplained) FROM gold.window_reconciliation") == 0
        assert one("SELECT COUNT(*) FROM gold.window_metrics") > 0
    broker.close()
