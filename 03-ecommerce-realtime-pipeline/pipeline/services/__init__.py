"""Build services from config/pipeline.yaml so every entry point wires them identically."""

from pipeline.config import PROJECT_ROOT, load_pipeline_config
from pipeline.services.aggregator import Aggregator
from pipeline.services.alerts import Alerts
from pipeline.services.sink import Sink
from pipeline.services.validator import Validator

CONSUMERS = ("validator", "aggregator", "alerts", "sink")


def build_service(name: str, broker):
    cfg = load_pipeline_config()
    if name == "validator":
        return Validator(broker)
    if name == "aggregator":
        return Aggregator(broker, **cfg["aggregator"])
    if name == "alerts":
        return Alerts(broker, **cfg["alerts"])
    if name == "sink":
        sink = cfg["sink"]
        return Sink(
            broker,
            database=PROJECT_ROOT / sink["database"],
            flush_max_rows=sink["flush_max_rows"],
            flush_interval_seconds=sink["flush_interval_seconds"],
            window_seconds=cfg["aggregator"]["window_seconds"],
        )
    raise ValueError(f"unknown service {name!r}; choose from {CONSUMERS}")
