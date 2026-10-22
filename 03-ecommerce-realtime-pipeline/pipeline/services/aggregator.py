"""
AGGREGATOR: clean-events -> window-results

Real-time KPIs per 10-second event-time window (see pipeline/windowing.py).

Two kinds of records are published to window-results:
  record_type=window      a closed window and its metrics (final result)
  record_type=late_event  an event that arrived after its window closed
                          (a "side output" - kept for reconciliation)

Trade-off made explicitly here: window state lives in memory and messages are
acked once they are added to it. A crash loses the open windows. Production
engines (Flink, Dataflow, Spark) avoid that by checkpointing state together
with their read position. STREAMING_CONCEPTS.md explains how.
"""

from pipeline.events import decode, encode, parse_ts, to_iso, utc_now
from pipeline.services.base import Service
from pipeline.windowing import DedupCache, TumblingWindows

METRIC_FOR_TYPE = {
    "page_viewed": "page_views",
    "product_viewed": "product_views",
    "add_to_cart": "add_to_carts",
    "checkout_started": "checkouts",
    "order_placed": "orders",
    "payment_failed": "payment_failures",
}


def metrics_for(event: dict) -> dict:
    metrics = {"events": 1}
    name = METRIC_FOR_TYPE.get(event["event_type"])
    if name:
        metrics[name] = 1
    if event["event_type"] == "order_placed":
        metrics["revenue"] = event["payload"]["order_total"]
    return metrics


class Aggregator(Service):
    name = "aggregator"
    subscriptions = ["aggregator-sub"]

    def __init__(self, broker, window_seconds: int, allowed_lateness_seconds: int,
                 dedup_cache_size: int):
        super().__init__(broker)
        self.windows = TumblingWindows(window_seconds, allowed_lateness_seconds)
        self.dedup = DedupCache(dedup_cache_size)
        self.duplicates = 0
        self.late = 0
        self.emitted = 0

    def handle(self, msg) -> None:
        event = decode(msg.data)
        if self.dedup.seen(event["event_id"]):
            self.duplicates += 1
            return

        event_time = parse_ts(event["event_time"])
        if not self.windows.add(event_time, metrics_for(event)):
            self.late += 1
            self.broker.publish("window-results", encode({
                "event_id": event["event_id"],
                "event_type": event["event_type"],
                "event_time": event["event_time"],
                "watermark": to_iso(self.windows.watermark),
                "detected_at": to_iso(utc_now()),
            }), record_type="late_event")

    def tick(self) -> None:
        self._emit(self.windows.pop_closed())

    def close(self) -> None:
        self._emit(self.windows.pop_all())
        self.log.info("windows emitted=%d late events=%d duplicates dropped=%d",
                      self.emitted, self.late, self.duplicates)

    def _emit(self, windows) -> None:
        for w in windows:
            self.broker.publish("window-results", encode({
                "window_start": to_iso(w.start),
                "window_end": to_iso(w.end),
                "metrics": w.metrics,
                "emitted_at": to_iso(utc_now()),
            }), record_type="window")
            self.emitted += 1
