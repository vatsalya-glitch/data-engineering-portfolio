"""
ALERTS: clean-events -> alerts

Event-driven reactions: the business wants to KNOW within seconds, not find
out tomorrow in a dashboard.

  high_value_order           stateless rule - one event is enough
  repeated_payment_failures  stateful rule  - N failures by one user within
                             a time window (a classic card-testing signal)

Alert ids are derived from the triggering event_id, so a redelivered event
produces the same alert_id and the sink stores it only once (idempotency).
"""

from collections import defaultdict, deque
from datetime import timedelta

from pipeline.events import decode, encode, parse_ts, to_iso, utc_now
from pipeline.services.base import Service
from pipeline.windowing import DedupCache


class Alerts(Service):
    name = "alerts"
    subscriptions = ["alerts-sub"]

    def __init__(self, broker, high_value_order_threshold: float,
                 payment_failure_burst: int, payment_failure_window_seconds: int):
        super().__init__(broker)
        self.high_value = high_value_order_threshold
        self.burst = payment_failure_burst
        self.burst_window = timedelta(seconds=payment_failure_window_seconds)
        self.failures: dict[str, deque] = defaultdict(deque)   # user_id -> failure times
        self.dedup = DedupCache(50_000)
        self.raised = 0

    def handle(self, msg) -> None:
        event = decode(msg.data)
        if self.dedup.seen(event["event_id"]):
            return
        if event["event_type"] == "order_placed":
            self._check_high_value(event)
        elif event["event_type"] == "payment_failed" and event.get("user_id"):
            self._check_failure_burst(event)

    def _check_high_value(self, event) -> None:
        total = event["payload"]["order_total"]
        if total >= self.high_value:
            self._raise("high_value_order", "medium", event,
                        {"order_id": event["payload"]["order_id"], "order_total": total})

    def _check_failure_burst(self, event) -> None:
        user, t = event["user_id"], parse_ts(event["event_time"])
        times = self.failures[user]
        times.append(t)
        while times and t - times[0] > self.burst_window:
            times.popleft()
        if len(times) >= self.burst:
            self._raise("repeated_payment_failures", "high", event,
                        {"failures": len(times),
                         "window_seconds": int(self.burst_window.total_seconds())})
            times.clear()   # one alert per burst, not one per extra failure

    def _raise(self, alert_type, severity, event, details) -> None:
        self.broker.publish("alerts", encode({
            "alert_id": f"{alert_type}:{event['event_id']}",
            "alert_type": alert_type,
            "severity": severity,
            "event_id": event["event_id"],
            "user_id": event.get("user_id"),
            "details": details,
            "created_at": to_iso(utc_now()),
        }), alert_type=alert_type, severity=severity)
        self.raised += 1
        self.log.info("ALERT %-26s user=%s %s", alert_type, event.get("user_id"), details)

    def close(self) -> None:
        self.log.info("alerts raised=%d", self.raised)
