"""
EVENT-TIME TUMBLING WINDOWS WITH A WATERMARK

The core idea of stream processing, in one small class.

  Event time       when something happened (event["event_time"])
  Processing time  when we happen to see it (now)

Events arrive out of order, so we can never be *sure* a window is complete.
A watermark is our bet: "I don't expect events older than this any more."

  watermark = max(event_time seen so far) - allowed_lateness

  Window [start, end) is CLOSED and emitted once watermark >= end.
  An event for a window that is already closed is LATE -> side output.

    event time ──────────────────────────────────────────────────►
    | window 1 | window 2 | window 3 | window 4 |
                          ▲                     ▲
                      watermark            max event time
                  (windows left of here    (allowed_lateness
                   are final)               between them)

Bigger allowed_lateness = more complete results, but higher latency.
That trade-off is THE central knob in streaming systems.
"""

from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass
class Window:
    start: datetime
    end: datetime
    metrics: dict = field(default_factory=dict)

    def add(self, metrics: dict) -> None:
        for key, value in metrics.items():
            self.metrics[key] = self.metrics.get(key, 0) + value


class TumblingWindows:
    def __init__(self, window_seconds: int, allowed_lateness_seconds: int):
        self.size = timedelta(seconds=window_seconds)
        self.lateness = timedelta(seconds=allowed_lateness_seconds)
        self.open: dict[datetime, Window] = {}
        self.max_event_time: datetime | None = None

    @property
    def watermark(self) -> datetime | None:
        if self.max_event_time is None:
            return None
        return self.max_event_time - self.lateness

    def window_start(self, event_time: datetime) -> datetime:
        """Align to fixed boundaries: 12:00:00, 12:00:10, 12:00:20, ..."""
        offset = (event_time - EPOCH) // self.size
        return EPOCH + offset * self.size

    def add(self, event_time: datetime, metrics: dict) -> bool:
        """Add an event. Returns False if it is late (its window already closed)."""
        start = self.window_start(event_time)
        end = start + self.size
        if self.watermark is not None and end <= self.watermark:
            return False

        window = self.open.get(start)
        if window is None:
            window = self.open[start] = Window(start, end)
        window.add(metrics)

        if self.max_event_time is None or event_time > self.max_event_time:
            self.max_event_time = event_time
        return True

    def pop_closed(self) -> list[Window]:
        """Remove and return every window the watermark has passed."""
        if self.watermark is None:
            return []
        closed = sorted(
            (w for w in self.open.values() if w.end <= self.watermark),
            key=lambda w: w.start,
        )
        for w in closed:
            del self.open[w.start]
        return closed

    def pop_all(self) -> list[Window]:
        """End of stream (shutdown): emit everything still open."""
        windows = sorted(self.open.values(), key=lambda w: w.start)
        self.open.clear()
        return windows


class DedupCache:
    """Remembers the last N ids. Bounded state = bounded memory.

    At-least-once delivery means duplicates WILL arrive; a stateful operator
    that counts things must drop them or its numbers drift upward.
    """

    def __init__(self, max_size: int):
        self.max_size = max_size
        self._seen: OrderedDict[str, None] = OrderedDict()

    def seen(self, key: str) -> bool:
        """Return True if key was seen before; otherwise remember it."""
        if key in self._seen:
            self._seen.move_to_end(key)
            return True
        self._seen[key] = None
        if len(self._seen) > self.max_size:
            self._seen.popitem(last=False)
        return False
