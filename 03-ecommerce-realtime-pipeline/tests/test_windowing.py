from datetime import datetime, timedelta, timezone

from pipeline.windowing import DedupCache, TumblingWindows

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


def test_events_are_assigned_to_aligned_windows():
    w = TumblingWindows(window_seconds=10, allowed_lateness_seconds=5)
    assert w.window_start(at(0)) == at(0)
    assert w.window_start(at(9.999)) == at(0)
    assert w.window_start(at(10)) == at(10)


def test_window_closes_only_when_watermark_passes_its_end():
    w = TumblingWindows(window_seconds=10, allowed_lateness_seconds=5)
    w.add(at(1), {"events": 1})
    w.add(at(14), {"events": 1})       # watermark = 9  -> window [0,10) still open
    assert w.pop_closed() == []

    w.add(at(15), {"events": 1})       # watermark = 10 -> window [0,10) closes
    [closed] = w.pop_closed()
    assert closed.start == at(0)
    assert closed.metrics == {"events": 1}


def test_out_of_order_event_within_lateness_is_counted():
    w = TumblingWindows(window_seconds=10, allowed_lateness_seconds=5)
    w.add(at(12), {"events": 1})
    assert w.add(at(3), {"events": 1}) is True      # older, but window [0,10) still open
    w.add(at(20), {"events": 1})
    [first, *_] = w.pop_closed()
    assert first.metrics["events"] == 1 and first.start == at(0)


def test_event_for_closed_window_is_late():
    w = TumblingWindows(window_seconds=10, allowed_lateness_seconds=5)
    w.add(at(1), {"events": 1})
    w.add(at(30), {"events": 1})       # watermark = 25
    w.pop_closed()
    assert w.add(at(2), {"events": 1}) is False


def test_metrics_are_summed_and_pop_all_flushes_everything():
    w = TumblingWindows(window_seconds=10, allowed_lateness_seconds=5)
    w.add(at(1), {"events": 1, "revenue": 10.5})
    w.add(at(2), {"events": 1, "revenue": 4.5})
    w.add(at(11), {"events": 1})
    windows = w.pop_all()
    assert [x.metrics for x in windows] == [{"events": 2, "revenue": 15.0}, {"events": 1}]
    assert w.open == {}


def test_dedup_cache_is_bounded():
    cache = DedupCache(max_size=2)
    assert cache.seen("a") is False
    assert cache.seen("a") is True
    cache.seen("b")
    cache.seen("c")                    # evicts "a"
    assert cache.seen("a") is False
