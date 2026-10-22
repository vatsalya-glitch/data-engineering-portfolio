"""
PRODUCER: a simulated online store -> raw-events

Shoppers move through a funnel, each step emitting an event:

  page_viewed -> product_viewed -> add_to_cart -> checkout_started
              -> order_placed -> payment_succeeded | payment_failed (-> retry)

Most shoppers leave early, like in real life. A few "risky" users fail
payment repeatedly, which the alerts service should catch.

On top of that, real-world mess is injected ON PURPOSE (rates in
config/pipeline.yaml) so the rest of the pipeline has something to handle:

  duplicates   same event published twice (client retry after a timeout)
  late events  event_time minutes in the past (phone was offline)
  malformed    missing fields, wrong types, negative totals, broken JSON
  old schema   schema_version 1 events without the 'channel' field
"""

import copy
import logging
import random
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import timedelta

from pipeline.events import encode, to_iso, utc_now

log = logging.getLogger("producer")

CATALOG = [
    ("P-1001", "electronics", 899.00), ("P-1002", "electronics", 249.99),
    ("P-1003", "electronics", 59.90),  ("P-1004", "electronics", 129.00),
    ("P-2001", "fashion", 39.95),      ("P-2002", "fashion", 89.00),
    ("P-2003", "fashion", 149.50),     ("P-2004", "fashion", 24.99),
    ("P-3001", "home", 74.90),         ("P-3002", "home", 19.99),
    ("P-3003", "home", 199.00),        ("P-4001", "beauty", 14.50),
    ("P-4002", "beauty", 32.00),       ("P-5001", "sports", 54.99),
    ("P-5002", "sports", 279.00),      ("P-6001", "books", 12.99),
    ("P-6002", "books", 22.50),
]
PAGES = ["home", "search", "category", "deals", "help"]
CHANNELS = ["web", "web", "web", "ios", "android"]
FAILURE_REASONS = ["card_declined", "insufficient_funds", "3ds_failed", "expired_card"]


@dataclass
class Session:
    session_id: str
    user_id: str | None
    channel: str
    schema_version: int
    risky: bool
    stage: str = "start"
    cart: dict = field(default_factory=dict)       # product_id -> (category, price, qty)
    viewed: list = field(default_factory=list)
    order_id: str | None = None
    order_total: float = 0.0
    payment_attempts: int = 0


class Storefront:
    """Deterministic (seeded) generator of shopper behaviour."""

    def __init__(self, seed: int = 42, v1_rate: float = 0.3, concurrent_sessions: int = 40):
        self.rng = random.Random(seed)
        self.v1_rate = v1_rate
        self.concurrent = concurrent_sessions
        self.sessions: list[Session] = []
        self.users = [f"U-{n:04d}" for n in range(1, 401)]
        self.risky_users = self.rng.sample(self.users, 6)

    def _new_session(self) -> Session:
        rng = self.rng
        user = None
        if rng.random() < 0.7:   # 70% of visitors are logged in
            pool = self.risky_users if rng.random() < 0.08 else self.users
            user = rng.choice(pool)
        return Session(
            session_id=f"S-{uuid.UUID(int=rng.getrandbits(128)).hex[:12]}",
            user_id=user,
            channel=rng.choice(CHANNELS),
            schema_version=1 if rng.random() < self.v1_rate else 2,
            risky=user in self.risky_users,
        )

    def next_event(self) -> dict:
        while len(self.sessions) < self.concurrent:
            self.sessions.append(self._new_session())
        session = self.rng.choice(self.sessions)
        event = self._advance(session)
        if session.stage == "done":
            self.sessions.remove(session)
        return event

    def _advance(self, s: Session) -> dict:
        rng = self.rng

        if s.stage == "start":
            s.stage = "browsing"
            return self._event(s, "page_viewed", page="home")

        if s.stage == "browsing":
            roll = rng.random()
            if roll < 0.45 or not s.viewed:
                pid, cat, price = rng.choice(CATALOG)
                s.viewed.append((pid, cat, price))
                return self._event(s, "product_viewed", product_id=pid, category=cat, price=price)
            if roll < 0.70:
                pid, cat, price = rng.choice(s.viewed)
                qty = rng.choice([1, 1, 1, 2, 3])
                _, _, old = s.cart.get(pid, (cat, price, 0))
                s.cart[pid] = (cat, price, old + qty)
                return self._event(s, "add_to_cart", product_id=pid, category=cat,
                                   price=price, quantity=qty)
            if roll < 0.75 and s.cart:
                pid = rng.choice(list(s.cart))
                _, _, qty = s.cart.pop(pid)
                return self._event(s, "remove_from_cart", product_id=pid, quantity=qty)
            # Card testers don't browse: as soon as something is in the cart they pay.
            if (roll < 0.88 or s.risky) and s.cart:
                s.stage = "checkout"
                return self._event(s, "checkout_started", cart_value=self._cart_value(s),
                                   item_count=sum(q for _, _, q in s.cart.values()))
            if roll < 0.93:
                s.stage = "done"   # bounced
            return self._event(s, "page_viewed", page=rng.choice(PAGES))

        if s.stage == "checkout":
            if rng.random() < 0.25 and not s.risky:
                s.stage = "done"   # abandoned checkout
                return self._event(s, "page_viewed", page="cart")
            s.stage = "paying"
            s.order_id = f"O-{uuid.UUID(int=rng.getrandbits(128)).hex[:10]}"
            s.order_total = self._cart_value(s)
            items = [
                {"product_id": pid, "category": cat, "quantity": qty, "unit_price": price}
                for pid, (cat, price, qty) in s.cart.items()
            ]
            return self._event(s, "order_placed", order_id=s.order_id, order_total=s.order_total,
                               currency="EUR", items=items)

        if s.stage == "paying":
            s.payment_attempts += 1
            fail_prob = 0.85 if s.risky else 0.08
            if rng.random() < fail_prob:
                if s.payment_attempts >= (5 if s.risky else 2):
                    s.stage = "done"
                return self._event(s, "payment_failed", order_id=s.order_id,
                                   reason=rng.choice(FAILURE_REASONS))
            s.stage = "done"
            return self._event(s, "payment_succeeded", order_id=s.order_id, amount=s.order_total)

        raise RuntimeError(f"unexpected stage {s.stage}")

    @staticmethod
    def _cart_value(s: Session) -> float:
        return round(sum(price * qty for _, price, qty in s.cart.values()), 2)

    def _event(self, s: Session, event_type: str, **payload) -> dict:
        event = {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
            "event_type": event_type,
            "schema_version": s.schema_version,
            "event_time": to_iso(utc_now()),
            "session_id": s.session_id,
            "user_id": s.user_id,
            "payload": payload,
        }
        if s.schema_version >= 2:
            event["channel"] = s.channel
        return event


class ChaosMonkey:
    """Applies the configured real-world messiness to clean events."""

    def __init__(self, rng: random.Random, duplicate_rate, late_rate, late_delay_seconds,
                 malformed_rate, **_):
        self.rng = rng
        self.duplicate_rate = duplicate_rate
        self.late_rate = late_rate
        self.late_delay = late_delay_seconds
        self.malformed_rate = malformed_rate

    def make_late(self, event: dict) -> bool:
        if self.rng.random() >= self.late_rate:
            return False
        delay = self.rng.uniform(*self.late_delay)
        event["event_time"] = to_iso(utc_now() - timedelta(seconds=delay))
        return True

    def corrupt(self, event: dict) -> bytes | None:
        """Return broken bytes instead of the event, or None to leave it alone."""
        if self.rng.random() >= self.malformed_rate:
            return None
        bad = copy.deepcopy(event)
        kind = self.rng.choice(["missing_field", "wrong_type", "unknown_type",
                                "negative_total", "not_json", "future_time"])
        if kind == "missing_field":
            del bad[self.rng.choice(["event_id", "session_id", "event_time"])]
        elif kind == "wrong_type":
            bad["schema_version"] = "two"
        elif kind == "unknown_type":
            bad["event_type"] = "wishlist_added"
        elif kind == "negative_total":
            bad["event_type"] = "order_placed"
            bad["payload"] = {"order_id": "O-broken", "order_total": -10.0,
                              "currency": "EUR", "items": [{"product_id": "P-1001"}]}
        elif kind == "future_time":
            bad["event_time"] = to_iso(utc_now() + timedelta(days=1))
        else:
            return b'{"event_id": "' + event["event_id"].encode() + b'", "truncated'
        return encode(bad)

    def duplicate(self) -> bool:
        return self.rng.random() < self.duplicate_rate


def run_producer(broker, stop: threading.Event, *, events_per_second: float,
                 seed: int = 42, max_events: int | None = None,
                 duration_seconds: float | None = None, **chaos) -> dict:
    """Publish events at a steady rate until stopped, max_events, or duration."""
    store = Storefront(seed=seed, v1_rate=chaos.get("v1_rate", 0.3))
    monkey = ChaosMonkey(random.Random(seed + 1), **chaos)
    stats = {"published": 0, "duplicates": 0, "late": 0, "malformed": 0, "valid_unique": 0}

    interval = 1.0 / events_per_second
    started = time.monotonic()
    next_at = started
    log.info("publishing ~%s events/sec to raw-events", events_per_second)

    while not stop.is_set():
        if max_events is not None and stats["published"] >= max_events:
            break
        if duration_seconds is not None and time.monotonic() - started >= duration_seconds:
            break

        event = store.next_event()
        broken = monkey.corrupt(event)
        if broken is not None:
            broker.publish("raw-events", broken)
            stats["malformed"] += 1
        else:
            if monkey.make_late(event):
                stats["late"] += 1
            data = encode(event)
            broker.publish("raw-events", data, event_type=event["event_type"])
            stats["valid_unique"] += 1
            if monkey.duplicate():
                broker.publish("raw-events", data, event_type=event["event_type"])
                stats["duplicates"] += 1
                stats["published"] += 1
        stats["published"] += 1

        if stats["published"] % 250 == 0:
            log.info("published %d events", stats["published"])

        # Pace ourselves on a fixed schedule (no drift from slow publishes).
        next_at += interval
        delay = next_at - time.monotonic()
        if delay > 0:
            stop.wait(delay)

    log.info("done: %s", stats)
    return stats
