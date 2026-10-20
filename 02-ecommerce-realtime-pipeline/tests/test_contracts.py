from datetime import timedelta

import pytest

from pipeline.contracts import ContractViolation, validate_event
from pipeline.events import to_iso, utc_now


def make_event(**overrides):
    event = {
        "event_id": "e-1",
        "event_type": "order_placed",
        "schema_version": 2,
        "event_time": to_iso(utc_now()),
        "session_id": "s-1",
        "user_id": "U-0001",
        "channel": "web",
        "payload": {"order_id": "O-1", "order_total": 42.0, "currency": "EUR",
                    "items": [{"product_id": "P-1", "category": "books",
                               "quantity": 1, "unit_price": 42.0}]},
    }
    event.update(overrides)
    return event


def test_valid_event_passes():
    out = validate_event(make_event())
    assert out["event_id"] == "e-1"
    assert out["source_schema_version"] == 2


def test_v1_event_is_upcast_to_latest_schema():
    v1 = make_event(schema_version=1)
    del v1["channel"]
    out = validate_event(v1)
    assert out["schema_version"] == 2
    assert out["source_schema_version"] == 1
    assert out["channel"] == "unknown"
    assert "channel" not in v1, "input must not be mutated"


def test_anonymous_user_is_allowed():
    validate_event(make_event(user_id=None))


@pytest.mark.parametrize("event, reason", [
    (make_event(event_id=None), "missing required envelope field 'event_id'"),
    (make_event(schema_version="two"), "must be integer"),
    (make_event(schema_version=True), "must be integer"),
    (make_event(schema_version=99), "unsupported schema_version"),
    (make_event(event_type="wishlist_added"), "unknown event_type"),
    (make_event(event_time="yesterday"), "must be timestamp"),
    (make_event(event_time=to_iso(utc_now() + timedelta(hours=1))), "in the future"),
    (make_event(payload=None), "missing payload"),
    (make_event(payload={"order_id": "O-1", "order_total": -5, "currency": "EUR", "items": [{}]}),
     "must be >= 0"),
    (make_event(payload={"order_id": "O-1", "order_total": 5, "currency": "EUR", "items": []}),
     "must not be empty"),
    (make_event(event_type="page_viewed", payload={}), "missing required payload field 'page'"),
    ("not a dict", "JSON object"),
])
def test_contract_violations(event, reason):
    with pytest.raises(ContractViolation, match=reason):
        validate_event(event)
