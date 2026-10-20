import json
from pathlib import Path

from pipeline.contracts import validate_event
from pipeline.sources.shopify import order_to_event

SAMPLE = Path(__file__).resolve().parent.parent / "samples" / "shopify_orders_create.json"


def load_order():
    return json.loads(SAMPLE.read_text())


def test_shopify_order_satisfies_our_contract():
    event = validate_event(order_to_event(load_order()))
    assert event["payload"]["order_total"] == 129.90          # "129.90" string -> number
    assert event["user_id"] == "shopify-115310627314723954"
    assert [i["category"] for i in event["payload"]["items"]] == ["fashion", "books"]


def test_timestamp_is_converted_to_utc():
    event = order_to_event(load_order())
    assert event["event_time"] == "2026-10-15T14:02:00.000Z"  # 10:02 at -04:00


def test_retried_webhook_maps_to_same_event_id():
    # Shopify retries failed deliveries: same order -> same event_id -> stored once.
    assert order_to_event(load_order())["event_id"] == order_to_event(load_order())["event_id"]


def test_guest_checkout_and_unknown_sku():
    order = load_order()
    order["customer"] = None
    order["line_items"][0]["sku"] = "NEW-SKU"
    event = validate_event(order_to_event(order))
    assert event["user_id"] is None
    assert event["payload"]["items"][0]["category"] == "unknown"
