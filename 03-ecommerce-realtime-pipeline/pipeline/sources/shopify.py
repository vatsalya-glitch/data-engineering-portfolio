"""
SHOPIFY ADAPTER: map an `orders/create` webhook onto our event contract

Real pipelines rarely control the shape of incoming data. The usual pattern:
translate the external payload into YOUR contract at the edge, so nothing
downstream depends on Shopify's format.

What changes on the way in:
  - Money arrives as strings ("89.00")         -> numbers
  - Timestamps carry a shop-local offset        -> UTC
  - The order id becomes a deterministic event_id, so a webhook that Shopify
    retries maps to the SAME event_id and the sink stores it only once
  - Line items carry no category                -> joined from a catalog,
    the way you'd join a products table in a real pipeline

Try it against a running pipeline:
  python -m pipeline.sources.shopify samples/shopify_orders_create.json
"""

import json
import sys

from pipeline.events import encode, parse_ts, to_iso

# Stand-in for a products table. Shopify line items have a SKU but no category.
SKU_CATEGORIES = {"P-2002": "fashion", "P-6002": "books"}


def order_to_event(order: dict, catalog: dict | None = None) -> dict:
    """Shopify orders/create payload -> our `order_placed` event (schema v2)."""
    catalog = SKU_CATEGORIES if catalog is None else catalog
    customer = order.get("customer") or {}
    return {
        "event_id": f"shopify-order-{order['id']}",
        "event_type": "order_placed",
        "schema_version": 2,
        "event_time": to_iso(parse_ts(order["created_at"])),
        # The cart token ties the order to the shopper's browsing session.
        "session_id": order.get("cart_token") or order.get("checkout_token") or f"order-{order['id']}",
        "user_id": f"shopify-{customer['id']}" if customer.get("id") else None,
        "channel": order.get("source_name") or "unknown",
        "payload": {
            "order_id": str(order["id"]),
            "order_total": float(order["total_price"]),
            "currency": order["currency"],
            "items": [
                {
                    "product_id": item.get("sku") or str(item["product_id"]),
                    "category": catalog.get(item.get("sku"), "unknown"),
                    "quantity": item["quantity"],
                    "unit_price": float(item["price"]),
                }
                for item in order["line_items"]
            ],
        },
    }


def main(path: str) -> None:
    from pipeline.broker import get_broker, setup_topology

    with open(path) as f:
        event = order_to_event(json.load(f))
    broker = get_broker()
    setup_topology(broker)
    broker.publish("raw-events", encode(event), event_type=event["event_type"], source="shopify")
    broker.close()
    print(f"published {event['event_id']} to raw-events")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "samples/shopify_orders_create.json")
