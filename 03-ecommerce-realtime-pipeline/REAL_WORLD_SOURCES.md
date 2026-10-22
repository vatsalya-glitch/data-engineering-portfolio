# From Simulator to Real Sources

This project's producer is a simulator. In a real company, the events in `raw-events` come from the store itself. This page connects the two.

## Database copies vs events

Most teams start by copying the shop's database into a warehouse every night or hour. That works, but a database stores **the current state**, not **what happened**:

| A shopper's visit | Stored in the shop database? |
|---|---|
| Viewed a product | ❌ Viewing writes nothing |
| Added to cart, then removed | ❌ Overwritten or deleted |
| Order placed | ✅ One row |
| Payment failed, then succeeded | ⚠️ Usually only the final status |

Funnels, cart abandonment and fraud patterns need **events**. Freshness is the other gap: an hourly copy means an alert arrives up to an hour late.

## Where real events come from

| Source | What it captures | Examples |
|---|---|---|
| **Tracking in the browser or app** | Clicks, page views, cart actions | GA4, Segment, Snowplow, Shopify Web Pixels |
| **Backend services** | Orders, payments, refunds | Webhooks, or services publishing events (use a *transactional outbox* so the database and the event can't disagree) |
| **Change Data Capture (CDC)** | Every row change in the database, within seconds | Debezium, Google Datastream, Fivetran, Airbyte |

## Example: a Shopify store

```
Shopify webhooks (orders/create, orders/paid, ...) ──► Google Pub/Sub ──┐
Shopify Web Pixels (product_viewed, ...) ──► small collector ───────────┼──► raw-events ──► this pipeline
Nightly Admin API bulk export ──────────────────────────────────────────┴──► reconciliation
```

- **Webhooks** can be delivered straight to a Google Pub/Sub topic, so the project's `PubSubBroker` would read them directly.
- **Web Pixels** report shopper behaviour. Ad blockers and cookie consent mean they miss some visitors: use them for trends and funnels, not exact revenue.
- **A nightly export** remains the source of truth for money. Real-time numbers are compared against it.

## The real problems map onto this project's sections

| Real-world problem | Handled in |
|---|---|
| Shopify retries webhooks, so the same order arrives twice | Idempotency: deterministic `event_id`, `ON CONFLICT` |
| A webhook arrives late or out of order | Event-time windows and watermarks |
| A new app version changes the payload | Contracts and upcasting |
| A payload that crashes processing | Dead-letter queue |
| A webhook is missed entirely | Reconciliation against the nightly export |

## Try it: map a Shopify order into the pipeline

`samples/shopify_orders_create.json` is a trimmed `orders/create` webhook. `pipeline/sources/shopify.py` converts it into our `order_placed` event:

| Shopify sends | Our contract wants | Conversion |
|---|---|---|
| `"total_price": "129.90"` (string) | number | `float(...)` |
| `"created_at": "...T10:02:00-04:00"` | UTC | `parse_ts` → `to_iso` |
| `"id": 820982911946154508` | stable `event_id` | `shopify-order-<id>`, so a retried webhook gets the same id |
| line items without a category | `category` | looked up from a catalog (in real life, a join with the products table) |

```bash
python -m pipeline sink &          # or run the services in separate terminals
python -m pipeline validator &
python -m pipeline.sources.shopify samples/shopify_orders_create.json
python -m pipeline report          # the order shows up in silver.orders
```

Run the last-but-one command twice: the order is still stored once.

This step (translating an external payload into **your** contract at the edge) is what makes the rest of the pipeline independent of any one vendor.
