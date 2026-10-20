# E-commerce Real-Time Pipeline

**An educational project about streaming data engineering.** A simulated online store streams shopper events through a Pub/Sub-style message broker. Small event-driven services validate, aggregate and react to those events, and a single sink writes everything to DuckDB.

Everything runs on your laptop with **zero cloud setup**. The same code also runs unchanged against the **Google Cloud Pub/Sub emulator**, or against a real GCP project.

```bash
pip install -r requirements.txt
python run_pipeline.py          # 60 seconds of live traffic, then a report
```

## What You'll Learn

| Concept | Where it lives in this project |
|---------|-------------------------------|
| **Pub/Sub messaging**: topics, subscriptions, fan-out | `pipeline/broker/`, `config/pipeline.yaml` |
| **Delivery guarantees**: at-least-once, ack deadlines, redelivery | `pipeline/broker/local.py`, `pipeline/services/base.py` |
| **Dead-letter queues**: handling poison messages | `pipeline/broker/local.py` (`pull`) |
| **Data contracts and schema evolution**: validate, quarantine, upcast | `pipeline/contracts.py`, `config/event_contracts.yaml` |
| **Idempotency**: making duplicates harmless | `sql/tables.sql`, `pipeline/services/sink.py` |
| **Event time vs processing time**: windows, watermarks, late data | `pipeline/windowing.py`, `pipeline/services/aggregator.py` |
| **Stateful stream processing**: dedup, rolling counters | `pipeline/services/alerts.py` |
| **Micro-batching sinks**: ack only after commit | `pipeline/services/sink.py` |
| **Stream vs batch reconciliation** | `gold.window_reconciliation` in `sql/models.sql` |
| **Local-first development**: ports and adapters, swappable infra | `pipeline/broker/__init__.py` |

The theory behind each concept is in **[STREAMING_CONCEPTS.md](./STREAMING_CONCEPTS.md)**.

## Architecture

```
                                   ┌──────────────┐
  ┌──────────────┐   raw-events    │  VALIDATOR   │   invalid-events
  │   PRODUCER   │ ──────────────► │  contracts,  │ ─────────────────────────────┐
  │  simulated   │                 │  upcasting   │                              │
  │  storefront  │                 └──────┬───────┘                              │
  └──────────────┘                        │ clean-events                         │
   + duplicates                           │ (fan-out: 3 subscriptions)           │
   + late events          ┌───────────────┼──────────────────┐                   │
   + malformed            ▼               ▼                  ▼                   ▼
   + old schema   ┌──────────────┐ ┌──────────────┐  ┌──────────────────────────────┐
                  │  AGGREGATOR  │ │    ALERTS    │  │            SINK              │
                  │  10s windows │ │ high value,  │  │  micro-batch, idempotent,    │
                  │  watermark   │ │ card testing │  │  ack after commit            │
                  └──────┬───────┘ └──────┬───────┘  └──────────────┬───────────────┘
                         │ window-results │ alerts       ▲   ▲      │
                         └────────────────┼──────────────┘   │      ▼
                                          └──────────────────┘  ┌─────────┐
                                                                │ DuckDB  │
             any subscription failing 5x ──► dead-letter ──► sink  └─────────┘
```

Services never call each other. They only publish to topics and subscribe to them. That gives you:

- **Decoupling**: add a new consumer (for example fraud scoring) without touching the producer
- **Buffering**: if the sink is slow or down, messages wait in the broker instead of being lost
- **Independent scaling**: each service is its own process

### Why only one service writes to DuckDB

DuckDB lets only one process write to a database file at a time. Instead of working around that, the design accepts it: the **sink is the only writer**, and every other service sends its results through a topic. Real systems often follow the same pattern, with a single dedicated loader in front of a warehouse.

## Two Brokers, One Interface

```python
class Broker(Protocol):                      # pipeline/broker/base.py
    def publish(self, topic, data, **attributes) -> str: ...
    def pull(self, subscription, max_messages) -> list[ReceivedMessage]: ...
    def ack(self, subscription, ack_ids): ...
    def nack(self, subscription, ack_ids): ...
```

| | `LocalBroker` (default) | `PubSubBroker` |
|---|---|---|
| Backed by | One SQLite file (`state/broker.sqlite3`) | Google Cloud Pub/Sub |
| Setup | None | Emulator (Docker) or a GCP project |
| Dead letters | ✅ enforced | ✅ real Pub/Sub · ❌ not enforced by the emulator |
| Multi-process | ✅ (SQLite WAL mode) | ✅ |
| Inspect backlog | `python -m pipeline stats` | Cloud Console / Metrics |

`LocalBroker` is about 250 lines of commented Python and SQL. It is meant to be read: it shows exactly what a broker does when it publishes, leases, acks, redelivers and dead-letters a message.

## Running It

### Option 1: one command (recommended first)

```bash
pip install -r requirements.txt
python run_pipeline.py                        # 60s, ~25 events/sec
python run_pipeline.py --duration 120 --rate 50
```

All services run as threads. After the traffic stops, they shut down **in dependency order** (upstream first, so nothing is lost), and a report is printed.

### Option 2: one terminal per service (how it runs in production)

```bash
python -m pipeline setup          # create topics + subscriptions
python -m pipeline sink           # terminal 1
python -m pipeline aggregator     # terminal 2
python -m pipeline alerts         # terminal 3
python -m pipeline validator      # terminal 4
python -m pipeline producer       # terminal 5
python -m pipeline stats          # terminal 6: watch the backlogs
python -m pipeline report         # any time: what's in DuckDB
```

Things to try while it runs:

1. **Stop the sink** (Ctrl+C) and watch `stats`. The `sink-*` backlogs grow, but nothing is lost. Start it again and it catches up. That is buffering.
2. **Kill the validator with `kill -9`** while it processes. Its leased messages are redelivered after the 10s ack deadline (`redelivered` column). Bronze still has no duplicates. That is at-least-once delivery plus idempotency.
3. **Start a second validator.** The two compete for messages from the same subscription. That is horizontal scaling.
4. **Change `allowed_lateness_seconds`** in `config/pipeline.yaml` to 0 or 60, rerun, and compare `gold.late_events` with the window latency.

### Option 3: the Google Pub/Sub emulator

```bash
pip install -r requirements-pubsub.txt
docker compose up -d                          # starts the emulator on :8085
export PUBSUB_EMULATOR_HOST=localhost:8085
BROKER=pubsub python run_pipeline.py
```

To use a real GCP project instead, unset `PUBSUB_EMULATOR_HOST`, run `gcloud auth application-default login`, and set `PUBSUB_PROJECT_ID`.

### Tests

```bash
python -m pytest -q
```

36 tests specify the broker semantics (fan-out, redelivery, DLQ), the contracts, windowing, sink idempotency, and an end-to-end run that checks every number adds up.

## Data Model (DuckDB)

| Layer | Table / view | What it holds |
|---|---|---|
| bronze | `bronze.events` | Every valid event once, deduplicated on `event_id` |
| quarantine | `quarantine.invalid_events` | Contract violations, with the raw payload and the reason |
| quarantine | `quarantine.dead_letters` | Messages a subscriber failed to process 5 times |
| silver | `silver.orders`, `silver.order_items`, `silver.payments` | Typed business entities parsed from JSON |
| gold | `gold.window_metrics` | Real-time KPIs per 10-second window (from the aggregator) |
| gold | `gold.late_events` | Events that arrived after their window closed |
| gold | `gold.conversion_funnel`, `gold.revenue_by_category`, `gold.channel_mix` | Business answers |
| gold | `gold.ingestion_latency` | p50/p95/p99 seconds from event time to DuckDB |
| gold | `gold.window_reconciliation` | Streaming results compared with a batch recomputation |
| ops | `ops.alerts`, `ops.pipeline_health` | Alerts raised, row counts |

Explore the data with the DuckDB CLI or Python:

```bash
duckdb duckdb/ecommerce.duckdb "SELECT * FROM gold.conversion_funnel"
```

## Sample Results (60s run, seed 42)

```
PRODUCER                          DUCKDB
  published      1543               bronze.events              1474   ← = valid_unique: duplicates dropped
  duplicates       43               quarantine.invalid_events    26   ← = malformed: nothing silently lost
  late             79               gold.late_events             54   ← late beyond the 10s allowed lateness
  malformed        26               ops.alerts                   18
  valid_unique   1474

Stream vs batch reconciliation
  stream_events 1420 + late_events 54 = batch_events 1474   →   unexplained 0 ✓

Conversion funnel                    sessions  % of sessions
  page_viewed                           226       100.0
  product_viewed                        223        98.7
  add_to_cart                           108        47.8
  checkout_started                       67        29.6
  order_placed                           51        22.6
  payment_succeeded                      48        21.2

Ingestion latency (event_time → DuckDB)
  p50 0.21s   p95 6.43s   p99 35.05s   ← p99 is the "phone was offline" late events

Alerts
  high_value_order            16
  repeated_payment_failures    2     ← stateful rule caught the simulated card testers
```

The producer is seeded, so the same number of events always produces the same data. Late-event and window counts depend on wall-clock timing, so they vary slightly from run to run.

## Project Structure

```
02-ecommerce-realtime-pipeline/
├── config/
│   ├── pipeline.yaml           # topology: topics, subscriptions, service settings
│   └── event_contracts.yaml    # what a valid event looks like + schema evolution
├── pipeline/
│   ├── broker/
│   │   ├── base.py             # the Broker interface
│   │   ├── local.py            # SQLite-backed Pub/Sub clone
│   │   └── pubsub.py           # Google Cloud Pub/Sub adapter
│   ├── services/
│   │   ├── base.py             # pull → process → ack loop, graceful shutdown
│   │   ├── producer.py         # storefront simulator + chaos injection
│   │   ├── validator.py        # contracts → clean-events | invalid-events
│   │   ├── aggregator.py       # event-time windows → window-results
│   │   ├── alerts.py           # rules → alerts
│   │   └── sink.py             # micro-batched, idempotent DuckDB writer
│   ├── contracts.py            # validation + upcasting
│   ├── windowing.py            # tumbling windows, watermark, dedup cache
│   ├── report.py               # summary printed after a run
│   └── __main__.py             # `python -m pipeline <service>`
├── sql/
│   ├── tables.sql              # sink tables (bronze / quarantine / gold / ops)
│   └── models.sql              # silver + gold views
├── tests/                      # pytest suite
├── run_pipeline.py             # one-command demo
├── docker-compose.yml          # optional Pub/Sub emulator
├── STREAMING_CONCEPTS.md       # the theory, in depth
└── README.md
```

## Design Philosophy

- **Read the machinery.** The broker, the windowing and the sink are small enough to read in one sitting. No framework hides the concepts.
- **Config is the contract.** Topology and event schemas live in YAML that every service reads.
- **Inject the mess.** Duplicates, late events, malformed payloads and old schema versions are generated on purpose, because handling them is the real job.
- **Prove it.** The reconciliation view and the end-to-end test check that every event is accounted for.

## What You'd Add for Production

- **Fault-tolerant state.** Checkpoint the aggregator's windows together with its read position (Flink, Dataflow and Spark do this), or keep state in an external store.
- **Schema registry.** Use Avro or Protobuf schemas with compatibility checks (Pub/Sub schemas, Confluent Schema Registry) instead of YAML.
- **Ordering.** Use Pub/Sub ordering keys (for example per `session_id`) where order matters.
- **Exactly-once.** Pub/Sub exactly-once delivery, or transactional sinks.
- **Observability.** Track backlog and oldest-unacked-age metrics with alerting, and add distributed tracing.
- **Replay.** Seek a subscription to a timestamp or snapshot to reprocess after a bug fix.
- **Warehouse.** Point the sink at BigQuery or Snowflake (or MotherDuck for hosted DuckDB).

## Technologies

- **Python 3.10+**: services and orchestration
- **SQLite**: local message broker (standard library, no install)
- **DuckDB**: analytical sink and SQL models
- **Google Cloud Pub/Sub**: optional, through the emulator or GCP
- **YAML**: topology and data contracts
- **pytest**: tests

## License

MIT. See the LICENSE file.
