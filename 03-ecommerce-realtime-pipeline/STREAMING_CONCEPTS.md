# Streaming Concepts: A Practical Guide

This guide explains the theory behind each part of this pipeline. Every section links back to the code that implements it, so you can read the idea and then see it working.

## Table of Contents
1. [Batch vs Streaming](#1-batch-vs-streaming)
2. [Publish/Subscribe Messaging](#2-publishsubscribe-messaging)
3. [Delivery Guarantees and Acknowledgements](#3-delivery-guarantees-and-acknowledgements)
4. [Idempotency: Making Duplicates Harmless](#4-idempotency-making-duplicates-harmless)
5. [Dead Letters and Quarantine](#5-dead-letters-and-quarantine)
6. [Data Contracts and Schema Evolution](#6-data-contracts-and-schema-evolution)
7. [Event Time, Windows and Watermarks](#7-event-time-windows-and-watermarks)
8. [Stateful Processing](#8-stateful-processing)
9. [Sinks: Micro-batching and Ack-after-Commit](#9-sinks-micro-batching-and-ack-after-commit)
10. [Stream vs Batch Reconciliation](#10-stream-vs-batch-reconciliation)
11. [Local-First Development](#11-local-first-development)
12. [Glossary](#12-glossary)

---

## 1. Batch vs Streaming

| | Batch | Streaming |
|---|---|---|
| Data | A bounded set (yesterday's orders) | An unbounded, never-ending flow |
| Runs | On a schedule (nightly) | Continuously |
| Latency | Minutes to hours | Milliseconds to seconds |
| Completeness | You know when you have it all | You never know for sure |
| Re-running | Easy: run the job again | Hard: state, ordering and side effects |

The last two rows are what make streaming hard. In batch you wait until the data is complete and then compute. In streaming you compute as data arrives, so you have to decide **when a result is good enough to publish** (see [watermarks](#7-event-time-windows-and-watermarks)) and **what happens when the same input is processed twice** (see [idempotency](#4-idempotency-making-duplicates-harmless)).

**Rule of thumb:** use streaming when someone acts on the result within seconds (fraud alerts, live dashboards, inventory). For everything else, batch is simpler and cheaper.

---

## 2. Publish/Subscribe Messaging

### The model

```
producer ──publish──► TOPIC ──► subscription A ──pull──► consumer A1, A2 (compete)
                          └───► subscription B ──pull──► consumer B1
```

- A **topic** is a named stream that producers publish messages to.
- A **subscription** is a named cursor on a topic. **Each subscription receives every message** (fan-out).
- Consumers that share **one subscription share the work** (competing consumers). Each message goes to only one of them.

In this project `clean-events` has three subscriptions (`sink-events-sub`, `aggregator-sub`, `alerts-sub`). All three receive every event, and each handles it independently.

### Why not just call functions?

| Direct calls | Pub/Sub |
|---|---|
| If the sink is down, the validator fails | If the sink is down, messages wait in the backlog |
| Adding a consumer means changing the producer | Adding a consumer means creating a subscription |
| The slowest step sets the pace | Each service runs at its own speed |

### A subtlety: subscriptions only see the future

A subscription receives only messages published **after it was created**. That is why `run_pipeline.py` creates the topology and starts consumers *before* the producer. The test `test_subscription_only_sees_messages_published_after_it_exists` checks this behaviour.

### See it in code

`pipeline/broker/local.py` implements all of this with two tables:

```sql
messages   (message_id, topic, data, attributes, publish_time)   -- the log
deliveries (subscription, message_id, delivery_attempt, visible_at)  -- per-sub state
```

Publishing inserts one row into `messages` and **one row into `deliveries` per subscription**. That is the fan-out.

---

## 3. Delivery Guarantees and Acknowledgements

### The three guarantees

| Guarantee | Meaning | How you get it |
|---|---|---|
| **At-most-once** | Never duplicated, possibly lost | Ack *before* processing |
| **At-least-once** | Never lost, possibly duplicated | Ack *after* processing |
| **Exactly-once** | Never lost, never duplicated | At-least-once + idempotency (or transactions) |

Pub/Sub (like Kafka, SQS and most brokers) is **at-least-once by default**. This pipeline acks after processing, everywhere.

### The lease and ack cycle

```
pull()   → message is LEASED: hidden from other consumers until the ack deadline
ack()    → done, never delivered again
nack()   → "I failed", redeliver now
(nothing before the deadline) → consumer presumed dead, redeliver
```

In `LocalBroker.pull`, leasing is a single UPDATE:

```sql
UPDATE deliveries SET delivery_attempt = delivery_attempt + 1,
                      visible_at = now + ack_deadline
```

And "presumed dead" needs no extra code: a row whose `visible_at` has passed simply matches the next pull's `WHERE visible_at <= now`.

### Why duplicates are unavoidable

```
validator: pull → validate → publish to clean-events → 💥 crash → (ack never sent)
broker:    ack deadline passes → redeliver → validator publishes AGAIN
```

The broker can't tell whether the work happened. **Duplicates are a normal part of at-least-once systems, not a bug.** The producer also creates them on purpose (`duplicate_rate`), to simulate a client retrying after a timeout.

### Choosing the ack deadline

- Too short: slow messages get redelivered while they are still being processed, which wastes work and creates duplicates.
- Too long: a crashed consumer's messages sit idle until the deadline expires.

The sink uses 30 seconds because it holds messages for up to `flush_interval_seconds` (2s) before acking. The validator uses 10 seconds because it acks right away.

---

## 4. Idempotency: Making Duplicates Harmless

An operation is **idempotent** if doing it twice has the same effect as doing it once.

```
SET balance = 100           ✅ idempotent
SET balance = balance + 10  ❌ not idempotent
```

### Rule: every write uses a natural key from the data

```sql
-- sql/tables.sql
CREATE TABLE bronze.events (event_id VARCHAR PRIMARY KEY, ...);

-- pipeline/services/sink.py
INSERT INTO bronze.events VALUES (...) ON CONFLICT DO NOTHING;
```

| Table | Idempotency key | On conflict |
|---|---|---|
| `bronze.events` | `event_id` (set by the producer) | do nothing |
| `quarantine.invalid_events` | `source_message_id` | do nothing |
| `gold.window_metrics` | `window_start` | replace (latest result wins) |
| `ops.alerts` | `alert_type:event_id` (derived, deterministic) | do nothing |

**Never use auto-increment ids or `now()` as the key.** A retry would produce a different key, so the "duplicate" would look like a new row.

### Counting needs deduplication too

A sink can use `ON CONFLICT`, but an aggregator that adds `+1` per event can't. So the aggregator and the alerts service keep a **`DedupCache`** (`pipeline/windowing.py`): a bounded LRU of recently seen `event_id`s. It is bounded because unbounded state eventually runs out of memory. The trade-off is that a duplicate arriving after its id has been evicted gets counted. Size the cache to cover your realistic redelivery window.

---

## 5. Dead Letters and Quarantine

This pipeline handles two different kinds of bad message:

| | Quarantine (`invalid-events`) | Dead letter (`dead-letter`) |
|---|---|---|
| Problem | The **data** is bad | The **processing** keeps failing |
| Example | Negative order total, unknown event type | Bug in the sink, poison payload that crashes code |
| Detected by | Your validation logic, on the first attempt | The broker, after N failed attempts |
| Retrying helps? | Never | Sometimes (transient errors) |
| Configured in | `validator.py` | Subscription `dead_letter_topic` + `max_delivery_attempts` |

### Why not just drop bad data?

Dropped data is invisible. If 3% of orders silently vanish, revenue reports are wrong and nobody knows why. Quarantined data can be **counted** (alert when the rate spikes), **inspected** (`SELECT error, raw FROM quarantine.invalid_events`), and **replayed** after the producer is fixed.

### How the DLQ works (local broker)

```python
# pipeline/broker/local.py - inside pull()
if dlq_topic and attempts >= max_attempts:
    self._publish_in_txn(conn, dlq_topic, data, attributes)   # park it
    self._delete(conn, subscription, [message_id])            # stop retrying
```

Without a DLQ, one poison message is retried forever. It burns CPU, fills logs with the same error, and with ordered delivery it blocks everything behind it.

---

## 6. Data Contracts and Schema Evolution

### Contract = the schema plus the rules

`config/event_contracts.yaml` defines the envelope every event shares (`event_id`, `event_time`, ...), the payload fields per `event_type`, and business rules (`order_total >= 0`, `items` not empty). The validator is the **only** consumer of `raw-events`. Everything downstream reads `clean-events` and can trust it.

### Schema evolution

Producers can't all be upgraded at once. Old mobile app versions keep sending old event shapes for months. This project has two versions:

```
v1: { event_id, event_type, event_time, session_id, user_id, payload }
v2: v1 + channel ("web" | "ios" | "android")
```

The validator **upcasts** v1 events to v2 by filling defaults (`channel: unknown`) and records `source_schema_version: 1`. Downstream code only knows about v2. You can see the effect in `gold.channel_mix` under the `unknown` channel.

| Change | Safe? | Why |
|---|---|---|
| Add an optional field | ✅ Backward compatible | Old events just lack it, so fill a default |
| Remove a field consumers use | ❌ | Consumers break |
| Rename a field | ❌ | Equivalent to remove plus add |
| Change a field's type | ❌ | Parsing breaks |

In production, a **schema registry** (Pub/Sub schemas, Confluent Schema Registry) enforces these compatibility rules automatically when producers publish.

---

## 7. Event Time, Windows and Watermarks

This section covers the central concept in stream processing.

### Two clocks

- **Event time:** when the thing happened (`event_time`, set by the client).
- **Processing time:** when the pipeline sees it.

They differ because of network delay, retries, and phones that were offline. The producer simulates this with `late_rate` and `late_delay_seconds`.

**Always aggregate on event time.** If you aggregate on processing time, a network blip moves sales into the wrong minute, and replaying data gives different answers.

### Tumbling windows

Fixed-size, non-overlapping intervals aligned to the clock:

```
|  12:00:00  |  12:00:10  |  12:00:20  |  ...
   [0, 10)      [10, 20)     [20, 30)
```

`TumblingWindows.window_start()` floors the event time to the window size. Other window types you should know: **sliding/hopping** windows (overlapping, for example a 1-minute window every 10s) and **session** windows (closed by a gap in activity).

### The completeness problem

When can we publish the result for `[12:00:00, 12:00:10)`? An event from that window could still arrive at any moment, so we can never be sure. We have to make a bet.

### Watermarks: the bet

```
watermark = max(event_time seen) − allowed_lateness
```

The watermark says "I don't expect any more events older than this." When the watermark passes a window's end, the window is **closed**: its result is emitted and its state is freed.

```
event time ─────────────────────────────────────────────────►
  [ w1 ][ w2 ][ w3 ][ w4 ][ w5 ]
              ▲                 ▲
          watermark        max event time
       ◄── closed │ open ──►   (allowed_lateness apart)
```

### Late data

An event whose window has already closed is **late**. You have three options:

1. **Drop it.** Simple, but numbers are quietly too low.
2. **Side output.** Emit it somewhere else for later correction. *This project does this* (`gold.late_events`).
3. **Update the window.** Re-emit a corrected result. Downstream must handle updates (the upsert on `window_start` would allow it).

### The fundamental trade-off

| `allowed_lateness` | Results appear | Completeness |
|---|---|---|
| 0s | Immediately | Many late events |
| 10s *(this project)* | ~10s behind | Most events counted |
| 60s | A minute behind | Almost everything counted |

**No setting gives you both low latency and full completeness.** Try changing the value in `config/pipeline.yaml` and compare `gold.late_events`.

### The idle-source problem

The watermark only moves forward when events arrive. If traffic stops, the last windows never close. This project emits remaining windows on shutdown (`TumblingWindows.pop_all`). Production engines add **idle timeouts** that advance the watermark using processing time.

---

## 8. Stateful Processing

**Stateless** operators look at one event at a time (validation, `high_value_order`). **Stateful** operators remember earlier events (windows, deduplication, `repeated_payment_failures`).

```python
# pipeline/services/alerts.py - "3 failed payments by one user within 5 minutes"
times = self.failures[user]          # state: recent failure timestamps per user
times.append(t)
while times and t - times[0] > self.burst_window:
    times.popleft()                  # expire old state
if len(times) >= self.burst: raise_alert()
```

State raises three questions you must answer:

1. **How big can it grow?** Expire it (time windows, LRU caches) or it grows forever.
2. **Where is it partitioned?** With several alert workers, all events for one user must reach the same worker. Brokers solve this with keys: Pub/Sub ordering keys, Kafka partition keys.
3. **What happens on a crash?** In this project the state is in memory, so a crash loses it. The aggregator has already acked the messages that built its open windows, so those contributions are gone.

### How production engines survive crashes

Flink, Dataflow and Spark Structured Streaming periodically take a **checkpoint**: a consistent snapshot of *all operator state* plus *the input position*, written atomically. On a crash they restore the snapshot and re-read the input from the saved position, which gives exactly-once *state* even over at-least-once delivery. Implementing this yourself is hard, and it is the main reason those engines exist.

---

## 9. Sinks: Micro-batching and Ack-after-Commit

### Micro-batching

One INSERT per message means one transaction per message, which is slow on any database and very slow on a columnar one like DuckDB. The sink buffers rows and flushes when **either** threshold is reached:

```yaml
flush_max_rows: 500           # big enough → throughput
flush_interval_seconds: 2.0   # old enough → bounded latency
```

This is the throughput-versus-latency trade-off again, at a smaller scale.

### Ack after commit

```python
con.execute("BEGIN TRANSACTION")
...insert all buffered rows...
con.execute("COMMIT")
# only now:
broker.ack(subscription, ack_ids)
```

| Crash point | Result |
|---|---|
| Before COMMIT | Transaction rolls back, messages are redelivered and written later ✅ |
| After COMMIT, before ack | Messages are redelivered and written again, `ON CONFLICT` makes that a no-op ✅ |

The test `test_sink_acks_only_after_flush` checks that the backlog stays non-zero until the data is durable.

### Graceful shutdown

On Ctrl+C the services finish their current batch, flush, ack, and only then exit (`stop_on_signals` → `Service.close`). `run_pipeline.py` stops services in **topological order** (validator → aggregator/alerts → sink), so every downstream service is still running while its inputs drain.

---

## 10. Stream vs Batch Reconciliation

How do you know a streaming pipeline is correct? **Recompute the same numbers in batch and compare.**

`gold.window_reconciliation` recomputes per-window event counts from `bronze.events` and compares them with what the aggregator emitted in real time:

```
batch_events = stream_events + late_events + unexplained
```

- `late_events` explains the expected differences: events that arrived after their window closed.
- `unexplained` should be **0**. If it isn't, events were lost or double counted, and you have a bug.

The end-to-end test asserts `unexplained == 0`. This pattern (a fast streaming path checked by a correct batch path) is the core idea of the **Lambda architecture**. The **Kappa architecture** instead replays the stream itself to recompute.

---

## 11. Local-First Development

### Ports and adapters

Services depend on the `Broker` *interface* (`pipeline/broker/base.py`), never on a vendor SDK. Adapters plug in behind it:

```
           ┌──────────────── Broker interface ────────────────┐
 services ─┤  LocalBroker (SQLite)  │  PubSubBroker (GCP SDK) │
           └──────────────────────────────────────────────────┘
```

### Why it matters

| Cloud-only development | Local-first development |
|---|---|
| Every test run needs credentials and network | Runs offline, on a plane |
| Shared dev environments conflict | Each developer has their own broker file |
| Tests are slow and cost money | 40 tests in seconds, free |
| Hard to simulate crashes or time | `FakeClock` fast-forwards ack deadlines in tests |

### The three-step ladder

1. **`LocalBroker`**: fastest feedback, fully inspectable (`sqlite3 state/broker.sqlite3`)
2. **Pub/Sub emulator**: the real client library and API, still local and free
3. **Real Pub/Sub**: the same code, with IAM, quotas, DLQ enforcement and scaling

Be honest about the gaps between the steps. The emulator does not enforce dead-letter policies, and neither local option shows you real network latency or quota limits. Local-first reduces the number of things to test in the cloud. It doesn't remove them.

---

## 12. Glossary

| Term | Meaning |
|---|---|
| **Ack / nack** | Tell the broker a message succeeded or failed |
| **Ack deadline** | How long a consumer holds a message before it is redelivered |
| **Backlog** | Messages published but not yet acked on a subscription (the #1 health metric) |
| **Backpressure** | Slowing producers when consumers can't keep up (with a broker, the backlog absorbs it) |
| **Dead-letter queue** | Where messages go after too many failed delivery attempts |
| **Event time** | When an event happened in the real world |
| **Fan-out** | One message delivered to many independent subscriptions |
| **Idempotent** | Doing it twice has the same effect as doing it once |
| **Late event** | An event whose window had already closed when it arrived |
| **Processing time** | When the pipeline sees an event |
| **Upcasting** | Converting old-version events to the latest schema |
| **Watermark** | The pipeline's estimate that no events older than this remain |
| **Window** | A finite slice of an infinite stream to aggregate over |
