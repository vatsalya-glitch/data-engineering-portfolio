# 04 - Real-Time Payment Monitor (learn Kafka step by step)

A small payment-monitoring system built one Kafka idea at a time. Fake card
payments go into Kafka, consumers read them, and by the end we flag cards with
too many transactions in a 10-minute window and send alerts to a second topic.

**Tech:** Python 3, [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python),
Apache Kafka 4.1 (single broker, KRaft mode, no ZooKeeper) via Docker Compose.
No stream-processing frameworks: just plain producers and consumers.

## Setup (once)

```bash
cd 04-kafka-payment-monitor
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d        # one Kafka broker, shared by every lesson
```

## Lessons

Each folder has a `README.md` (concept, commands, expected output, experiment)
and a few short, heavily commented scripts.

| # | Folder | You learn |
|---|---|---|
| 1 | [lesson_01_setup](./lesson_01_setup) | Broker, cluster, controller, KRaft. Start Kafka and check it's healthy |
| 2 | lesson_02_topics | Topics and partitions. Produce and consume with the CLI tools |
| 3 | lesson_03_producer | A Python producer sending JSON payment events |
| 4 | lesson_04_consumer | A Python consumer that reads and prints events |
| 5 | lesson_05_keys_partitions | Message keys, and how `card_id` decides the partition |
| 6 | lesson_06_offsets | Offsets, auto vs manual commit, and restarting a consumer |
| 7 | lesson_07_consumer_groups | Splitting work in one group vs. every group getting all messages |
| 8 | lesson_08_duplicates | At-least-once delivery, and deduplicating by `transaction_id` |
| 9 | lesson_09_event_time_windows | Event time, 10-minute windows, and late events |
| 10 | lesson_10_alerts_chaining | Publishing alerts to a second topic |
| 11 | lesson_11_consumer_lag | Measuring consumer lag with `kafka-consumer-groups` |

Lessons are added one at a time. Unlinked rows are not written yet.

## Layout

```
04-kafka-payment-monitor/
├── docker-compose.yml     # the single Kafka broker (shared by all lessons)
├── requirements.txt
└── lesson_XX_.../         # README.md + scripts, run from this folder
```
