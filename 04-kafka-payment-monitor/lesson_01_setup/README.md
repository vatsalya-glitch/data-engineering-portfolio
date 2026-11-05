# Lesson 1: Start Kafka and check it's running

## 1. The concept

**Kafka** is a system for moving *events* between programs as they happen. An
**event** is a small record of something that occurred: "card 42 paid $18.50
at 10:03". Programs that write events are **producers**. Programs that read
them are **consumers**. Kafka sits in the middle and keeps the events.

The server that holds the events is called a **broker**. Real companies run
several brokers together as a **cluster**, so the data survives if one machine
dies. We run one broker, which is enough to learn everything in this project.

**Analogy: a post office sorting room.** Producers drop letters off at the
counter. The sorting room (the broker) files them into the right pigeonholes.
Consumers come by and pick up their mail. The sender and the reader never need
to be in the room at the same time, and the post office keeps the letters even
after someone has picked them up. That last part matters: Kafka keeps events
after they are read, and Pub/Sub, which you already know, doesn't. We come
back to this in Lesson 6.

A cluster also needs someone in charge: tracking which brokers are alive and
who owns what. That job is the **controller**. Older Kafka used a separate
program called ZooKeeper for this. Newer Kafka handles it itself, in a mode
called **KRaft** (Kafka + Raft, Raft being the voting method the controllers
use to agree). In our `docker-compose.yml`, one container does both jobs:
`KAFKA_PROCESS_ROLES: broker,controller`.

**Coming from BigQuery + Pub/Sub:**

| You know | Kafka equivalent |
|---|---|
| Pub/Sub topic | Kafka topic (Lesson 2) |
| Publisher | Producer |
| Subscription | Consumer group (Lesson 7) |
| Google runs the servers | *You* run the broker (this lesson) |

## 2. Commands to run, in order

Run everything from the project folder `04-kafka-payment-monitor/`.

```bash
# (once) Create a virtual environment and install the Kafka client library
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# a) Check the script fails cleanly while Kafka is NOT running yet
python lesson_01_setup/check_kafka.py

# b) Start Kafka in the background (-d = "detached")
docker compose up -d

# c) Wait until STATUS says "(healthy)". Run it again if it says "starting".
docker compose ps

# d) Look at the broker's own log. Ctrl+C to stop watching.
docker compose logs -f kafka

# e) Ask Kafka about itself from Python
python lesson_01_setup/check_kafka.py

# f) Ask the same thing using Kafka's own CLI tool, inside the container
docker exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | head -1
```

To stop Kafka later: `docker compose stop` (keeps data), or
`docker compose down` (deletes the container *and all its events*).

## 3. What you should see, and why

**a) Before starting Kafka:**
```
Could not reach Kafka at localhost:9092.
Is the container running? Try:  docker compose ps
Details: KafkaError{code=_TRANSPORT,val=-195,str="Failed to get metadata: Local: Broker transport failure"}
```
"Transport failure" means the network connection failed: nothing is listening on
port 9092 yet. You'll see this error again whenever Kafka is down, so it's
worth knowing what it looks like.

**c) `docker compose ps`:**
```
NAME    IMAGE               ...  STATUS                  PORTS
kafka   apache/kafka:4.1.0  ...  Up 9 seconds (healthy)  0.0.0.0:9092->9092/tcp
```
"Up" means the container is running. "(healthy)" means Docker also ran the
`healthcheck` in our compose file (a real Kafka request), and it worked. A
container can be up while Kafka inside it is still booting, so wait for
"healthy" before going on. The `PORTS` column shows port 9092 on your machine is
connected to port 9092 inside the container.

**d) The log** ends with lines like:
```
[BrokerServer id=1] Transition from STARTING to STARTED
[KafkaRaftServer nodeId=1] Kafka Server started
```
`KafkaRaftServer` confirms KRaft mode, with no ZooKeeper involved.

**e) The Python check:**
```
Connected to Kafka!
  Cluster ID : 5L6g3nShT-eMCtK--X86sw
  Controller : broker 1
  Brokers    : 1
    - id=1 at localhost:9092
  Topics     : (none yet, that is expected)
```
- **Cluster ID**: every cluster has an ID, and a broker refuses to join a
  cluster whose ID doesn't match its own. The apache/kafka image uses this
  fixed default ID, so yours will be the same. In production you generate
  a unique one.
- **Controller: broker 1**: our single broker is also the one in charge.
- **id=1 at localhost:9092**: this is the `ADVERTISED_LISTENERS` address from
  the compose file. The broker tells clients "reach me here". If that address
  were wrong, the first connection would work but the next ones would fail.
  This is the most common Kafka setup mistake.
- **No topics**: we turned off auto-creation (`AUTO_CREATE_TOPICS_ENABLE:
  "false"`), so topics only exist when we create them on purpose. Kafka's
  internal topics (names starting with `__`) are hidden by the script.

**f) The CLI check** prints `localhost:9092 (id: 1 rack: null isFenced: false) -> (`
followed by a long list of request types. **Fenced** would mean the controller
has blocked this broker, so `isFenced: false` means it's active.
`docker exec kafka <command>` runs a command *inside* the container. Kafka's
CLI tools live there in `/opt/kafka/bin/`. Lesson 2 uses them a lot.

## 4. Experiment on your own

Recreate the most common Kafka setup mistake: the broker advertises an
address the client can't reach.

1. In `docker-compose.yml`, change the port line to `"19092:9092"`. Your laptop
   now reaches the broker on port **19092**, but `ADVERTISED_LISTENERS` still
   says `localhost:9092`.
2. `docker compose up -d` (Docker sees the change and recreates the container).
3. In `check_kafka.py`, set `BOOTSTRAP = "localhost:19092"` and run it.

**Before you run it, guess:** will it connect? The first connection on 19092
works. But the broker replies "talk to me at localhost:9092", where nothing is
listening anymore. You'll get `Timed out` this time, not the
`Broker transport failure` from step (a). Compare the two errors: one means
"I couldn't reach anything", the other means "I reached it, but then
couldn't use it".

**Put everything back** (port `"9092:9092"`, `BOOTSTRAP = "localhost:9092"`,
`docker compose up -d`) before Lesson 2.
