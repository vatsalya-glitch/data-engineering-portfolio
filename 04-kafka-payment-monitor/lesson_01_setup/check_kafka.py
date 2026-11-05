"""
Lesson 1: Is Kafka alive?

This script connects to the broker the same way every later lesson will,
then asks it to describe itself. If this works, your setup is good.

Run from the project root:  python lesson_01_setup/check_kafka.py
"""
import sys

# AdminClient = a Kafka client for management tasks (list topics, create
# topics, describe the cluster). It does not send or read messages.
from confluent_kafka.admin import AdminClient
from confluent_kafka import KafkaException

# "bootstrap.servers" = the first address a client contacts. The broker
# answers with the full list of brokers in the cluster (here, just one).
# "log_level": 0 silences the library's own retry messages, so that if
# Kafka is down you see our short error below instead of 100 noisy lines.
BOOTSTRAP = "localhost:9092"
config = {"bootstrap.servers": BOOTSTRAP, "log_level": 0}
admin = AdminClient(config)

try:
    # Ask for the cluster's "metadata": brokers, topics, partitions.
    # timeout=5 means: give up after 5 seconds instead of waiting forever.
    metadata = admin.list_topics(timeout=5)
except KafkaException as err:
    print(f"Could not reach Kafka at {BOOTSTRAP}.")
    print("Is the container running? Try:  docker compose ps")
    print(f"Details: {err}")
    sys.exit(1)

print("Connected to Kafka!")
print(f"  Cluster ID : {metadata.cluster_id}")
print(f"  Controller : broker {metadata.controller_id}")

# metadata.brokers is a dict of {broker_id: BrokerMetadata}
print(f"  Brokers    : {len(metadata.brokers)}")
for broker in metadata.brokers.values():
    print(f"    - id={broker.id} at {broker.host}:{broker.port}")

# Kafka hides its internal topics (names starting with "__") from most
# tools. We filter them out too, so we only see topics we created.
user_topics = [t for t in metadata.topics if not t.startswith("__")]
print(f"  Topics     : {user_topics if user_topics else '(none yet, that is expected)'}")
