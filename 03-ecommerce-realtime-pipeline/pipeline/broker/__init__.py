"""Broker factory: pick an implementation from config or the BROKER env var."""

import os

from pipeline.config import PROJECT_ROOT, load_pipeline_config


def broker_type(override: str | None = None) -> str:
    cfg = load_pipeline_config()
    return override or os.environ.get("BROKER") or cfg["broker"]["type"]


def get_broker(override: str | None = None):
    cfg = load_pipeline_config()
    kind = broker_type(override)

    if kind == "local":
        from pipeline.broker.local import LocalBroker
        return LocalBroker(PROJECT_ROOT / cfg["broker"]["local"]["path"])

    if kind == "pubsub":
        # Imported lazily so the google-cloud-pubsub package stays optional.
        from pipeline.broker.pubsub import PubSubBroker
        project_id = os.environ.get("PUBSUB_PROJECT_ID") or cfg["broker"]["pubsub"]["project_id"]
        return PubSubBroker(project_id)

    raise ValueError(f"Unknown broker type: {kind!r} (expected 'local' or 'pubsub')")


def setup_topology(broker) -> None:
    """Create every topic and subscription declared in config/pipeline.yaml."""
    cfg = load_pipeline_config()
    for topic in cfg["topics"]:
        broker.create_topic(topic)
    for name, sub in cfg["subscriptions"].items():
        broker.create_subscription(
            name,
            sub["topic"],
            ack_deadline_seconds=sub.get("ack_deadline_seconds", 10),
            dead_letter_topic=sub.get("dead_letter_topic"),
            max_delivery_attempts=sub.get("max_delivery_attempts", 5),
        )
