"""
THE BROKER INTERFACE

Every service in this project depends on this small interface - never on a
concrete broker. That single decision is what makes "local-first" possible:

  - LocalBroker  (local.py)  - SQLite file, zero setup, runs offline
  - PubSubBroker (pubsub.py) - Google Cloud Pub/Sub or its emulator

Swap one for the other with BROKER=pubsub and no service code changes.

The method names deliberately mirror Google Pub/Sub so the concepts carry over:
topics, subscriptions, publish, pull, ack, nack, ack deadlines, dead letters.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True)
class ReceivedMessage:
    """A message handed to a subscriber. Must be acked or nacked."""

    ack_id: str                 # handle used to ack/nack THIS delivery
    message_id: str             # broker-assigned id, stable across redeliveries
    data: bytes
    attributes: dict = field(default_factory=dict)
    publish_time: datetime | None = None
    delivery_attempt: int = 1   # 1 on first delivery, 2+ on redelivery


class Broker(Protocol):
    def create_topic(self, topic: str) -> None:
        """Create a topic. No-op if it exists."""

    def create_subscription(
        self,
        subscription: str,
        topic: str,
        ack_deadline_seconds: int = 10,
        dead_letter_topic: str | None = None,
        max_delivery_attempts: int = 5,
    ) -> None:
        """Attach a subscription to a topic. No-op if it exists.

        Only messages published AFTER the subscription exists are delivered
        to it - exactly like real Pub/Sub.
        """

    def publish(self, topic: str, data: bytes, **attributes: str) -> str:
        """Publish one message. Returns the broker-assigned message_id."""

    def pull(self, subscription: str, max_messages: int = 100) -> list[ReceivedMessage]:
        """Return up to max_messages messages (possibly none). Never blocks long."""

    def ack(self, subscription: str, ack_ids: list[str]) -> None:
        """Processing succeeded - never deliver these again."""

    def nack(self, subscription: str, ack_ids: list[str]) -> None:
        """Processing failed - make these available for redelivery now."""

    def close(self) -> None:
        """Release connections."""
