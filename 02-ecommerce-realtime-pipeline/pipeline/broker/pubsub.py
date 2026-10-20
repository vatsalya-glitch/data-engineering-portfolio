"""
GOOGLE CLOUD PUB/SUB ADAPTER

The same Broker interface, backed by real Pub/Sub. Point it at the emulator
for local development:

  gcloud beta emulators pubsub start --project=local-ecommerce
  export PUBSUB_EMULATOR_HOST=localhost:8085
  BROKER=pubsub python run_pipeline.py

The client libraries detect PUBSUB_EMULATOR_HOST automatically, so this exact
code also runs against a real GCP project once you remove that variable and
authenticate (gcloud auth application-default login).

Notes on differences from the local broker:
  - The emulator does not enforce dead-letter policies; real Pub/Sub does
    (and needs IAM permissions for its service account to publish to the DLQ).
  - Real Pub/Sub requires max_delivery_attempts between 5 and 100.
"""

from google.api_core import exceptions
from google.cloud import pubsub_v1

from pipeline.broker.base import ReceivedMessage

PULL_TIMEOUT_SECONDS = 1.0


class PubSubBroker:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()

    def _topic(self, topic: str) -> str:
        return self.publisher.topic_path(self.project_id, topic)

    def _sub(self, subscription: str) -> str:
        return self.subscriber.subscription_path(self.project_id, subscription)

    def create_topic(self, topic: str) -> None:
        try:
            self.publisher.create_topic(request={"name": self._topic(topic)})
        except exceptions.AlreadyExists:
            pass

    def create_subscription(self, subscription, topic, ack_deadline_seconds=10,
                            dead_letter_topic=None, max_delivery_attempts=5) -> None:
        request = {
            "name": self._sub(subscription),
            "topic": self._topic(topic),
            "ack_deadline_seconds": ack_deadline_seconds,
        }
        if dead_letter_topic:
            request["dead_letter_policy"] = {
                "dead_letter_topic": self._topic(dead_letter_topic),
                "max_delivery_attempts": max_delivery_attempts,
            }
        try:
            self.subscriber.create_subscription(request=request)
        except exceptions.AlreadyExists:
            pass

    def publish(self, topic: str, data: bytes, **attributes: str) -> str:
        # publish() batches in the background and returns a future;
        # .result() waits for the server to confirm (simple, not the fastest).
        return self.publisher.publish(self._topic(topic), data, **attributes).result()

    def pull(self, subscription: str, max_messages: int = 100) -> list[ReceivedMessage]:
        try:
            response = self.subscriber.pull(
                request={"subscription": self._sub(subscription), "max_messages": max_messages},
                timeout=PULL_TIMEOUT_SECONDS,
            )
        except (exceptions.DeadlineExceeded, exceptions.RetryError):
            return []   # nothing arrived within the timeout
        return [
            ReceivedMessage(
                ack_id=rm.ack_id,
                message_id=rm.message.message_id,
                data=rm.message.data,
                attributes=dict(rm.message.attributes),
                publish_time=rm.message.publish_time,
                # delivery_attempt is only populated when a DLQ policy exists.
                delivery_attempt=rm.delivery_attempt or 1,
            )
            for rm in response.received_messages
        ]

    def ack(self, subscription: str, ack_ids: list[str]) -> None:
        if ack_ids:
            self.subscriber.acknowledge(
                request={"subscription": self._sub(subscription), "ack_ids": ack_ids}
            )

    def nack(self, subscription: str, ack_ids: list[str]) -> None:
        # A nack is just "set my ack deadline to zero" - redeliver now.
        if ack_ids:
            self.subscriber.modify_ack_deadline(
                request={
                    "subscription": self._sub(subscription),
                    "ack_ids": ack_ids,
                    "ack_deadline_seconds": 0,
                }
            )

    def close(self) -> None:
        self.subscriber.close()
