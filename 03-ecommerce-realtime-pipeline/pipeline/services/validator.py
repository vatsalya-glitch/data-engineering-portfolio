"""
VALIDATOR: raw-events -> clean-events | invalid-events

The gatekeeper. Producers are outside our control (old app versions, bugs,
bad clocks), so nothing downstream reads raw-events directly.

  valid   -> normalised (upcast to latest schema) and published to clean-events
  invalid -> original bytes + reason published to invalid-events

Invalid data is QUARANTINED, not dropped: someone can inspect it, fix the
producer, and replay it. Silently dropping bad data is how numbers go wrong
without anyone noticing.
"""

import json

from pipeline.contracts import ContractViolation, validate_event
from pipeline.events import decode, encode
from pipeline.services.base import Service


class Validator(Service):
    name = "validator"
    subscriptions = ["validator-sub"]

    def __init__(self, broker):
        super().__init__(broker)
        self.valid = 0
        self.invalid = 0

    def handle(self, msg) -> None:
        try:
            event = validate_event(decode(msg.data))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            self._quarantine(msg, f"unparseable: {exc}")
        except ContractViolation as exc:
            self._quarantine(msg, str(exc))
        else:
            # Attributes let subscribers filter/route without parsing the body.
            self.broker.publish(
                "clean-events", encode(event),
                event_type=event["event_type"], event_id=event["event_id"],
            )
            self.valid += 1

    def _quarantine(self, msg, reason: str) -> None:
        self.broker.publish(
            "invalid-events", msg.data,
            error=reason[:500], source_message_id=msg.message_id,
        )
        self.invalid += 1

    def close(self) -> None:
        self.log.info("valid=%d invalid=%d", self.valid, self.invalid)
