"""
SERVICE RUNTIME: the pull -> process -> ack loop every consumer shares

    while running:
        for each subscription:
            messages = pull()
            process(messages)      # the service decides what to ack / nack
        tick()                     # timers: flush buffers, emit windows ...
    close()                        # graceful shutdown: flush, then stop

Acking is the service's job because WHEN you ack defines your guarantee:

    ack before processing  -> at-most-once  (crash = message lost)
    ack after processing   -> at-least-once (crash = message redelivered)

Every service here acks AFTER its side effect, so duplicates are possible and
every downstream step is written to be idempotent.
"""

import logging
import signal
import threading

from pipeline.broker.base import Broker, ReceivedMessage


class Service:
    name = "service"
    subscriptions: list[str] = []

    def __init__(self, broker: Broker):
        self.broker = broker
        self.log = logging.getLogger(self.name)
        self.processed = 0
        self.failed = 0

    def process(self, subscription: str, messages: list[ReceivedMessage]) -> None:
        """Default: handle each message on its own, ack successes, nack failures."""
        acks, nacks = [], []
        for msg in messages:
            try:
                self.handle(msg)
                acks.append(msg.ack_id)
                self.processed += 1
            except Exception:
                self.log.exception("failed message %s (attempt %d) - nacking",
                                   msg.message_id, msg.delivery_attempt)
                nacks.append(msg.ack_id)
                self.failed += 1
        self.broker.ack(subscription, acks)
        self.broker.nack(subscription, nacks)

    def handle(self, msg: ReceivedMessage) -> None:
        raise NotImplementedError

    def tick(self) -> None:
        """Called on every loop iteration, with or without messages."""

    def close(self) -> None:
        """Called once on shutdown."""


def run_service(service: Service, stop: threading.Event,
                poll_interval: float = 0.2, max_messages: int = 200) -> None:
    service.log.info("started - subscriptions: %s", ", ".join(service.subscriptions))
    try:
        while not stop.is_set():
            received = 0
            for subscription in service.subscriptions:
                messages = service.broker.pull(subscription, max_messages=max_messages)
                if messages:
                    service.process(subscription, messages)
                    received += len(messages)
            service.tick()
            if not received:
                stop.wait(poll_interval)   # idle: back off instead of spinning
    finally:
        service.close()
        service.log.info("stopped - processed=%d failed=%d", service.processed, service.failed)


def stop_on_signals() -> threading.Event:
    """Ctrl+C / SIGTERM -> set an Event so loops can finish their batch cleanly."""
    stop = threading.Event()

    def _handler(signum, frame):
        logging.getLogger("runtime").info("signal %s received - shutting down gracefully", signum)
        stop.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
    return stop


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-10s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("google").setLevel(logging.WARNING)
