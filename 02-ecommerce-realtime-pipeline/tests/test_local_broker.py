"""The local broker must behave like Pub/Sub - these tests are the spec."""

import pytest


@pytest.fixture
def topic(broker):
    broker.create_topic("orders")
    broker.create_subscription("billing", "orders", ack_deadline_seconds=10,
                               dead_letter_topic="dlq", max_delivery_attempts=3)
    broker.create_topic("dlq")
    broker.create_subscription("dlq-sub", "dlq")
    return "orders"


def test_publish_pull_ack(broker, topic):
    broker.publish(topic, b"hello", kind="greeting")

    [msg] = broker.pull("billing")
    assert msg.data == b"hello"
    assert msg.attributes == {"kind": "greeting"}
    assert msg.delivery_attempt == 1

    broker.ack("billing", [msg.ack_id])
    assert broker.pull("billing") == []


def test_fan_out_gives_each_subscription_its_own_copy(broker, topic):
    broker.create_subscription("analytics", topic)
    broker.publish(topic, b"order-1")

    assert [m.data for m in broker.pull("billing")] == [b"order-1"]
    assert [m.data for m in broker.pull("analytics")] == [b"order-1"]


def test_subscription_only_sees_messages_published_after_it_exists(broker, topic):
    broker.publish(topic, b"before")
    broker.create_subscription("late-joiner", topic)
    broker.publish(topic, b"after")

    assert [m.data for m in broker.pull("late-joiner")] == [b"after"]


def test_leased_message_is_hidden_until_ack_deadline(broker, topic, clock):
    broker.publish(topic, b"x")
    broker.pull("billing")

    clock.advance(9)
    assert broker.pull("billing") == []          # still leased


def test_unacked_message_is_redelivered_after_deadline(broker, topic, clock):
    broker.publish(topic, b"x")
    [first] = broker.pull("billing")

    clock.advance(11)                             # consumer "crashed", never acked
    [again] = broker.pull("billing")
    assert again.message_id == first.message_id   # same message ...
    assert again.delivery_attempt == 2            # ... delivered again: at-least-once


def test_nack_redelivers_immediately(broker, topic):
    broker.publish(topic, b"x")
    [msg] = broker.pull("billing")
    broker.nack("billing", [msg.ack_id])

    [again] = broker.pull("billing")
    assert again.delivery_attempt == 2


def test_poison_message_goes_to_dead_letter_topic(broker, topic):
    broker.publish(topic, b"poison")
    for _ in range(3):                            # max_delivery_attempts = 3
        [msg] = broker.pull("billing")
        broker.nack("billing", [msg.ack_id])

    assert broker.pull("billing") == []           # no 4th attempt ...
    [dead] = broker.pull("dlq-sub")               # ... parked in the DLQ instead
    assert dead.data == b"poison"
    assert dead.attributes["source_subscription"] == "billing"
    assert dead.attributes["delivery_attempts"] == "3"


def test_pull_respects_max_messages_and_order(broker, topic):
    for i in range(5):
        broker.publish(topic, str(i).encode())
    first = broker.pull("billing", max_messages=2)
    rest = broker.pull("billing", max_messages=10)
    assert [m.data for m in first + rest] == [b"0", b"1", b"2", b"3", b"4"]


def test_stats_report_backlog(broker, topic):
    broker.publish(topic, b"a")
    broker.publish(topic, b"b")
    [row] = [r for r in broker.stats() if r["subscription"] == "billing"]
    assert row["backlog"] == 2


def test_unknown_topic_and_subscription_raise(broker):
    with pytest.raises(ValueError):
        broker.publish("nope", b"x")
    with pytest.raises(ValueError):
        broker.pull("nope")
