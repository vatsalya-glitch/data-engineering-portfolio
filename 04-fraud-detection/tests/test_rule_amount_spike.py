from conftest import make_history, make_txn
from fraud.rules import amount_spike


def test_flags_amount_over_five_times_average(account):
    flagged, reason = amount_spike(make_txn(amount=201.0), account, make_history(amount=40.0))
    assert flagged
    assert "5.0x" in reason and "40.00 EUR" in reason


def test_exactly_five_times_average_is_not_flagged(account):
    assert amount_spike(make_txn(amount=200.0), account, make_history(amount=40.0)) == (False, "")


def test_normal_amount_is_not_flagged(account):
    assert amount_spike(make_txn(amount=60.0), account, make_history(amount=40.0)) == (False, "")


def test_quiet_without_enough_history(account):
    assert amount_spike(make_txn(amount=10_000.0), account, make_history(n=4, amount=40.0)) == (False, "")
