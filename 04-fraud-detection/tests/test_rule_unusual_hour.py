import pandas as pd

from conftest import START, make_history, make_txn
from fraud.rules import unusual_hour

NIGHT = START.normalize() + pd.Timedelta(days=30, hours=3, minutes=15)


def test_flags_night_transaction_for_day_account(account):
    flagged, reason = unusual_hour(make_txn(timestamp=NIGHT), account, make_history())
    assert flagged
    assert "03:15" in reason


def test_daytime_is_not_flagged(account):
    assert unusual_hour(make_txn(), account, make_history()) == (False, "")


def test_window_ends_at_five(account):
    txn = make_txn(timestamp=START.normalize() + pd.Timedelta(days=30, hours=5))
    assert unusual_hour(txn, account, make_history()) == (False, "")


def test_night_is_usual_for_night_owl(account):
    history = make_history(n=10, start=NIGHT - pd.Timedelta(days=20))
    assert unusual_hour(make_txn(timestamp=NIGHT), account, history) == (False, "")


def test_quiet_without_enough_history(account):
    assert unusual_hour(make_txn(timestamp=NIGHT), account, make_history(n=4)) == (False, "")
