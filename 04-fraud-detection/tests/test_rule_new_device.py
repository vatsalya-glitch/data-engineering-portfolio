import pandas as pd

from conftest import START, make_history, make_txn
from fraud.rules import new_device


def test_flags_device_never_seen(account):
    flagged, reason = new_device(make_txn(device_id="ATTACKER"), account, make_history())
    assert flagged
    assert "never used" in reason


def test_known_device_is_not_flagged(account):
    assert new_device(make_txn(device_id="PHONE"), account, make_history()) == (False, "")


def test_device_first_seen_within_window_is_still_new(account):
    # The attacker's second transaction: device seen once, 3 hours earlier.
    history = pd.concat([make_history(),
                         make_history(n=1, device_id="ATTACKER", start=START + pd.Timedelta(days=30))])
    txn = make_txn(device_id="ATTACKER", timestamp=START + pd.Timedelta(days=30, hours=3))
    flagged, reason = new_device(txn, account, history)
    assert flagged
    assert "3h ago" in reason


def test_device_older_than_window_is_not_new(account):
    history = pd.concat([make_history(),
                         make_history(n=1, device_id="NEW_PHONE", start=START + pd.Timedelta(days=28))])
    txn = make_txn(device_id="NEW_PHONE", timestamp=START + pd.Timedelta(days=30))
    assert new_device(txn, account, history) == (False, "")


def test_quiet_without_enough_history(account):
    assert new_device(make_txn(device_id="ATTACKER"), account, make_history(n=4)) == (False, "")
