from conftest import make_history, make_txn
from fraud.rules import ip_country_mismatch


def test_flags_foreign_ip(account):
    flagged, reason = ip_country_mismatch(make_txn(channel="ONLINE", ip_country="BR"), account, make_history())
    assert flagged
    assert "BR" in reason and "NL" in reason


def test_home_ip_is_not_flagged(account):
    assert ip_country_mismatch(make_txn(ip_country="NL"), account, make_history()) == (False, "")


def test_missing_ip_is_not_flagged(account):
    # Card-present transactions carry no IP; unknown is not evidence of fraud.
    assert ip_country_mismatch(make_txn(ip_country=None), account, make_history()) == (False, "")


def test_works_without_history(account):
    flagged, _ = ip_country_mismatch(make_txn(ip_country="US"), account, make_history(n=0))
    assert flagged
