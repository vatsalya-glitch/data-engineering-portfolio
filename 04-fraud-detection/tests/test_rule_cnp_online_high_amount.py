from conftest import make_history, make_txn
from fraud.rules import cnp_online_high_amount


def cnp(**overrides):
    return make_txn(channel="ONLINE", card_present=False, ip_country="NL", **overrides)


def test_flags_online_cnp_over_500(account):
    flagged, reason = cnp_online_high_amount(cnp(amount=500.01), account, make_history())
    assert flagged
    assert "500.01 EUR" in reason


def test_exactly_500_is_not_flagged(account):
    assert cnp_online_high_amount(cnp(amount=500.0), account, make_history()) == (False, "")


def test_card_present_is_not_flagged(account):
    txn = make_txn(channel="ONLINE", card_present=True, amount=900.0)
    assert cnp_online_high_amount(txn, account, make_history()) == (False, "")


def test_other_channels_are_not_flagged(account):
    txn = make_txn(channel="TRANSFER", card_present=False, amount=900.0)
    assert cnp_online_high_amount(txn, account, make_history()) == (False, "")
