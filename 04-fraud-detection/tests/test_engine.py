import pandas as pd
import pytest

from conftest import START, make_history, make_txn
from fraud.engine import flag, score_transactions


@pytest.fixture
def accounts(account):
    return pd.DataFrame([account])


def account_with_takeover() -> pd.DataFrame:
    """Ten normal days, then one attacker transaction at 03:00 from Brazil."""
    attack = make_txn(transaction_id="ATTACK", channel="ONLINE", card_present=False, amount=800.0,
                      device_id="ATTACKER", ip_country="BR",
                      timestamp=START.normalize() + pd.Timedelta(days=20, hours=3))
    return pd.concat([make_history(), pd.DataFrame([attack])], ignore_index=True)


def test_risk_score_counts_rules_fired(accounts):
    scored = score_transactions(account_with_takeover(), accounts).set_index("transaction_id")
    attack = scored.loc["ATTACK"]
    assert attack["risk_score"] == 5
    assert attack["rules_triggered"].split("; ") == [
        "amount_spike", "ip_country_mismatch", "new_device", "unusual_hour", "cnp_online_high_amount"]
    assert scored.drop("ATTACK")["risk_score"].eq(0).all()


def test_history_only_contains_earlier_transactions(accounts):
    # Input in reverse order: the engine must still score in time order.
    transactions = account_with_takeover().iloc[::-1].reset_index(drop=True)
    scored = score_transactions(transactions, accounts)
    assert list(scored["transaction_id"]) == list(transactions["transaction_id"])
    assert scored.set_index("transaction_id").loc["ATTACK", "risk_score"] == 5


def test_refuses_to_score_with_label(accounts):
    transactions = account_with_takeover().assign(is_fraud_label=False)
    with pytest.raises(ValueError, match="is_fraud_label"):
        score_transactions(transactions, accounts)


def test_flag_applies_threshold_and_sorts_by_risk():
    scored = pd.DataFrame({"transaction_id": ["a", "b", "c", "d"],
                           "rules_triggered": ["", "x", "x; y", "x; y; z"],
                           "reasons": ["", "r", "r; r", "r; r; r"],
                           "risk_score": [0, 1, 2, 3]})
    assert list(flag(scored, threshold=2)["transaction_id"]) == ["d", "c"]
    assert list(flag(scored, threshold=1)["transaction_id"]) == ["d", "c", "b"]
