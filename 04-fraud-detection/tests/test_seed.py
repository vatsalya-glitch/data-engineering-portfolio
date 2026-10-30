import pytest

from seed import generate


@pytest.fixture(scope="module")
def seeded():
    return generate(n_transactions=2_000, n_accounts=40, fraud_rate=0.03, seed=7)


def test_sizes_and_fraud_rate(seeded):
    accounts, transactions = seeded
    assert len(accounts) == 40
    assert len(transactions) == 2_000
    assert transactions["is_fraud_label"].sum() == 60


def test_ids_are_unique_and_accounts_exist(seeded):
    accounts, transactions = seeded
    assert transactions["transaction_id"].is_unique
    assert set(transactions["account_id"]) <= set(accounts["account_id"])


def test_takeover_looks_like_a_takeover(seeded):
    _, transactions = seeded
    fraud = transactions[transactions["is_fraud_label"]]
    assert set(fraud["channel"]) <= {"ONLINE", "TRANSFER"}
    assert not fraud["card_present"].any()
    # One attacker device per victim, never used by the victim themselves.
    legit_devices = set(transactions.loc[~transactions["is_fraud_label"], "device_id"])
    assert fraud.groupby("account_id")["device_id"].nunique().eq(1).all()
    assert not set(fraud["device_id"]) & legit_devices


def test_card_present_has_no_ip(seeded):
    _, transactions = seeded
    assert transactions.loc[transactions["card_present"], "ip_country"].isna().all()


def test_same_seed_same_data():
    _, a = generate(n_transactions=300, n_accounts=10, seed=1)
    _, b = generate(n_transactions=300, n_accounts=10, seed=1)
    assert a.equals(b)
