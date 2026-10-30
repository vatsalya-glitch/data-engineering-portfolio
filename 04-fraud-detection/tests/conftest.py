import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

START = pd.Timestamp("2026-07-01 12:00")


def make_txn(**overrides) -> pd.Series:
    """A plain daytime POS transaction at home; override what the test is about."""
    txn = {
        "transaction_id": "T1",
        "account_id": "A1",
        "timestamp": START + pd.Timedelta(days=30),
        "amount": 40.0,
        "currency": "EUR",
        "merchant_name": "Corner Cafe",
        "merchant_country": "NL",
        "channel": "POS",
        "card_present": True,
        "device_id": "PHONE",
        "ip_country": None,
        **overrides,
    }
    return pd.Series(txn)


def make_history(n: int = 10, start: pd.Timestamp = START, **overrides) -> pd.DataFrame:
    """n earlier transactions, one per day from `start` (noon), from the account's usual device."""
    rows = [make_txn(transaction_id=f"H{i}", timestamp=start + pd.Timedelta(days=i), **overrides)
            for i in range(n)]
    return pd.DataFrame(rows)


@pytest.fixture
def account() -> pd.Series:
    return pd.Series({"account_id": "A1", "home_country": "NL", "avg_monthly_spend": 1200.0,
                      "created_at": pd.Timestamp("2020-01-01")})
