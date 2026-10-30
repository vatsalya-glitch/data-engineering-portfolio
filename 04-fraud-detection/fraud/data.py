"""Load the seeded CSVs and keep the fraud label away from the rules."""

from pathlib import Path

import pandas as pd

LABEL_COLUMN = "is_fraud_label"


def load_accounts(path: Path) -> pd.DataFrame:
    # GCP: SELECT * FROM `project.bank.accounts` in BigQuery
    return pd.read_csv(path, parse_dates=["created_at"])


def load_transactions(path: Path) -> tuple[pd.DataFrame, pd.Series]:
    """Return (transactions without the label, label indexed by transaction_id)."""
    # GCP: SELECT * FROM `project.bank.transactions` in BigQuery
    df = pd.read_csv(
        path,
        parse_dates=["timestamp"],
        dtype={"transaction_id": str, "account_id": str, "device_id": str,
               "ip_country": "string", "card_present": bool},
    )
    labels = df.set_index("transaction_id")[LABEL_COLUMN].astype(bool)
    return df.drop(columns=[LABEL_COLUMN]), labels
