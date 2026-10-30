"""
RULES ENGINE

Runs every rule on every transaction, in time order per account, and turns
the results into a risk score (the number of rules that fired).
"""

import pandas as pd

from fraud import config
from fraud.data import LABEL_COLUMN
from fraud.rules import RULES

SEPARATOR = "; "


def check_transaction(txn, account, history, rules=RULES) -> list[tuple[str, str]]:
    """Return (rule_name, reason) for every rule that fires on one transaction."""
    hits = []
    for rule in rules:
        flagged, reason = rule(txn, account, history)
        if flagged:
            hits.append((rule.__name__, reason))
    return hits


def score_transactions(transactions: pd.DataFrame, accounts: pd.DataFrame, rules=RULES) -> pd.DataFrame:
    """One row per transaction: transaction_id, rules_triggered, reasons, risk_score."""
    if LABEL_COLUMN in transactions.columns:
        raise ValueError(f"{LABEL_COLUMN} is for evaluation only; drop it before scoring")

    # GCP: Dataflow (Apache Beam) keyed by account_id with per-key state for the
    #      history, or BigQuery window functions for a batch run.
    accounts_by_id = accounts.set_index("account_id")
    ordered = transactions.sort_values(["account_id", "timestamp"], kind="stable")
    rows = []
    for account_id, group in ordered.groupby("account_id", sort=False):
        account = accounts_by_id.loc[account_id]
        for i in range(len(group)):
            txn = group.iloc[i]
            hits = check_transaction(txn, account, group.iloc[:i], rules)
            rows.append({
                "transaction_id": txn["transaction_id"],
                "rules_triggered": SEPARATOR.join(name for name, _ in hits),
                "reasons": SEPARATOR.join(reason for _, reason in hits),
                "risk_score": len(hits),
            })
    scored = pd.DataFrame(rows, columns=["transaction_id", "rules_triggered", "reasons", "risk_score"])
    order = transactions[["transaction_id"]]                    # back to input order
    return order.merge(scored, on="transaction_id", how="left")


def flag(scored: pd.DataFrame, threshold: int = config.SCORE_THRESHOLD) -> pd.DataFrame:
    """Transactions for analyst review, highest risk first. Nothing is blocked."""
    flagged = scored[scored["risk_score"] >= threshold]
    return flagged.sort_values(["risk_score", "transaction_id"], ascending=[False, True]).reset_index(drop=True)
