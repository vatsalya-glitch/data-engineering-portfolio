"""
EVALUATION

Compares flags with is_fraud_label. This is the only module that reads the label.

  precision = of the transactions we flagged, how many were fraud
  recall    = of the fraud transactions, how many we flagged
"""

import pandas as pd

from fraud.engine import SEPARATOR
from fraud.rules import RULES


def precision_recall(predicted: pd.Series, actual: pd.Series) -> dict:
    """Both series are booleans with the same index."""
    tp = int((predicted & actual).sum())
    fp = int((predicted & ~actual).sum())
    fn = int((~predicted & actual).sum())
    return {
        "flagged": tp + fp,
        "true_positives": tp,
        "false_positives": fp,
        "missed": fn,
        "precision": tp / (tp + fp) if tp + fp else 0.0,
        "recall": tp / (tp + fn) if tp + fn else 0.0,
    }


def evaluate(scored: pd.DataFrame, labels: pd.Series, threshold: int) -> dict:
    """Overall result at `threshold`, a sweep of other thresholds, and each rule on its own."""
    # GCP: a scheduled BigQuery query joining flags to confirmed-fraud cases
    scored = scored.set_index("transaction_id")
    actual = labels.reindex(scored.index).astype(bool)
    fired = scored["rules_triggered"].fillna("").str.split(SEPARATOR)

    return {
        "transactions": len(scored),
        "fraud": int(actual.sum()),
        "threshold": threshold,
        "overall": precision_recall(scored["risk_score"] >= threshold, actual),
        "sweep": {t: precision_recall(scored["risk_score"] >= t, actual)
                  for t in range(1, len(RULES) + 1)},
        "per_rule": {rule.__name__: precision_recall(fired.apply(lambda names, n=rule.__name__: n in names), actual)
                     for rule in RULES},
    }


def format_report(result: dict) -> str:
    o = result["overall"]
    lines = [
        "Fraud detection evaluation",
        "==========================",
        f"Transactions: {result['transactions']:,}   Labelled fraud: {result['fraud']:,} "
        f"({result['fraud'] / result['transactions']:.1%})",
        f"Flag rule: risk_score >= {result['threshold']}",
        "",
        f"Flagged: {o['flagged']:,}   True positives: {o['true_positives']:,}   "
        f"False positives: {o['false_positives']:,}   Missed: {o['missed']:,}",
        f"Precision: {o['precision']:.1%}   Recall: {o['recall']:.1%}",
        "",
        "If the threshold were...",
    ]
    for t, m in result["sweep"].items():
        marker = "  <- current" if t == result["threshold"] else ""
        lines.append(f"  score >= {t}   flagged {m['flagged']:>5,}   precision {m['precision']:>6.1%}   "
                     f"recall {m['recall']:>6.1%}{marker}")
    lines += ["", "Each rule on its own", f"  {'rule':<24}{'fired':>6}{'precision':>11}{'recall':>9}"]
    for name, m in result["per_rule"].items():
        lines.append(f"  {name:<24}{m['flagged']:>6,}{m['precision']:>11.1%}{m['recall']:>9.1%}")
    return "\n".join(lines)
