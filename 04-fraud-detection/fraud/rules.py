"""
FRAUD RULES

Each rule looks at one transaction and returns (flagged, reason). They all
share one signature so the engine can loop over them:

    rule(txn, account, history) -> (bool, str)

  txn      the transaction being checked (pd.Series)
  account  its row from the accounts table (pd.Series)
  history  the same account's earlier transactions, oldest first (pd.DataFrame)

A rule never sees later transactions or the fraud label.
"""

import pandas as pd

from fraud import config

NOT_FLAGGED = (False, "")


def amount_spike(txn, account, history):
    """Amount is more than AMOUNT_MULTIPLIER x the account's average transaction."""
    # GCP: BigQuery AVG(amount) OVER (PARTITION BY account_id ORDER BY timestamp
    #      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
    if len(history) < config.MIN_HISTORY:
        return NOT_FLAGGED
    average = history["amount"].mean()
    if txn["amount"] > config.AMOUNT_MULTIPLIER * average:
        return True, (f"amount {txn['amount']:.2f} EUR is {txn['amount'] / average:.1f}x "
                      f"the account average of {average:.2f} EUR")
    return NOT_FLAGGED


def ip_country_mismatch(txn, account, history):
    """IP country is known and differs from the account's home country."""
    ip = txn["ip_country"]
    if pd.isna(ip) or ip == account["home_country"]:
        return NOT_FLAGGED
    return True, f"IP country {ip} differs from home country {account['home_country']}"


def new_device(txn, account, history):
    """Device was first used on this account less than NEW_DEVICE_WINDOW ago."""
    # GCP: Bigtable or Memorystore keyed by (account_id, device_id) -> first_seen
    if len(history) < config.MIN_HISTORY:
        return NOT_FLAGGED
    seen = history.loc[history["device_id"] == txn["device_id"], "timestamp"]
    if seen.empty:
        return True, f"device {txn['device_id']} never used on this account before"
    age = txn["timestamp"] - seen.min()
    if age < config.NEW_DEVICE_WINDOW:
        return True, (f"device {txn['device_id']} first used on this account "
                      f"{age.total_seconds() / 3600:.0f}h ago")
    return NOT_FLAGGED


def unusual_hour(txn, account, history):
    """Transaction between 02:00 and 05:00, and the account rarely transacts then."""
    # GCP: BigQuery COUNTIF(EXTRACT(HOUR FROM timestamp) BETWEEN 2 AND 4) OVER (...)
    if txn["timestamp"].hour not in config.NIGHT_HOURS or len(history) < config.MIN_HISTORY:
        return NOT_FLAGGED
    night_share = history["timestamp"].dt.hour.isin(config.NIGHT_HOURS).mean()
    if night_share < config.NIGHT_USUAL_SHARE:
        return True, (f"transaction at {txn['timestamp']:%H:%M}; only {night_share:.0%} of this "
                      f"account's earlier transactions were between 02:00 and 05:00")
    return NOT_FLAGGED


def cnp_online_high_amount(txn, account, history):
    """Card-not-present ONLINE transaction over CNP_AMOUNT_EUR."""
    if txn["channel"] == "ONLINE" and not txn["card_present"] and txn["amount"] > config.CNP_AMOUNT_EUR:
        return True, f"card-not-present online transaction of {txn['amount']:.2f} EUR"
    return NOT_FLAGGED


RULES = [amount_spike, ip_country_mismatch, new_device, unusual_hour, cnp_online_high_amount]
