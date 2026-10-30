"""
SEED SYNTHETIC BANK DATA

Writes two CSV files to data/:
  accounts.csv      one row per account
  transactions.csv  one row per transaction, plus is_fraud_label

Fraud pattern injected: account takeover (ATO). An attacker gets into a
victim's online banking and, over a day or two, makes a burst of online
purchases and transfers from their own device, usually from another country
and often at night. is_fraud_label marks those rows. It is for evaluation only;
the rules engine refuses to run if it is present.

Legitimate customers also make noise on purpose, so the rules get false
positives the way they would in real life: holidays abroad, new phones,
the odd night out, a rare big purchase, and VPNs.

  python seed.py
  python seed.py --transactions 10000 --accounts 200 --fraud-rate 0.03 --seed 42
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent / "data"

PERIOD_START = pd.Timestamp("2026-07-01")
PERIOD_DAYS = 92                       # July to September
MIN_LEGIT_PER_ACCOUNT = 15

HOME_COUNTRIES = ["NL", "DE", "FR", "BE", "ES", "IT", "IE", "AT"]
HOME_WEIGHTS = [0.25, 0.20, 0.15, 0.10, 0.10, 0.10, 0.05, 0.05]
TRAVEL_COUNTRIES = ["ES", "IT", "PT", "GR", "FR", "GB", "US", "CH", "HR"]
ATTACKER_COUNTRIES = ["US", "BR", "NG", "RO", "VN", "GB", "TR", "ID"]
ONLINE_HUB_COUNTRIES = ["IE", "LU", "NL", "US"]
CURRENCY_BY_COUNTRY = {
    "GB": "GBP", "US": "USD", "CH": "CHF", "TR": "TRY", "BR": "BRL",
    "NG": "NGN", "RO": "RON", "VN": "VND", "ID": "IDR",
}                                      # everything else is a euro country

POS_MERCHANTS = [
    "FreshMart Supermarket", "City Bakery", "Fuel Stop", "Urban Outfit",
    "Home & Living", "Corner Cafe", "PharmaCare", "Sports Depot",
]
ONLINE_MERCHANTS = [
    "ShopNow Online", "StreamBox", "TravelBooker", "GadgetHub",
    "FoodDash Delivery", "AppMarket Digital", "Fashion Online",
]
FRAUD_MERCHANTS = ["GadgetHub", "GiftCard World", "CryptoSwap Exchange", "LuxeWatch Online"]

CHANNELS = ["POS", "ONLINE", "ATM", "TRANSFER"]
CHANNEL_WEIGHTS = [0.55, 0.30, 0.08, 0.07]

# Relative chance of a legitimate transaction in each hour of the day.
DAY_PROFILE = np.array([0.4, 0.2, 0.15, 0.15, 0.15, 0.3, 0.8, 2, 3, 3.5, 4, 5,
                        6, 5, 4.5, 4.5, 5, 6, 6.5, 6, 5, 3.5, 2, 1])
NIGHT_OWL_PROFILE = DAY_PROFILE.copy()
NIGHT_OWL_PROFILE[[1, 2, 3, 4]] = 3.0  # shift workers, students


def device_id(rng) -> str:
    return f"DEV-{int(rng.integers(16**8)):08x}"


def make_account(rng, idx: int) -> dict:
    """An account plus the hidden habits that drive its transactions."""
    profile = NIGHT_OWL_PROFILE if rng.random() < 0.05 else DAY_PROFILE
    trip = None
    if rng.random() < 0.25:
        trip = (int(rng.integers(0, PERIOD_DAYS - 7)), int(rng.integers(4, 11)),
                str(rng.choice(TRAVEL_COUNTRIES)))
    return {
        "account_id": f"A{idx:04d}",
        "home_country": str(rng.choice(HOME_COUNTRIES, p=HOME_WEIGHTS)),
        "created_at": PERIOD_START - pd.Timedelta(days=int(rng.integers(30, 3650))),
        # hidden habits, not written to accounts.csv
        "typical_amount": float(np.exp(rng.normal(np.log(35), 0.5))),
        "devices": [device_id(rng) for _ in range(rng.integers(1, 3))],
        "card_id": f"CARD-{idx:04d}",
        "hour_p": profile / profile.sum(),
        "trip": trip,                                          # (start day, length, country)
        "new_phone_day": int(rng.integers(10, PERIOD_DAYS)) if rng.random() < 0.15 else None,
    }


def legit_transaction(rng, acct: dict) -> dict:
    day = int(rng.integers(0, PERIOD_DAYS))
    ts = (PERIOD_START + pd.Timedelta(days=day, hours=int(rng.choice(24, p=acct["hour_p"])),
                                      minutes=int(rng.integers(60)), seconds=int(rng.integers(60))))
    channel = str(rng.choice(CHANNELS, p=CHANNEL_WEIGHTS))

    here = acct["home_country"]
    if acct["trip"] and acct["trip"][0] <= day < acct["trip"][0] + acct["trip"][1]:
        here = acct["trip"][2]

    typical = acct["typical_amount"]
    if channel in ("POS", "ATM"):
        card_present, dev, ip = True, acct["card_id"], None   # card-present has no IP
        merchant_country = here
        if channel == "POS":
            merchant, amount = str(rng.choice(POS_MERCHANTS)), rng.lognormal(np.log(typical), 0.6)
        else:
            merchant, amount = "Cash Withdrawal ATM", 20.0 * int(rng.integers(1, 11))
    else:
        card_present = False
        devices = list(acct["devices"])
        if acct["new_phone_day"] is not None and day >= acct["new_phone_day"]:
            devices[0] = acct["card_id"].replace("CARD", "PHONE2")   # replaced their phone
        dev = devices[0] if len(devices) == 1 or rng.random() < 0.8 else devices[1]
        ip = here if rng.random() > 0.01 else str(rng.choice(ATTACKER_COUNTRIES))  # 1% VPN
        if channel == "ONLINE":
            merchant = str(rng.choice(ONLINE_MERCHANTS))
            merchant_country = here if rng.random() < 0.7 else str(rng.choice(ONLINE_HUB_COUNTRIES))
            amount = rng.lognormal(np.log(typical * 1.3), 0.7)
        else:
            merchant = f"Transfer to IBAN ****{int(rng.integers(10000)):04d}"
            merchant_country = acct["home_country"]
            amount = rng.lognormal(np.log(typical * 4), 0.6)

    if rng.random() < 0.01 and channel != "ATM":
        amount *= rng.uniform(6, 15)                           # a legit big purchase

    return _row(acct, ts, amount, merchant, merchant_country, channel, card_present, dev, ip, False)


def ato_episode(rng, acct: dict, size: int) -> list[dict]:
    """Account takeover: one attacker device, one burst over up to two days."""
    start = PERIOD_START + pd.Timedelta(days=int(rng.integers(21, PERIOD_DAYS - 2)))
    attacker_device = device_id(rng)
    attacker_country = str(rng.choice([c for c in ATTACKER_COUNTRIES if c != acct["home_country"]]))
    # A quarter of attackers route through a proxy in the victim's country.
    ip = acct["home_country"] if rng.random() < 0.25 else attacker_country

    rows = []
    for _ in range(size):
        hour = int(rng.integers(2, 5)) if rng.random() < 0.6 else int(rng.integers(0, 24))
        ts = start + pd.Timedelta(days=int(rng.integers(0, 2)), hours=hour,
                                  minutes=int(rng.integers(60)), seconds=int(rng.integers(60)))
        typical = acct["typical_amount"]
        if rng.random() < 0.65:
            channel, merchant = "ONLINE", str(rng.choice(FRAUD_MERCHANTS))
            merchant_country = str(rng.choice(ONLINE_HUB_COUNTRIES + [attacker_country]))
            amount = typical * rng.uniform(2, 12)
        else:
            channel = "TRANSFER"
            merchant = f"Transfer to IBAN ****{int(rng.integers(10000)):04d}"
            merchant_country = attacker_country
            amount = typical * rng.uniform(5, 25)
        rows.append(_row(acct, ts, amount, merchant, merchant_country, channel,
                         False, attacker_device, ip, True))
    return rows


def _row(acct, ts, amount, merchant, merchant_country, channel, card_present, dev, ip, is_fraud) -> dict:
    return {
        "account_id": acct["account_id"],
        "timestamp": ts,
        "amount": round(float(amount), 2),
        "currency": CURRENCY_BY_COUNTRY.get(merchant_country, "EUR"),
        "merchant_name": merchant,
        "merchant_country": merchant_country,
        "channel": channel,
        "card_present": card_present,
        "device_id": dev,
        "ip_country": ip,
        "is_fraud_label": is_fraud,
    }


def generate(n_transactions: int = 10_000, n_accounts: int = 200,
             fraud_rate: float = 0.03, seed: int = 42) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (accounts, transactions). Same seed, same data."""
    rng = np.random.default_rng(seed)
    accounts = [make_account(rng, i + 1) for i in range(n_accounts)]

    n_fraud = round(n_transactions * fraud_rate)
    n_legit = n_transactions - n_fraud

    # Some customers use their card far more than others.
    floor = min(MIN_LEGIT_PER_ACCOUNT, n_legit // n_accounts)
    weights = rng.gamma(2.0, 1.0, size=n_accounts)
    per_account = floor + rng.multinomial(n_legit - floor * n_accounts, weights / weights.sum())

    rows = [legit_transaction(rng, acct) for acct, n in zip(accounts, per_account) for _ in range(n)]

    # Split the fraud budget into episodes of 3-8 transactions on distinct victims.
    victims = iter(rng.permutation(n_accounts))
    remaining = n_fraud
    while remaining > 0:
        size = min(int(rng.integers(3, 9)), remaining)
        rows += ato_episode(rng, accounts[next(victims)], size)
        remaining -= size

    transactions = pd.DataFrame(rows).sort_values("timestamp", kind="stable").reset_index(drop=True)
    transactions.insert(0, "transaction_id", [f"T{i:06d}" for i in range(1, len(transactions) + 1)])

    legit = transactions[~transactions["is_fraud_label"]]
    monthly_spend = legit.groupby("account_id")["amount"].sum() / (PERIOD_DAYS / 30.4)
    accounts_df = pd.DataFrame({
        "account_id": [a["account_id"] for a in accounts],
        "home_country": [a["home_country"] for a in accounts],
        "avg_monthly_spend": [round(float(monthly_spend.get(a["account_id"], 0.0)), 2) for a in accounts],
        "created_at": [a["created_at"] for a in accounts],
    })
    return accounts_df, transactions


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--transactions", type=int, default=10_000)
    parser.add_argument("--accounts", type=int, default=200)
    parser.add_argument("--fraud-rate", type=float, default=0.03)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    accounts, transactions = generate(args.transactions, args.accounts, args.fraud_rate, args.seed)

    # GCP: write to Cloud Storage and load into BigQuery (bq load / external table)
    DATA_DIR.mkdir(exist_ok=True)
    accounts.to_csv(DATA_DIR / "accounts.csv", index=False)
    transactions.to_csv(DATA_DIR / "transactions.csv", index=False, float_format="%.2f")

    n_fraud = int(transactions["is_fraud_label"].sum())
    print(f"Wrote {len(accounts):,} accounts and {len(transactions):,} transactions to {DATA_DIR}/")
    print(f"Injected {n_fraud:,} account-takeover transactions ({n_fraud / len(transactions):.1%})")


if __name__ == "__main__":
    main()
