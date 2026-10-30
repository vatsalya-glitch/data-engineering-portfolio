"""
FRAUD DETECTION PIPELINE

  load -> score (run every rule) -> flag (score >= threshold) -> write CSV -> evaluate

  python seed.py            # once, to create data/
  python run_pipeline.py
  python run_pipeline.py --threshold 1
"""

import argparse
import sys
from pathlib import Path

from fraud import config
from fraud.data import load_accounts, load_transactions
from fraud.engine import flag, score_transactions
from fraud.evaluate import evaluate, format_report

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "output"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--threshold", type=int, default=config.SCORE_THRESHOLD,
                        help="flag when at least this many rules fire")
    args = parser.parse_args()

    if not (DATA_DIR / "transactions.csv").exists():
        sys.exit("No data yet. Run: python seed.py")

    accounts = load_accounts(DATA_DIR / "accounts.csv")
    transactions, labels = load_transactions(DATA_DIR / "transactions.csv")

    scored = score_transactions(transactions, accounts)
    flagged = flag(scored, args.threshold)

    # GCP: write to a BigQuery table that feeds the analyst review queue (e.g. Looker)
    OUTPUT_DIR.mkdir(exist_ok=True)
    out = OUTPUT_DIR / "flagged_transactions.csv"
    flagged.to_csv(out, index=False)
    print(f"Wrote {len(flagged):,} flagged transactions to {out.relative_to(ROOT)}\n")

    print(format_report(evaluate(scored, labels, args.threshold)))


if __name__ == "__main__":
    main()
