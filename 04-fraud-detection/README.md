# Fraud Detection Rules Pipeline

**An educational project about rule-based fraud detection.** A bank's card and transfer transactions run through five small rules. Each rule that fires adds one point to a risk score, and transactions scoring 2 or more go to an analyst review list along with the reasons. Nothing is blocked.

Plain Python and pandas, no ML, no cloud. Comments marked `# GCP:` show where each step would run on Google Cloud.

```bash
pip install -r requirements.txt
python seed.py              # 200 accounts, 10,000 transactions, 3% account takeover
python run_pipeline.py      # writes output/flagged_transactions.csv, prints the evaluation
python -m pytest -q
```

## The Situation

An attacker gets into a customer's online banking. Over the next day or two they buy gift cards and electronics and send transfers abroad, from their own device, usually from another country and often while the customer is asleep. This is **account takeover (ATO)**, and it is the only fraud pattern `seed.py` injects.

The bank wants analysts to see these transactions quickly without drowning them in false alarms from customers who are just on holiday, bought a new phone, or stayed up late.

## How It Works

```
 seed.py ──► data/accounts.csv
         └─► data/transactions.csv  (+ is_fraud_label, evaluation only)
                     │
                     ▼
 load ──► label split off ──► score: per account, in time order,
                │               run 5 rules against earlier history
                │                         │
                │                         ▼
                │               risk_score = rules fired
                │                         │
                │                         ▼
                │               flag: risk_score >= 2 ──► output/flagged_transactions.csv
                ▼                         │
            evaluate ◄────────────────────┘  precision, recall, threshold sweep, per rule
```

| Rule | Fires when |
|------|------------|
| `amount_spike` | amount is more than 5x the account's average earlier transaction |
| `ip_country_mismatch` | the IP country is known and is not the home country |
| `new_device` | the device was first used on this account less than 24 hours ago |
| `unusual_hour` | 02:00 to 04:59, and under 5% of the account's earlier transactions were at that time |
| `cnp_online_high_amount` | an ONLINE, card-not-present transaction over 500 EUR |

## Project Layout

```
04-fraud-detection/
├── seed.py                 # synthetic accounts + transactions with injected ATO
├── run_pipeline.py         # load → score → flag → write CSV → evaluate
├── fraud/
│   ├── config.py           # every threshold in one place
│   ├── data.py             # loaders; splits off is_fraud_label
│   ├── rules.py            # the five rules, one function each
│   ├── engine.py           # runs rules per transaction, builds the risk score
│   └── evaluate.py         # precision/recall report (only place the label is read)
└── tests/                  # one test file per rule, plus engine, evaluation, seed
```

## Output

`output/flagged_transactions.csv`, highest risk first:

| column | example |
|--------|---------|
| `transaction_id` | `T002747` |
| `rules_triggered` | `amount_spike; ip_country_mismatch; new_device; unusual_hour` |
| `reasons` | `amount 421.74 EUR is 7.2x the account average of 58.59 EUR; IP country US differs from home country IT; ...` |
| `risk_score` | `4` |

## Results (seed 42)

```
Flagged: 240   True positives: 203   False positives: 37   Missed: 97
Precision: 84.6%   Recall: 67.7%

If the threshold were...
  score >= 1   flagged   763   precision  38.0%   recall  96.7%
  score >= 2   flagged   240   precision  84.6%   recall  67.7%  <- current
  score >= 3   flagged    95   precision  98.9%   recall  31.3%

Each rule on its own
  rule                     fired  precision   recall
  amount_spike               249      32.9%    27.3%
  ip_country_mismatch        332      71.4%    79.0%
  new_device                 359      56.5%    67.7%
  unusual_hour               142      55.6%    26.3%
  cnp_online_high_amount      41      26.8%     3.7%
```

What to notice:

- **The threshold is the precision/recall dial.** At 1 analysts see 3x more cases to catch 29 more frauds; at 3 almost every case is real but two thirds of the fraud is missed.
- **`ip_country_mismatch` and `new_device` carry ATO detection**, because they describe the attacker, not the purchase.
- **`cnp_online_high_amount` barely helps against ATO.** Attackers here spend 2-12x the customer's usual amount, which is rarely over 500 EUR. That rule targets a different pattern (card testing / stolen card details) that is not in this data yet.
- **The missed 32% comes from the attack lasting too long.** 71 of the 97 misses happen more than 24 hours after the attacker's first transaction, so `new_device` has gone quiet. And 48 of the 50 missed night transactions don't fire `unusual_hour`, because the attacker's own earlier night transactions are now in the history and push the night share over 5%. The attacker has poisoned the baseline. Both are fixable, for example by excluding already-flagged transactions from the history.

## Design Choices

**Data**

- **10,000 transactions over 200 accounts**: about 50 per account gives each account a stable enough history for the per-account rules.
- **Only account takeover is injected**: one well-understood pattern makes it clear which rules help and which do not.
- **Legitimate customers make noise on purpose** (holidays, new phones, VPNs, night owls, rare big purchases): without false positive sources, precision would be meaningless.
- **25% of attackers proxy through the victim's country**: real attackers do this, and it keeps recall honest.
- **Card-present (POS/ATM) transactions have a null `ip_country`** and use the card ID as `device_id`: a shop terminal has no customer IP or phone.
- **Amounts are stored with two decimals and computed as floats**: thresholds like "over 500" do not need exact decimal arithmetic; a production ledger would use `NUMERIC`/`Decimal`.
- **A fixed random seed**: every run, test and README number is reproducible.

**Rules and scoring**

- **Every rule has the same signature `(txn, account, history) -> (bool, reason)`**: the engine can loop over a list, and each rule is testable on its own with hand-built inputs.
- **A rule only sees the account's earlier transactions**: using later data would be a leak that no live system could reproduce.
- **History-based rules stay quiet below 5 earlier transactions**: without a baseline there is nothing to compare against, so they would fire on every new account.
- **"Average" means the mean of earlier transactions, not `avg_monthly_spend`**: one transaction versus a month of spending is a different scale.
- **A device stays "new" for 24 hours after first use**: an attacker reuses their device for the whole takeover, so "never seen" alone only catches the first transaction.
- **"Unusual hour" checks the account's own habits**: 03:00 is normal for a night-shift worker and suspicious for everyone else.
- **Missing IP country is not a signal**: unknown is not evidence of fraud.
- **Score = number of rules fired, flag at 2 or more**: one weak signal (a new phone, a holiday) is common and harmless; two together are much rarer.
- **Thresholds live in `fraud/config.py`**: tuning is one edit, not a hunt through the code.

**Pipeline and evaluation**

- **The engine raises an error if `is_fraud_label` is present**: the rule "never use the label as input" is enforced by code, not by discipline.
- **Output includes a `reasons` column next to `rules_triggered`**: analysts need to know why, not just which rule.
- **The report shows a threshold sweep and per-rule numbers**: the threshold is a business decision, and these numbers are what you decide it with.
- **Plain pandas, row by row**: easy to read and step through; 10,000 rows take a few seconds.

## Cloud Equivalents

| Step | Here | GCP |
|------|------|-----|
| Raw data | CSV in `data/` | Cloud Storage + BigQuery tables |
| Per-account history | `group.iloc[:i]` | BigQuery window functions (batch) or Dataflow stateful processing (streaming) |
| Device first-seen lookup | filter on history | Bigtable or Memorystore keyed by account and device |
| Flag list | `output/flagged_transactions.csv` | BigQuery table feeding a Looker review queue |
| Evaluation | `fraud/evaluate.py` | Scheduled BigQuery query against confirmed fraud cases |

## Ideas for Next Steps

- Inject card testing (many small CNP transactions at one merchant) and see which new rule it needs.
- Add a velocity rule: N transactions in M minutes.
- Give rules weights instead of 1 point each, and compare the threshold sweep.
- Rewrite the history rules as BigQuery SQL with window functions and check the flags match.
