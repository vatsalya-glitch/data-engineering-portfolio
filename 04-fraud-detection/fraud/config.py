"""Every threshold the rules and the engine use, in one place."""

import pandas as pd

# A transaction is flagged for review when at least this many rules fire.
SCORE_THRESHOLD = 2

# History-based rules stay quiet until the account has this many earlier
# transactions. Below that there is no "normal" to compare against.
MIN_HISTORY = 5

# amount_spike: amount above this multiple of the account's average transaction.
AMOUNT_MULTIPLIER = 5

# new_device: a device counts as new until this long after its first use.
# An attacker reuses their device for the whole takeover, so "never seen
# before" alone would only catch the first transaction.
NEW_DEVICE_WINDOW = pd.Timedelta(hours=24)

# unusual_hour: 02:00-04:59 is unusual if the account rarely transacts then.
NIGHT_HOURS = (2, 3, 4)
NIGHT_USUAL_SHARE = 0.05

# cnp_online_high_amount: card-not-present online purchase above this.
CNP_AMOUNT_EUR = 500
