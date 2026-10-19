# Data Masking Techniques: A Comprehensive Guide

This guide goes deeper into the three masking techniques implemented in this pipeline. Use this to understand the theory and tradeoffs of each approach.

## Table of Contents
1. [One-Way Hashing](#one-way-hashing)
2. [Redaction](#redaction)
3. [Flag + Preserve](#flag--preserve)
4. [Comparison Matrix](#comparison-matrix)
5. [Implementation Details](#implementation-details)

---

## One-Way Hashing

### What is One-Way Hashing?

A one-way hash converts data into a fixed-length string using a mathematical function that:
- Always produces the same output for the same input (deterministic)
- Cannot be reversed to recover the original input (irreversible)
- Is fast to compute

```
Hash function: H(input) → output
Property: H(x) = H(x) always, but you cannot compute x from H(x)
```

### Example with Real Data

```python
import hashlib

names = ["Anna Mueller", "anna mueller", "ANNA MUELLER"]

for name in names:
    hashed = hashlib.md5(name.encode()).hexdigest()
    print(f"{name:20} → {hashed}")

# Output:
# Anna Mueller         → 61d1282a41d1f336c09d8c63d03eec67
# anna mueller         → 61d1282a41d1f336c09d8c63d03eec67
# ANNA MUELLER         → 61d1282a41d1f336c09d8c63d03eec67
```

**Key insight:** Different casings of the same name produce the SAME hash. This enables consistent matching.

### When to Use One-Way Hashing

| Scenario | Use Hash? | Why? |
|----------|-----------|------|
| Patient name | ✅ YES | Names need to be linkable (same patient across records) |
| Insurance number | ✅ YES | ID numbers enable joins on patient identity |
| Phone number | ✅ YES | Contact info should be consistent across records |
| Address | ❌ NO | Location data is usually redacted, not hashed |
| Test result | ❌ NO | Clinical data doesn't need to be masked this way |

### Advantages of One-Way Hashing

1. **Deterministic Linkage**
   - Same input always produces same hash
   - Enables: "Find all records for patient XYZ"
   - Example: Match emergency contacts across tables

2. **Irreversible**
   - Original value cannot be recovered
   - If hash is exposed, attacker cannot get real value

3. **Compact**
   - Hashes are fixed length (MD5: 32 chars)
   - Saves storage compared to original values

### Disadvantages of One-Way Hashing

1. **Rainbow Table Attack**
   - Common names can be pre-computed
   - Example: "Anna" might hash to known value
   - Mitigation: Use salt (add random prefix)

2. **No Partial Matching**
   - Cannot search for "Anna *" (first name matches)
   - Either exact match or no match

3. **Information Disclosure**
   - Same hash = same person (pattern visible)
   - Attackers know which records are identical

### Implementation in This Pipeline

```python
def hash_md5(value):
    """Hash a value consistently with MD5."""
    if value is None or value == "":
        return None
    return hashlib.md5(str(value).encode()).hexdigest()

# Usage:
hashed_name = hash_md5("Anna Mueller")  # → 61d1282a...
```

---

## Redaction

### What is Redaction?

Redaction permanently removes sensitive data by replacing it with a placeholder (usually `***` or `REDACTED`).

```
Original: "10115"
Redacted: "***"
Recovery: IMPOSSIBLE - data is destroyed
```

### Example with Real Data

```
Before redaction:
  | Patient ID | Name           | Zip Code |
  |------------|----------------|----------|
  | PAT-001    | Anna Mueller   | 10115    |
  | PAT-002    | James Smith    | 10243    |

After redaction:
  | Patient ID | Name           | Zip Code |
  |------------|----------------|----------|
  | PAT-001    | ***            | ***      |
  | PAT-002    | ***            | ***      |
```

### When to Use Redaction

| Scenario | Redact? | Why? |
|----------|---------|------|
| Postal code | ✅ YES | Location shouldn't be visible to analysts |
| Full address | ✅ YES | Complete location info is sensitive |
| Exact birth date | ✅ YES | Date + location = strong identifier |
| Patient name | ❌ NO | Names are usually hashed (linkable) |
| Insurance ID | ❌ NO | IDs should be linked across records |
| Age group (binned) | ❌ NO | Binned data (0-18, 19-29) is safe |

### Advantages of Redaction

1. **Complete Data Destruction**
   - No way to recover original value
   - Strongest privacy guarantee

2. **No Pattern Leakage**
   - Attackers cannot see which records match
   - Cannot infer from multiple redactions

3. **Simple to Implement**
   - Just replace with `***`
   - No cryptography needed

### Disadvantages of Redaction

1. **Information Loss**
   - Analysts cannot use redacted columns
   - Example: No location analysis possible

2. **No Linkage**
   - Cannot match records on redacted column
   - Example: Cannot join on postal code

3. **Reduced Analytics Value**
   - Demographic analysis without location is limited
   - May reduce usefulness of dataset

### Implementation in This Pipeline

```python
def redact(value):
    """Replace value with *** (irreversible)."""
    if value is None or value == "":
        return None
    return "***"

# Usage:
redacted_zip = redact("10115")  # → "***"
```

### When NOT to Redact Everything

A common mistake is redacting too much:

```
WRONG: Redact all sensitive columns
  | PatientID | Age | Gender | Diagnosis |
  |-----------|-----|--------|-----------|
  | ***       | *** | ***    | ***       |
  Result: Useless dataset!

RIGHT: Redact only truly sensitive columns
  | PatientID | Age | Gender | Diagnosis |
  |-----------|-----|--------|-----------|
  | PAT-001   | 45  | M      | Diabetes  |
  Result: Still useful for medical analysis
```

---

## Flag + Preserve

### What is Flag + Preserve?

Mark sensitive data with a flag while keeping the value for authorized downstream processing.

```
Original: "emergency_contact_phone"
Flagged:  "[FLAGGED]:emergency_contact_phone"
```

### Example with Real Data

```
Before flagging:
  | Patient ID | Emergency Contact | Phone         |
  |------------|-------------------|---------------|
  | PAT-001    | Anna Mueller      | +49-30-123456 |

After flagging:
  | Patient ID | Emergency Contact | Phone                       |
  |------------|-------------------|-----------------------------|
  | PAT-001    | [FLAGGED]:Anna... | [FLAGGED]:+49-30-123456     |

Access rule: Only show real values to medical team, not analysts
```

### When to Use Flag + Preserve

| Scenario | Flag+Preserve? | Why? |
|----------|---|---|
| Emergency contact | ✅ YES | Medical team needs real data; others don't |
| Billing address | ✅ YES | Finance needs it; analysts don't |
| Patient consent | ✅ YES | Legal may need to verify; clinical doesn't |
| Doctor's notes | ✅ YES | Care team needs; research team doesn't |
| Age | ❌ NO | Everyone can see age (not sensitive) |
| Medication | ❌ NO | Clinical team definitely needs this |

### Advantages of Flag + Preserve

1. **Flexibility**
   - Data available when needed with proper authorization
   - Different views for different users

2. **Audit Trail**
   - Clear marking of sensitive fields
   - Compliance: "We flagged this, asked for approval"

3. **No Information Loss**
   - Full data available to authorized users
   - Maximum utility for privileged access

### Disadvantages of Flag + Preserve

1. **Requires Governance**
   - Need access control system
   - Someone must enforce "authorized access"

2. **Cannot Share Broadly**
   - Dataset with flags cannot go to public/analysts
   - Requires infrastructure and process

3. **Risk if Access Control Fails**
   - If governance breaks down, PII is exposed
   - Not a technical protection, requires organizational discipline

### Implementation in This Pipeline

```python
def flag_sensitive(value):
    """Mark as flagged (keep value, add metadata)."""
    return f"[FLAGGED]:{value}" if value else None

# Usage:
flagged_phone = flag_sensitive("+49-30-1234567")
# → "[FLAGGED]:+49-30-1234567"
```

---

## Comparison Matrix

### By Privacy Level

```
STRONGEST PRIVACY:          WEAKEST PRIVACY:
┌──────────────┬─────────┬─────────┐
│  Redaction   │  Hashing │ Flagged │
│      ***     │  Hash77  │ [FLAG]: │
└──────────────┴─────────┴─────────┘
(Irreversible) (Linkable) (Preserved)
```

### By Analytics Utility

```
HIGHEST UTILITY:            LOWEST UTILITY:
┌──────────────┬─────────┬─────────┐
│  Flagged     │ Hashing │Redaction│
│[FLAG]:value  │ Hash77  │   ***   │
└──────────────┴─────────┴─────────┘
(Full data)   (Matches) (Destroyed)
```

### Decision Table

| Column Type | Privacy Critical? | Need to Link? | Choose |
|------------|---|---|---|
| Name | YES | YES | **Hash** |
| Insurance ID | YES | YES | **Hash** |
| Postal code | YES | NO | **Redact** |
| Address | YES | NO | **Redact** |
| Phone (emergency) | YES | NO* | **Flag** |
| Age | NO | - | No masking |
| Gender | NO | - | No masking |
| Diagnosis | NO | - | No masking |

*Emergency phone needs real value for emergencies, but not for analytics

---

## Implementation Details

### Applying Masking in SQL

```sql
-- Apply different strategies based on column
UPDATE patient_records SET
  name = MD5(name),                    -- Hash
  zip_code = '***',                    -- Redact
  emergency_phone = CONCAT('[FLAG]:', emergency_phone)  -- Flag
WHERE true;
```

### Handling NULL Values

All three techniques must handle NULL:

```python
def hash_md5(value):
    if value is None or value == "":
        return None  # Preserve NULL
    return hashlib.md5(str(value).encode()).hexdigest()

def redact(value):
    if value is None or value == "":
        return None  # Preserve NULL
    return "***"

def flag_sensitive(value):
    if value is None or value == "":
        return None  # Preserve NULL
    return f"[FLAGGED]:{value}"
```

### Handling Different Data Types

```python
# Phone numbers may need normalization before hashing
def normalize_phone(phone):
    # Remove spaces, dashes, etc. for consistent hashing
    return ''.join(c for c in phone if c.isdigit())

hash_md5(normalize_phone("+49 30 123456"))  # Works
```

### Salting Hashes (Advanced)

To prevent rainbow table attacks, add a salt:

```python
import hashlib

SALT = "phi-pipeline-2024"  # Organization-specific salt

def hash_md5_with_salt(value):
    if value is None or value == "":
        return None
    salted = f"{SALT}:{value}"
    return hashlib.md5(salted.encode()).hexdigest()
```

---

## Real-World Scenarios

### Scenario 1: Healthcare Analytics

**Goal:** Share patient data with researchers (cannot see PII)

```
✅ Hash patient names → Researchers see "hash123" (matches allowed)
✅ Redact zip codes → Researchers cannot infer location
✅ Keep diagnosis → Core data for research
❌ Do NOT flag emergency contact → Researchers don't have access anyway
```

### Scenario 2: Insurance Claims

**Goal:** Billing department needs patient data, but audit only

```
✅ Hash patient names → Claims system matches patients
✅ Redact address → Do not expose location
🚩 Flag SSN → Keep it, but log all access
✅ Keep account number → Needed for billing
```

### Scenario 3: Emergency Response

**Goal:** Emergency team can reach patients, analysis cannot

```
✅ Hash patient names → Matches in system
✅ Hash insurance ID → Matches with insurance
🚩 Flag emergency contact → Medical team sees real phone
✅ Redact home address → Emergency team uses GPS instead
```

---

## When to Use Each Technique

### Choose ONE-WAY HASHING When:
- Data needs to be linkable across records ✓
- You need to match on this column ✓
- But don't want analysts to see real values ✓
- **Best for:** Names, IDs, reference numbers

### Choose REDACTION When:
- Data is sensitive AND not needed for analysis ✓
- You want to ensure it cannot be recovered ✓
- Linkage on this column is not needed ✓
- **Best for:** Addresses, locations, specific dates

### Choose FLAG + PRESERVE When:
- Data is needed only by authorized users ✓
- You have access control infrastructure ✓
- Organizational governance is strong ✓
- **Best for:** Emergency contacts, billing info, legal data

---

## Testing Your Masking

Always verify masking worked:

```python
# Test 1: Original value is gone
assert original_name not in masked_data

# Test 2: Hashing is deterministic
assert hash_md5("Anna") == hash_md5("Anna")

# Test 3: Redaction is irreversible
assert "***" not in original_data

# Test 4: Flagged data is marked
assert "[FLAGGED]" in flagged_column
```

---

## Conclusion

- **One-way hashing** preserves linkability while hiding values
- **Redaction** destroys data completely for maximum privacy
- **Flag + preserve** balances access and governance

Choose based on:
1. Privacy requirements (how sensitive?)
2. Analytics needs (must we link records?)
3. Access controls (can we enforce authorization?)

This pipeline demonstrates all three in practice.
