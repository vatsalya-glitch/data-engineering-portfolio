# PHI Masking Pipeline

**An educational project focused on data masking techniques.** Learn how to identify, transform, and safely share sensitive information through practical masking strategies.

This pipeline demonstrates three fundamental masking approaches using realistic healthcare data, showing how each technique solves different privacy challenges.

## The Core: Three Masking Techniques

This project is built around teaching **three distinct masking strategies**:

| Technique | Method | Use Case | Example |
|-----------|--------|----------|---------|
| **hash_md5** | One-way hash | Linkable data (names, IDs, phone) | `Anna Mueller` → `61d1282a...` |
| **redact** | Destroy with *** | Non-linkable data (zip codes) | `10115` → `***` |
| **flag_sensitive** | Mark + preserve | Special handling | `value` → `[FLAGGED]:value` |

## How It Works

The pipeline demonstrates these techniques in action:

```
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 1: LOAD                                                   │
│ Read CSV/JSON data and validate against schema                  │
├─────────────────────────────────────────────────────────────────┤
│ STAGE 2: DETECT                                                 │
│ Scan for PII using schema policy (declared, not guessed)        │
├─────────────────────────────────────────────────────────────────┤
│ STAGE 3: MASK                                                   │
│ Apply masking strategies (hash_md5, redact, flag_sensitive)     │
├─────────────────────────────────────────────────────────────────┤
│ STAGE 4: STORE                                                  │
│ Create safe analytics views and verify no PII leakage           │
└─────────────────────────────────────────────────────────────────┘
```

## Key Concepts

### Single Source of Truth: config/schemas.yaml

All policy decisions are declared in one place:
- Which columns are PII
- What masking strategy to use (hash_md5, redact, etc.)
- Column types and descriptions

The pipeline reads this file to:
- Validate incoming data schema
- Detect PII (not hardcoded in code)
- Apply correct masking strategies

## Deep Dive: Masking Techniques

### Technique 1: One-Way Hashing (hash_md5)

**What it does:** Converts sensitive data to an irreversible hash while maintaining consistency.

```
Input:  "Anna Mueller"  (readable)
Output: "61d1282a..."   (hashed, same every time)
```

**Key properties:**
- ✅ **Deterministic** - Same input always produces same hash
- ✅ **Irreversible** - Cannot recover original from hash
- ✅ **Linkable** - Enables joins across tables
- ❌ **Rainbow table risk** - Simple values (names) can be reverse-engineered

**When to use:**
- Names (person identifiers need to be linkable)
- Insurance numbers (maintaining referential integrity)
- Phone numbers (consistent across records)

**Real example:**
```
Raw data:        anna Mueller, anna MUELLER, ANNA mueller
After hash:      61d1282a..., 61d1282a..., 61d1282a...
Result:          All variations collapse to same ID ✓
```

---

### Technique 2: Redaction (Replace with ***)

**What it does:** Permanently destroys sensitive data, replacing it with a mask.

```
Input:  "10115"     (readable postal code)
Output: "***"       (irreversible destruction)
```

**Key properties:**
- ✅ **Irreversible** - Value is completely destroyed
- ✅ **No inference risk** - Cannot pattern-match or reverse
- ❌ **Not linkable** - Cannot join records on this column
- ❌ **Information loss** - Analytics lose location patterns

**When to use:**
- Postal codes (when location privacy is critical)
- Specific identifying information (addresses, dates)
- Data you want completely gone, not just hidden

**Real example:**
```
Raw data:        Zip: 10115, 10115, 10243, 10115
After redact:    Zip: ***, ***, ***, ***
Result:          Location information is gone ✓
```

---

### Technique 3: Flag + Preserve (Special Handling)

**What it does:** Marks sensitive data while preserving it for authorized downstream processing.

```
Input:  "emergency_contact_phone"
Output: "[FLAGGED]:emergency_contact_phone"
```

**Key properties:**
- ✅ **Preserves value** - Data still available if needed
- ✅ **Audit trail** - Clearly marked for compliance
- ❌ **Requires governance** - Needs access controls downstream
- ❌ **Not safe for sharing** - Cannot be given to analysts

**When to use:**
- Emergency contact information (keep for medical emergencies)
- Data needing special approval (audit required before access)
- Compliance scenarios (regulatory exceptions)

**Real example:**
```
Raw data:        +49-30-1234567
After flagging:  [FLAGGED]:+49-30-1234567
Access:          Only authorized team can see
```

---

## Choosing the Right Technique

```
Is the data needed for analysis?
  ├─ YES, must match across records?
  │   └─→ Use ONE-WAY HASH (names, IDs)
  │
  ├─ NO, completely remove it?
  │   └─→ Use REDACTION (locations, dates)
  │
  └─ MAYBE, special approval needed?
      └─→ Use FLAG + PRESERVE (emergency data)
```

### Pattern-Based Safety Nets

Beyond declared PII, the pipeline includes:
- **Phone regex detection** - Catches phone numbers in any column (safety net for undeclared PII)
- **Audit trail** - Records all PII detections and how they were handled

## Project Structure

```
├── config/
│   └── schemas.yaml           # Single source of truth
├── data/
│   ├── portal_appointments.csv    # Raw input data
│   └── patient_records.json       # Raw input data
├── pipeline/
│   ├── load_data.py          # Stage 1
│   ├── detect_phi.py         # Stage 2
│   ├── mask_data.py          # Stage 3
│   └── store_data.py         # Stage 4
├── duckdb/
│   └── phi_pipeline.duckdb   # Database (gitignored)
├── run_pipeline.py           # Orchestrator
├── requirements.txt          # Dependencies
└── README.md                 # This file
```

## Database Schema

### raw schema (original data with PII)
- `raw.portal_appointments` - Booked appointments from patient portal
- `raw.patient_records` - Complete patient information

### masked schema (hashed/redacted)
- `masked.portal_appointments` - Names hashed
- `masked.patient_records` - Names, insurance, phone, zip codes masked

### audit schema (detection log)
- `audit.phi_detection` - All PII findings with detection method and strategy

### analytics schema (safe for sharing)
- `analytics.appointments` - Safe columns only (no names, no identifiers)
- `analytics.patients` - Demographics safe for analysis

## Running the Pipeline

### Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Generate sample data
python scripts/generate_data.py

# Run complete pipeline
python run_pipeline.py
```

### Individual Stages

```bash
# Stage 1: Load
python pipeline/load_data.py

# Stage 2: Detect
python pipeline/detect_phi.py

# Stage 3: Mask
python pipeline/mask_data.py

# Stage 4: Store
python pipeline/store_data.py
```

## Sample Data

The project includes `scripts/generate_data.py` which creates realistic hospital data:
- **96 portal appointments** - Booked appointments (some no-shows)
- **64 patient records** - 60 new patients from portal, 4 emergency walk-ins
- **Realistic messiness** - Inconsistent name casing, missing fields, multiple phone formats

Data is seeded (random.seed(42)) for reproducibility.

## Sample Results

### PII Detections
```
Total: 460 detections
  - hash_md5: 396 values (names, insurance, phone, contact names)
  - redact: 64 values (postal codes)
```

### Analytics (safe to share)
```
Appointment Status:
  showed_up: 86 (89.6%)
  no_show: 10 (10.4%)

Visit Types:
  scheduled: 60
  emergency: 4

Age Groups:
  0-18: 7
  19-29: 8
  30-40: 9
  41-50: 16
  51-60: 11
  61-70: 7
  71+: 6
```

## Design Philosophy

### Keep It Simple

- **Declarative, not algorithmic** - Policy lives in YAML, not hidden in code
- **No complex transformations** - Load, detect, mask, store (one thing per stage)
- **Explicit masking decisions** - Every masked column is intentional
- **Fail fast on drift** - Schema validation catches changes immediately

### Security Principles

1. **Single source of truth** - All PII policy in one YAML file
2. **Audit trail** - Every detection is recorded
3. **Defense in depth** - Pattern-based safety nets beyond declared PII
4. **Separation of concerns** - raw → masked → analytics (three layers)
5. **Verifiable safety** - Final stage checks for PII leakage

## Learning Outcomes

After working through this project, you'll understand:

1. **Masking strategy selection** - How to choose the right technique for each data type
2. **Tradeoffs** - What you gain and lose with each approach
3. **Referential integrity** - Why hashing maintains linkability
4. **One-way functions** - How hashing differs from encryption
5. **Data destruction** - When and why to use irreversible masking
6. **Governance** - Flagging data that needs special handling
7. **Practical implementation** - Writing masking logic in Python/SQL
8. **Verification** - Testing that sensitive data is actually masked

## Real-World Considerations

### What This Project Covers
- ✅ Basic PII identification and masking
- ✅ Audit trails for compliance
- ✅ Schema validation and consistency
- ✅ Safe analytics views

### What You'd Add for Production
- Encryption (at rest and in transit)
- Access control (role-based, column-level)
- More sophisticated pattern detection
- Formal consent/legal review workflows
- Retention policies and deletion workflows
- Integration with compliance frameworks (HIPAA, GDPR, etc.)

## Technologies

- **DuckDB** - Persistent SQL database in a single file
- **Python** - Pipeline orchestration
- **YAML** - Configuration and policy
- **Regex** - Pattern-based safety nets

## License

Educational use. See LICENSE file.

## Contributing

This is a learning project. Issues and suggestions are welcome.
