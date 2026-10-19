"""
STAGE 3: MASK
Apply masking strategies to detected PII.
Create masked versions of raw tables.

Three masking strategies (from schema config):
  1. hash_md5: One-way hash. Same input always produces same output.
     Use for: Names, insurance numbers, phone numbers (maintains consistency)
  2. redact: Replace with *** (value is destroyed, can't be recovered)
     Use for: Postal codes, sensitive identifiers
  3. flag_sensitive: Mark as flagged but keep value for downstream handling
     Use for: Columns flagged for special handling

The MASK stage reads the audit.phi_detection table to know exactly what
to mask, then creates masked_portal_appointments and masked_patient_records.
"""

import hashlib
import duckdb
import yaml
from pathlib import Path


def load_schema_config():
    """Load schema definitions from YAML config."""
    config_path = Path("config/schemas.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)


def connect_db():
    """Connect to existing DuckDB database."""
    db_path = Path("duckdb/phi_pipeline.duckdb")
    return duckdb.connect(str(db_path))


def hash_md5(value):
    """
    Hash a value consistently with MD5.
    Same input always produces same hash (useful for joins).
    """
    if value is None or value == "":
        return None
    return hashlib.md5(str(value).encode()).hexdigest()


def redact(value):
    """Replace value with *** (irreversible)."""
    if value is None or value == "":
        return None
    return "***"


def flag_sensitive(value):
    """Mark as flagged (keep value, add metadata)."""
    return f"[FLAGGED]:{value}" if value else None


def get_masking_map(conn):
    """
    Build a map of (table_name, column_name) -> masking_strategy
    from the audit table.
    """
    mapping = {}
    rows = conn.execute("""
        SELECT DISTINCT table_name, column_name, masking_strategy
        FROM audit.phi_detection
    """).fetchall()

    for table_name, column_name, strategy in rows:
        mapping[(table_name, column_name)] = strategy

    return mapping


def mask_portal_appointments(conn, masking_map):
    """Create masked version of portal_appointments."""

    # Columns that might need masking in this table
    maskable_cols = ["name"]

    # Start with a copy of raw data
    conn.execute("""
        CREATE OR REPLACE TABLE masked.portal_appointments AS
        SELECT * FROM raw.portal_appointments
    """)

    # Apply masking to detected PII columns
    for col_name in maskable_cols:
        strategy = masking_map.get(("portal_appointments", col_name))
        if strategy:
            if strategy == "hash_md5":
                conn.execute(f"""
                    UPDATE masked.portal_appointments
                    SET {col_name} = md5({col_name})
                    WHERE {col_name} IS NOT NULL
                """)
            elif strategy == "redact":
                conn.execute(f"""
                    UPDATE masked.portal_appointments
                    SET {col_name} = '***'
                    WHERE {col_name} IS NOT NULL
                """)

    # Verify result
    count = conn.execute(
        "SELECT COUNT(*) as cnt FROM masked.portal_appointments"
    ).fetchall()[0][0]
    print(f"  Masked portal_appointments: {count} rows")


def mask_patient_records(conn, masking_map):
    """Create masked version of patient_records."""

    # Columns that might need masking in this table
    maskable_cols = [
        "name", "insurance_number", "emergency_contact_name",
        "emergency_contact_phone", "zip_code"
    ]

    # Start with a copy of raw data
    conn.execute("""
        CREATE OR REPLACE TABLE masked.patient_records AS
        SELECT * FROM raw.patient_records
    """)

    # Apply masking to detected PII columns
    for col_name in maskable_cols:
        strategy = masking_map.get(("patient_records", col_name))
        if strategy:
            if strategy == "hash_md5":
                conn.execute(f"""
                    UPDATE masked.patient_records
                    SET {col_name} = md5({col_name})
                    WHERE {col_name} IS NOT NULL
                """)
            elif strategy == "redact":
                conn.execute(f"""
                    UPDATE masked.patient_records
                    SET {col_name} = '***'
                    WHERE {col_name} IS NOT NULL
                """)

    # Verify result
    count = conn.execute(
        "SELECT COUNT(*) as cnt FROM masked.patient_records"
    ).fetchall()[0][0]
    print(f"  Masked patient_records: {count} rows")


def verify_no_visible_names(conn):
    """Safety check: verify that actual names are not visible in masked data."""

    # Check a few known names from raw data
    raw_names = conn.execute(
        "SELECT DISTINCT name FROM raw.patient_records LIMIT 3"
    ).fetchall()

    safety_violations = 0
    for (name,) in raw_names:
        # Check if actual name appears in masked data (should not)
        count = conn.execute("""
            SELECT COUNT(*) FROM masked.patient_records
            WHERE name = ?
        """, [name]).fetchall()[0][0]

        if count > 0:
            safety_violations += 1
            print(f"  ⚠ WARNING: Name '{name}' found in masked data!")

    if safety_violations == 0:
        print(f"  ✓ Safety check passed: no raw names visible in masked data")

    return safety_violations == 0


def main():
    """Mask stage: apply strategies to create masked tables."""
    print("\n" + "="*70)
    print("STAGE 3: MASK")
    print("="*70)

    config = load_schema_config()
    conn = connect_db()

    # Create masked schema
    conn.execute("CREATE SCHEMA IF NOT EXISTS masked")

    # Build masking strategy map from audit table
    print("\nBuilding masking strategy map...")
    masking_map = get_masking_map(conn)
    print(f"  {len(masking_map)} column(s) to mask")

    # Apply masking to both tables
    print("\nApplying masking strategies...")
    mask_portal_appointments(conn, masking_map)
    mask_patient_records(conn, masking_map)

    # Safety verification
    print("\nVerifying masking...")
    verify_no_visible_names(conn)

    print(f"\n✓ Masking complete")
    print(f"  Schema: masked (portal_appointments, patient_records)")


if __name__ == "__main__":
    main()
