"""
STAGE 2: DETECT
Scan raw data tables for PII (Personally Identifiable Information).
Record all findings in audit.phi_detection table for the MASK stage.

Design principle: DECLARED, not guessed. Only columns marked as pii: true
in the schema config are detected. We also include pattern-based safety nets
(e.g., regex for phone numbers) to catch undeclared PII.
"""

import re
import duckdb
import yaml
from pathlib import Path
from datetime import datetime


def load_schema_config():
    """Load schema definitions from YAML config."""
    config_path = Path("config/schemas.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)


def connect_db():
    """Connect to existing DuckDB database."""
    db_path = Path("duckdb/phi_pipeline.duckdb")
    return duckdb.connect(str(db_path))


def create_audit_schema(conn):
    """Create audit schema and tables."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS audit")

    # Table to record all PII findings
    conn.execute("""
        CREATE TABLE IF NOT EXISTS audit.phi_detection (
            detection_id VARCHAR,
            table_name VARCHAR,
            row_id VARCHAR,
            column_name VARCHAR,
            detected_value VARCHAR,
            detection_method VARCHAR,
            masking_strategy VARCHAR,
            detected_at TIMESTAMP
        )
    """)


def get_pii_columns(config, table_name):
    """Extract PII columns from schema config for a given table."""
    if table_name not in config:
        return {}

    pii_cols = {}
    for col_name, col_spec in config[table_name]["columns"].items():
        if col_spec.get("pii", False):
            pii_cols[col_name] = col_spec.get("masking_strategy", "hash_md5")

    return pii_cols


def is_phone_number(value):
    """
    Safety net: pattern-based detection for phone numbers.
    Catches phone numbers in columns not explicitly marked as PII.

    Matches formats like:
      +49-30-1234567
      030-1234567
      0301234567
      +49 30 123456 78
    """
    if not isinstance(value, str):
        return False

    # German phone pattern (project-specific, can be adapted)
    phone_pattern = r'(\+49|0)[0-9\s\-]{8,}'
    return bool(re.search(phone_pattern, value))


def detect_in_portal_appointments(conn, config):
    """Detect PII in portal_appointments table."""
    pii_cols = get_pii_columns(config, "portal_appointments")
    detection_count = 0
    detection_id = 0

    # Get all rows
    rows = conn.execute("SELECT * FROM raw.portal_appointments").fetchall()
    columns = [desc[0] for desc in conn.description]

    for row_idx, row in enumerate(rows):
        row_dict = dict(zip(columns, row))
        row_id = row_dict.get("appointment_id", f"row_{row_idx}")

        # Check declared PII columns
        for col_name, masking_strategy in pii_cols.items():
            value = row_dict.get(col_name)
            if value:
                detection_id += 1
                conn.execute("""
                    INSERT INTO audit.phi_detection VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    f"DET-{str(detection_id).zfill(6)}",
                    "portal_appointments",
                    row_id,
                    col_name,
                    str(value)[:50],  # Truncate for logging
                    "declared_pii",
                    masking_strategy,
                    datetime.now()
                ])
                detection_count += 1

        # Safety net: check for phone numbers in all columns
        for col_name, value in row_dict.items():
            if col_name not in pii_cols and is_phone_number(value):
                detection_id += 1
                conn.execute("""
                    INSERT INTO audit.phi_detection VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    f"DET-{str(detection_id).zfill(6)}",
                    "portal_appointments",
                    row_id,
                    col_name,
                    str(value)[:50],
                    "phone_regex_safety_net",
                    "hash_md5",  # Default strategy for undeclared PII
                    datetime.now()
                ])
                detection_count += 1

    print(f"  Detected {detection_count} PII values in portal_appointments")


def detect_in_patient_records(conn, config):
    """Detect PII in patient_records table."""
    pii_cols = get_pii_columns(config, "patient_records")
    detection_count = 0
    detection_id_offset = int(conn.execute(
        "SELECT MAX(CAST(SUBSTRING(detection_id, 5) AS INT)) FROM audit.phi_detection"
    ).fetchall()[0][0] or 0)

    # Get all rows
    rows = conn.execute("SELECT * FROM raw.patient_records").fetchall()
    columns = [desc[0] for desc in conn.description]

    for row_idx, row in enumerate(rows):
        row_dict = dict(zip(columns, row))
        row_id = row_dict.get("patient_id", f"row_{row_idx}")

        # Check declared PII columns
        for col_name, masking_strategy in pii_cols.items():
            value = row_dict.get(col_name)
            # Skip JSON arrays and null values
            if value and not isinstance(value, list):
                detection_id_offset += 1
                conn.execute("""
                    INSERT INTO audit.phi_detection VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    f"DET-{str(detection_id_offset).zfill(6)}",
                    "patient_records",
                    row_id,
                    col_name,
                    str(value)[:50],
                    "declared_pii",
                    masking_strategy,
                    datetime.now()
                ])
                detection_count += 1

        # Safety net: check for phone numbers in all columns
        for col_name, value in row_dict.items():
            if col_name not in pii_cols and isinstance(value, str) and is_phone_number(value):
                detection_id_offset += 1
                conn.execute("""
                    INSERT INTO audit.phi_detection VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    f"DET-{str(detection_id_offset).zfill(6)}",
                    "patient_records",
                    row_id,
                    col_name,
                    str(value)[:50],
                    "phone_regex_safety_net",
                    "hash_md5",
                    datetime.now()
                ])
                detection_count += 1

    print(f"  Detected {detection_count} PII values in patient_records")


def main():
    """Detect stage: scan raw data for PII and record findings."""
    print("\n" + "="*70)
    print("STAGE 2: DETECT")
    print("="*70)

    config = load_schema_config()
    conn = connect_db()

    # Create audit schema
    create_audit_schema(conn)

    # Scan both tables
    print("\nScanning for PII...")
    detect_in_portal_appointments(conn, config)
    detect_in_patient_records(conn, config)

    # Summary
    total = conn.execute(
        "SELECT COUNT(*) as cnt FROM audit.phi_detection"
    ).fetchall()[0][0]

    print(f"\n✓ PII detection complete")
    print(f"  Total detections: {total}")
    print(f"  Audit table: audit.phi_detection")

    # Show breakdown by strategy
    breakdown = conn.execute("""
        SELECT masking_strategy, COUNT(*) as cnt
        FROM audit.phi_detection
        GROUP BY masking_strategy
        ORDER BY cnt DESC
    """).fetchall()

    print("\n  Breakdown by masking strategy:")
    for strategy, count in breakdown:
        print(f"    {strategy}: {count}")


if __name__ == "__main__":
    main()
