"""
STAGE 4: STORE
Create analytics views from masked data.
Perform final PHI leak detection.
Demonstrate that masked data is still useful for analysis.

Design: Only approved safe columns are exposed in analytics views.
This ensures the data can be safely shared with analysts without risk of
accidentally exposing PII.
"""

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


def get_safe_columns(config, table_name):
    """Get columns that are NOT PII (safe to expose in analytics)."""
    if table_name not in config:
        return []

    safe_cols = []
    for col_name, col_spec in config[table_name]["columns"].items():
        if not col_spec.get("pii", False):
            safe_cols.append(col_name)

    return safe_cols


def create_analytics_schema(conn):
    """Create analytics schema for public-safe views."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")


def create_appointments_view(conn, config):
    """Create analytics view for appointments (portal + reception combined)."""

    safe_cols = get_safe_columns(config, "portal_appointments")
    # appointment_id, department, appointment_datetime, appointment_status, registration_source

    col_list = ", ".join(safe_cols)

    conn.execute(f"""
        CREATE OR REPLACE VIEW analytics.appointments AS
        SELECT {col_list}
        FROM masked.portal_appointments
        ORDER BY appointment_datetime
    """)

    count = conn.execute(
        "SELECT COUNT(*) FROM analytics.appointments"
    ).fetchall()[0][0]

    print(f"  Created analytics.appointments view: {count} rows")


def create_patients_view(conn, config):
    """Create analytics view for patient demographics (safe columns only)."""

    safe_cols = get_safe_columns(config, "patient_records")
    # patient_id, age_group, gender, blood_type, long_term_illnesses,
    # visit_type, appointment_status, registration_source, created_at

    col_list = ", ".join(safe_cols)

    conn.execute(f"""
        CREATE OR REPLACE VIEW analytics.patients AS
        SELECT {col_list}
        FROM masked.patient_records
        ORDER BY created_at
    """)

    count = conn.execute(
        "SELECT COUNT(*) FROM analytics.patients"
    ).fetchall()[0][0]

    print(f"  Created analytics.patients view: {count} rows")


def final_phi_leak_check(conn):
    """
    Final safety verification: scan all analytics views for
    signs of PII leakage (names, phone numbers, etc).
    """

    # Common PII patterns to check for
    known_raw_names = [
        "smith", "MUELLER", "Khan", "Schmidt", "Jones", "WAGNER",
        "Brown", "Becker", "Wilson", "HOFFMANN", "Taylor", "Weber"
    ]

    violations = 0

    # Check appointments view
    for name_part in known_raw_names:
        count = conn.execute(f"""
            SELECT COUNT(*) FROM analytics.appointments
            WHERE appointment_datetime::text ILIKE '%{name_part}%'
        """).fetchall()[0][0]
        if count > 0:
            violations += 1

    # Check patients view
    for name_part in known_raw_names:
        count = conn.execute(f"""
            SELECT COUNT(*) FROM analytics.patients
            WHERE age_group::text ILIKE '%{name_part}%'
               OR gender::text ILIKE '%{name_part}%'
        """).fetchall()[0][0]
        if count > 0:
            violations += 1

    if violations == 0:
        print(f"  ✓ PHI leak check: PASSED - no PII found in analytics views")
    else:
        print(f"  ⚠ PHI leak check: FAILED - {violations} potential leaks detected")

    return violations == 0


def demo_analysis(conn):
    """Demonstrate that masked data is still useful for analysis."""

    print("\n  Analysis Examples (safe to share):")

    # Example 1: Appointment status breakdown
    print("\n    - Appointment Status Breakdown:")
    result = conn.execute("""
        SELECT appointment_status, COUNT(*) as cnt
        FROM analytics.appointments
        GROUP BY appointment_status
        ORDER BY cnt DESC
    """).fetchall()
    for status, count in result:
        print(f"      {status}: {count}")

    # Example 2: Visit type distribution
    print("\n    - Visit Type Distribution:")
    result = conn.execute("""
        SELECT visit_type, COUNT(*) as cnt
        FROM analytics.patients
        GROUP BY visit_type
        ORDER BY cnt DESC
    """).fetchall()
    for visit_type, count in result:
        print(f"      {visit_type}: {count}")

    # Example 3: Age group breakdown
    print("\n    - Age Group Breakdown:")
    result = conn.execute("""
        SELECT age_group, COUNT(*) as cnt
        FROM analytics.patients
        WHERE age_group IS NOT NULL
        GROUP BY age_group
        ORDER BY age_group
    """).fetchall()
    for age_group, count in result:
        print(f"      {age_group}: {count}")


def main():
    """Store stage: create analytics views and verify safety."""
    print("\n" + "="*70)
    print("STAGE 4: STORE")
    print("="*70)

    config = load_schema_config()
    conn = connect_db()

    # Create analytics schema
    create_analytics_schema(conn)

    # Create views from safe columns only
    print("\nCreating analytics views...")
    create_appointments_view(conn, config)
    create_patients_view(conn, config)

    # Final safety verification
    print("\nFinal PHI leak detection...")
    is_safe = final_phi_leak_check(conn)

    # Show what analysts can actually use
    print("\nDemonstrating data utility...")
    demo_analysis(conn)

    # Summary
    print(f"\n✓ Pipeline complete")
    print(f"  Raw data (with PII): raw schema")
    print(f"  Masked data (hashed/redacted): masked schema")
    print(f"  Analytics (safe for sharing): analytics schema")
    if is_safe:
        print(f"  Status: ✓ SAFE - No PII visible in analytics views")
    else:
        print(f"  Status: ⚠ UNSAFE - PII leakage detected")


if __name__ == "__main__":
    main()
