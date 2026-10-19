"""
STAGE 1: LOAD
Load raw data from CSV and JSON files into DuckDB.
Validate schema matches expectations from config/schemas.yaml.

Design principle: Simple and explicit. Each source file is loaded as-is,
with schema validation to catch upstream changes early.
"""

import csv
import json
import duckdb
import yaml
from pathlib import Path


def load_schema_config():
    """Load schema definitions from YAML config."""
    config_path = Path("config/schemas.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)


def create_database():
    """Create or connect to DuckDB database."""
    db_path = Path("duckdb/phi_pipeline.duckdb")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def validate_csv_schema(csv_path, expected_columns):
    """
    Validate that CSV headers match expected columns.
    Fail fast if schema drifts: this catches upstream changes early.
    """
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        actual_columns = reader.fieldnames

    expected_set = set(expected_columns.keys())
    actual_set = set(actual_columns)

    if expected_set != actual_set:
        missing = expected_set - actual_set
        extra = actual_set - expected_set
        msg = f"Schema mismatch in {csv_path}:\n"
        if missing:
            msg += f"  Missing columns: {missing}\n"
        if extra:
            msg += f"  Extra columns: {extra}\n"
        raise ValueError(msg)

    print(f"✓ CSV schema validated: {csv_path}")


def validate_json_schema(json_path, expected_columns):
    """
    Validate that JSON records have expected fields.
    We allow extra fields but require declared columns to exist.
    """
    with open(json_path) as f:
        records = json.load(f)

    if not records:
        raise ValueError(f"No records in {json_path}")

    # Check first record has all expected columns
    first_record = records[0]
    expected_set = set(expected_columns.keys())
    actual_set = set(first_record.keys())

    missing = expected_set - actual_set
    if missing:
        raise ValueError(
            f"Schema mismatch in {json_path}:\n"
            f"  Missing columns: {missing}"
        )

    print(f"✓ JSON schema validated: {json_path}")


def load_portal_appointments(conn, config):
    """Load portal_appointments.csv into raw.portal_appointments table."""
    spec = config["portal_appointments"]
    csv_path = Path(spec["source"])

    validate_csv_schema(csv_path, spec["columns"])

    # DuckDB can read CSV directly
    conn.execute(f"""
        CREATE OR REPLACE TABLE raw.portal_appointments AS
        SELECT * FROM read_csv_auto('{csv_path}')
    """)

    count = conn.execute(
        "SELECT COUNT(*) as cnt FROM raw.portal_appointments"
    ).fetchall()[0][0]

    print(f"  Loaded {count} rows into raw.portal_appointments")


def load_patient_records(conn, config):
    """Load patient_records.json into raw.patient_records table."""
    spec = config["patient_records"]
    json_path = Path(spec["source"])

    validate_json_schema(json_path, spec["columns"])

    # Read JSON and create table
    with open(json_path) as f:
        records = json.load(f)

    # Insert records row by row (simple and explicit)
    for record in records:
        cols = list(record.keys())
        placeholders = ", ".join(["?" for _ in cols])
        col_names = ", ".join(cols)
        values = [record[col] for col in cols]
        conn.execute(
            f"INSERT INTO raw.patient_records ({col_names}) VALUES ({placeholders})",
            values
        )

    count = conn.execute(
        "SELECT COUNT(*) as cnt FROM raw.patient_records"
    ).fetchall()[0][0]

    print(f"  Loaded {count} rows into raw.patient_records")


def create_raw_schema(conn):
    """Create raw schema and tables."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

    # Create empty tables with proper schema
    # Portal appointments: all string columns (we'll type-check in DETECT if needed)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.portal_appointments (
            appointment_id VARCHAR,
            name VARCHAR,
            department VARCHAR,
            appointment_datetime VARCHAR,
            appointment_status VARCHAR,
            registration_source VARCHAR
        )
    """)

    # Patient records: JSON array column for long_term_illnesses
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.patient_records (
            patient_id VARCHAR,
            insurance_number VARCHAR,
            name VARCHAR,
            age_group VARCHAR,
            gender VARCHAR,
            zip_code VARCHAR,
            blood_type VARCHAR,
            long_term_illnesses JSON,
            emergency_contact_name VARCHAR,
            emergency_contact_phone VARCHAR,
            appointment_id VARCHAR,
            visit_type VARCHAR,
            appointment_status VARCHAR,
            registration_source VARCHAR,
            created_at VARCHAR
        )
    """)


def main():
    """Load stage: read raw data and land in DuckDB."""
    print("\n" + "="*70)
    print("STAGE 1: LOAD")
    print("="*70)

    # Load config and database
    config = load_schema_config()
    conn = create_database()

    # Set up raw schema
    create_raw_schema(conn)

    # Load both data sources
    print("\nLoading data sources...")
    load_portal_appointments(conn, config)
    load_patient_records(conn, config)

    # Show what we have
    print("\n✓ Data load complete")
    print(f"  Database: duckdb/phi_pipeline.duckdb")
    print(f"  Schema: raw (portal_appointments, patient_records)")


if __name__ == "__main__":
    main()
