"""
PHI MASKING PIPELINE ORCHESTRATOR

Runs all four stages in sequence:
  Stage 1: LOAD   - Read raw data into DuckDB
  Stage 2: DETECT - Scan for PII and record findings
  Stage 3: MASK   - Apply masking strategies
  Stage 4: STORE  - Create analytics views and verify safety

Usage:
  python run_pipeline.py

Simple, transparent pipeline orchestration with clear output at each stage.
"""

import sys
import subprocess
from pathlib import Path


def run_stage(stage_num, script_name, description):
    """Run a single pipeline stage."""
    print(f"\n{'='*70}")
    print(f"Running {description}...")
    print(f"{'='*70}\n")

    script_path = Path("pipeline") / script_name

    if not script_path.exists():
        print(f"❌ Error: {script_path} not found")
        return False

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            check=True,
            capture_output=False
        )
        print(f"\n✓ Stage {stage_num} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Stage {stage_num} failed with error code {e.returncode}")
        return False


def main():
    """Run the complete PHI masking pipeline."""
    print("\n" + "="*70)
    print("PHI MASKING PIPELINE")
    print("="*70)
    print("\nThis pipeline demonstrates secure handling of healthcare data:")
    print("  1. LOAD   - Load raw data from CSV/JSON")
    print("  2. DETECT - Identify PII based on schema policy")
    print("  3. MASK   - Apply masking strategies (hash/redact)")
    print("  4. STORE  - Create safe analytics views")

    stages = [
        (1, "load_data.py", "Stage 1: LOAD"),
        (2, "detect_phi.py", "Stage 2: DETECT"),
        (3, "mask_data.py", "Stage 3: MASK"),
        (4, "store_data.py", "Stage 4: STORE"),
    ]

    completed = 0
    for stage_num, script, description in stages:
        if run_stage(stage_num, script, description):
            completed += 1
        else:
            print(f"\n❌ Pipeline halted at stage {stage_num}")
            return 1

    print("\n" + "="*70)
    print("✓ PIPELINE COMPLETE")
    print("="*70)
    print(f"\nAll {completed} stages completed successfully:")
    print(f"  raw schema:      Original data (with PII)")
    print(f"  masked schema:   Hashed/redacted data")
    print(f"  analytics schema: Safe views for analysis")
    print(f"\nDatabase: duckdb/phi_pipeline.duckdb")

    return 0


if __name__ == "__main__":
    sys.exit(main())
