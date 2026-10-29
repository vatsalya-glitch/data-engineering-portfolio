"""Table config shared by ingestion and freshness. Paths are relative to the project root."""

from pathlib import Path

import yaml

TABLES = yaml.safe_load(Path("config/tables.yaml").read_text())
