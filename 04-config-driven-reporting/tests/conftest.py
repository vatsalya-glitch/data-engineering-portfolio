"""Shared test helpers: a small valid pipeline that tests break one rule at a time."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

PROJECT_DIR = Path(__file__).resolve().parent.parent
DAGS_DIR = PROJECT_DIR / "dags"
sys.path.insert(0, str(DAGS_DIR))

from report_framework.config import ConfigError, load_config  # noqa: E402

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
BASE_CONFIG_PATH = FIXTURES_DIR / "test_pipeline.yaml"
BASE_SQL_DIR = FIXTURES_DIR / "sql" / "test_pipeline"


def base_config() -> dict:
    """A fresh copy of fixtures/test_pipeline.yaml, as a dict tests can change."""
    return yaml.safe_load(BASE_CONFIG_PATH.read_text())


def base_sql() -> dict[str, str]:
    """model name -> SQL, from fixtures/sql/test_pipeline/."""
    return {path.stem: path.read_text() for path in sorted(BASE_SQL_DIR.glob("*.sql"))}


def write_pipeline(root: Path, config: dict | str, sql: dict[str, str] | None = None) -> Path:
    """Write configs/<dag_id>.yaml and sql/<dag_id>/<model>.sql under `root`."""
    dag_id = config.get("dag_id", "test_pipeline") if isinstance(config, dict) else "test_pipeline"
    config_dir = root / "configs"
    sql_dir = root / "sql" / str(dag_id)
    config_dir.mkdir(parents=True, exist_ok=True)
    sql_dir.mkdir(parents=True, exist_ok=True)
    path = config_dir / f"{dag_id}.yaml"
    path.write_text(config if isinstance(config, str) else yaml.safe_dump(config, sort_keys=False))
    for name, text in (base_sql() if sql is None else sql).items():
        (sql_dir / f"{name}.sql").write_text(text)
    return path


@pytest.fixture
def problems(tmp_path):
    """problems(config, sql) -> the list of problems load_config reports ([] if valid)."""

    def run(config: dict | str, sql: dict[str, str] | None = None) -> list[str]:
        path = write_pipeline(tmp_path, config, sql)
        try:
            load_config(path)
        except ConfigError as error:
            return error.problems
        return []

    return run


@pytest.fixture
def load(tmp_path):
    """load(config, sql) -> PipelineConfig (raises ConfigError)."""

    def run(config: dict, sql: dict[str, str] | None = None):
        return load_config(write_pipeline(tmp_path, config, sql))

    return run
