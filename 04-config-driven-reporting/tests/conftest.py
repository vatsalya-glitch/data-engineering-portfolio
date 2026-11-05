"""Shared test helpers: a small valid pipeline that tests break one rule at a time."""

from __future__ import annotations

import copy
import sys
from pathlib import Path

import pytest
import yaml

PROJECT_DIR = Path(__file__).resolve().parent.parent
DAGS_DIR = PROJECT_DIR / "dags"
sys.path.insert(0, str(DAGS_DIR))

from report_framework.config import ConfigError, load_config  # noqa: E402

BASE_CONFIG = {
    "dag_id": "test_pipeline",
    "schedule": "0 6 * * *",
    "period": "day",
    "project": "my-project",
    "export_bucket": "my-bucket",
    "alert_email": ["data-team@example.com"],
    "sources": {
        "orders": {
            "ingest": {"uri": "gs://landing/orders/{{ period_start }}/*.parquet", "mode": "period"},
            "checks": ["SELECT COUNT(*) = 0 FROM raw.orders WHERE order_id IS NULL"],
        },
        "customers": {
            "ingest": {"uri": "gs://landing/customers/{{ period_start }}/*.parquet", "mode": "replace"},
        },
        "shipments": {
            "freshness": "SELECT MAX(event_time) >= TIMESTAMP('{{ period_end }}') FROM raw.shipments",
            "checks": ["SELECT COUNT(*) = 0 FROM raw.shipments WHERE order_id IS NULL"],
        },
    },
    "models": {
        "order_shipments": {
            "type": "intermediate",
            "uses": ["orders", "shipments"],
            "checks": ["SELECT COUNT(*) = 0 FROM mart.order_shipments WHERE order_id IS NULL"],
        },
        "delivery_report": {
            "type": "report",
            "uses": ["order_shipments", "customers"],
            "steps": ["approval", "export", "email"],
            "email_to": ["logistics@example.com"],
        },
    },
}

BASE_SQL = {
    "order_shipments": (
        "CREATE OR REPLACE TABLE mart.order_shipments AS\n"
        "SELECT o.order_id, s.carrier FROM raw.orders AS o\n"
        "JOIN raw.shipments AS s USING (order_id)\n"
        "WHERE o.order_date = DATE('{{ period_start }}')"
    ),
    "delivery_report": (
        "CREATE OR REPLACE TABLE reports.delivery_report AS\n"
        "SELECT c.region, COUNT(*) AS orders FROM mart.order_shipments AS os\n"
        "JOIN raw.customers AS c USING (customer_id) GROUP BY c.region"
    ),
}


def base_config() -> dict:
    return copy.deepcopy(BASE_CONFIG)


def base_sql() -> dict:
    return dict(BASE_SQL)


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
