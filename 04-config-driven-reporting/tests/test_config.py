"""One test per validation rule (UC12), plus the happy path."""

from datetime import date

import pytest

from conftest import base_config, base_sql, write_pipeline
from report_framework.config import ConfigError, load_config, partition_suffix, period_bounds


def has(problems: list[str], *fragments: str) -> bool:
    """True if one problem contains every fragment."""
    return any(all(f in p for f in fragments) for p in problems)


# --------------------------------------------------------------------------- happy path

def test_base_config_is_valid(load):
    config = load(base_config())
    assert config.dag_id == "test_pipeline"
    assert config.sources["orders"].ingest.mode == "period"
    assert config.sources["orders"].ingest.format == "PARQUET"
    assert config.sources["shipments"].freshness_timeout_hours == 4
    assert config.models["delivery_report"].options["email_to"] == ["logistics@example.com"]
    assert config.models["order_shipments"].table == "mart.order_shipments"
    assert config.models["delivery_report"].table == "reports.delivery_report"
    assert config.sources["orders"].table == "raw.orders"
    assert "CREATE OR REPLACE" in config.models["delivery_report"].sql
    assert config.reports == ["delivery_report"]


@pytest.mark.parametrize("name", ["daily_operations", "monthly_finance"])
def test_example_pipelines_are_valid(name):
    from conftest import DAGS_DIR

    config = load_config(DAGS_DIR / "configs" / f"{name}.yaml")
    assert config.dag_id == name


# --------------------------------------------------------------------------- tables and names

def test_unknown_table(problems):
    config = base_config()
    config["models"]["delivery_report"]["uses"].append("payments")
    assert has(problems(config), "delivery_report", "unknown table 'payments'")


def test_name_defined_as_source_and_model(problems):
    config = base_config()
    config["models"]["orders"] = {"type": "intermediate", "uses": ["customers"]}
    sql = base_sql() | {"orders": "CREATE OR REPLACE TABLE mart.orders AS SELECT * FROM raw.customers"}
    assert has(problems(config, sql), "'orders' is defined twice")


def test_name_defined_twice_in_the_same_section(problems):
    text = """
dag_id: test_pipeline
schedule: "0 6 * * *"
period: day
project: p
alert_email: [a@example.com]
sources:
  orders:
    freshness: "SELECT TRUE"
  orders:
    freshness: "SELECT FALSE"
models:
  r:
    type: report
    uses: [orders]
"""
    found = problems(text, {"r": "CREATE OR REPLACE TABLE reports.r AS SELECT * FROM raw.orders"})
    assert has(found, "'orders' is defined twice")


def test_model_without_sql_file(problems):
    sql = base_sql()
    del sql["delivery_report"]
    assert has(problems(base_config(), sql), "delivery_report", "no SQL file")


def test_sql_file_without_model(problems):
    sql = base_sql() | {"old_report": "SELECT 1"}
    assert has(problems(base_config(), sql), "old_report.sql", "has no model")


# --------------------------------------------------------------------------- model types

def test_type_missing(problems):
    config = base_config()
    del config["models"]["order_shipments"]["type"]
    assert has(problems(config), "order_shipments", "type is required")


def test_type_invalid(problems):
    config = base_config()
    config["models"]["order_shipments"]["type"] = "staging"
    assert has(problems(config), "order_shipments", "type must be one of", "'staging'")


def test_intermediate_with_steps(problems):
    config = base_config()
    config["models"]["order_shipments"]["steps"] = ["export"]
    assert has(problems(config), "order_shipments", "intermediate cannot have steps")


def test_report_used_by_another_model(problems):
    config = base_config()
    config["models"]["summary_report"] = {"type": "report", "uses": ["delivery_report"]}
    sql = base_sql() | {
        "summary_report": "CREATE OR REPLACE TABLE reports.summary_report AS "
                          "SELECT * FROM reports.delivery_report"
    }
    assert has(problems(config, sql), "summary_report", "'delivery_report', which is a report")


def test_intermediate_can_use_another_intermediate(load):
    config = base_config()
    config["models"]["enriched"] = {"type": "intermediate", "uses": ["order_shipments", "customers"]}
    config["models"]["delivery_report"]["uses"] = ["enriched"]
    sql = base_sql() | {
        "enriched": "CREATE OR REPLACE TABLE mart.enriched AS SELECT * FROM mart.order_shipments "
                    "JOIN raw.customers USING (customer_id)",
        "delivery_report": "CREATE OR REPLACE TABLE reports.delivery_report AS SELECT * FROM mart.enriched",
    }
    assert load(config, sql).models["enriched"].uses == ["order_shipments", "customers"]


# --------------------------------------------------------------------------- sources

def test_source_with_both_ingest_and_freshness(problems):
    config = base_config()
    config["sources"]["orders"]["freshness"] = "SELECT TRUE"
    assert has(problems(config), "sources.orders", "exactly one of", "found both")


def test_source_with_neither_ingest_nor_freshness(problems):
    config = base_config()
    config["sources"]["customers"] = {"checks": []}
    assert has(problems(config), "sources.customers", "exactly one of", "found neither")


def test_ingest_uri_must_be_gcs(problems):
    config = base_config()
    config["sources"]["orders"]["ingest"]["uri"] = "s3://landing/orders/*.parquet"
    assert has(problems(config), "sources.orders.ingest.uri", "gs://")


def test_ingest_mode_invalid(problems):
    config = base_config()
    config["sources"]["orders"]["ingest"]["mode"] = "append"
    assert has(problems(config), "sources.orders.ingest.mode", "'append'")


def test_ingest_mode_missing(problems):
    config = base_config()
    del config["sources"]["orders"]["ingest"]["mode"]
    assert has(problems(config), "sources.orders.ingest.mode", "None")


def test_ingest_format_cannot_be_guessed(problems):
    config = base_config()
    config["sources"]["orders"]["ingest"]["uri"] = "gs://landing/orders/{{ period_start }}/*"
    assert has(problems(config), "sources.orders.ingest.format is required")


def test_freshness_timeout_must_be_positive(problems):
    config = base_config()
    config["sources"]["shipments"]["freshness_timeout_hours"] = 0
    assert has(problems(config), "freshness_timeout_hours", "positive")


def test_freshness_timeout_is_configurable(load):
    config = base_config()
    config["sources"]["shipments"]["freshness_timeout_hours"] = 1.5
    assert load(config).sources["shipments"].freshness_timeout_hours == 1.5


# --------------------------------------------------------------------------- steps

def test_unknown_step(problems):
    config = base_config()
    config["models"]["delivery_report"]["steps"] = ["export", "publish_to_slack"]
    assert has(problems(config), "delivery_report", "unknown step 'publish_to_slack'")


def test_email_without_email_to(problems):
    config = base_config()
    del config["models"]["delivery_report"]["email_to"]
    assert has(problems(config), "delivery_report", "'email' needs 'email_to'")


def test_export_without_export_bucket(problems):
    config = base_config()
    del config["export_bucket"]
    assert has(problems(config), "delivery_report", "'export' needs 'export_bucket'")


def test_step_listed_twice(problems):
    config = base_config()
    config["models"]["delivery_report"]["steps"] = ["export", "export"]
    assert has(problems(config), "'export' is listed more than once")


def test_approval_timeout_must_be_a_number(problems):
    config = base_config()
    config["models"]["delivery_report"]["approval_timeout_hours"] = "soon"
    assert has(problems(config), "approval_timeout_hours must be a positive number")


# --------------------------------------------------------------------------- pipeline

@pytest.mark.parametrize("period", ["week", None, "daily"])
def test_period_invalid(problems, period):
    config = base_config()
    config["period"] = period
    assert has(problems(config), "period must be one of")


def test_alert_email_missing(problems):
    config = base_config()
    del config["alert_email"]
    assert has(problems(config), "alert_email is required")


def test_alert_email_empty(problems):
    config = base_config()
    config["alert_email"] = []
    assert has(problems(config), "alert_email is required")


def test_unknown_key_is_reported(problems):
    config = base_config()
    config["models"]["delivery_report"]["use"] = ["orders"]  # typo for "uses"
    assert has(problems(config), "models.delivery_report", "unknown key 'use'")


# --------------------------------------------------------------------------- drift (UC4)

def test_drift_table_in_uses_but_not_in_sql(problems):
    config = base_config()
    config["models"]["delivery_report"]["uses"].append("orders")
    assert has(problems(config), "delivery_report", "drift", "uses ['raw.orders']",
               "SQL does not read them")


def test_drift_table_in_sql_but_not_in_uses(problems):
    config = base_config()
    config["models"]["delivery_report"]["uses"] = ["order_shipments"]
    assert has(problems(config), "delivery_report", "drift", "reads ['raw.customers']",
               "not in uses")


def test_drift_wrong_dataset(problems):
    sql = base_sql() | {
        "delivery_report": "CREATE OR REPLACE TABLE reports.delivery_report AS "
                           "SELECT * FROM raw.order_shipments JOIN raw.customers USING (customer_id)"
    }
    found = problems(base_config(), sql)
    assert has(found, "delivery_report", "reads ['raw.order_shipments']")
    assert has(found, "delivery_report", "uses ['mart.order_shipments']")


def test_drift_ignores_ctes_target_and_jinja(load):
    sql = base_sql() | {
        "delivery_report": """
            CREATE OR REPLACE TABLE reports.delivery_report AS
            WITH day AS (
              SELECT * FROM mart.order_shipments WHERE order_date = DATE('{{ period_start }}')
            )
            SELECT * FROM day JOIN raw.customers USING (customer_id)
        """
    }
    load(base_config(), sql)


def test_unparseable_sql_is_reported(problems):
    sql = base_sql() | {"delivery_report": "SELECT FROM WHERE ("}
    assert has(problems(base_config(), sql), "delivery_report", "cannot parse SQL")


# --------------------------------------------------------------------------- cycles (UC3)

def test_cycle_is_reported_with_the_loop(problems):
    config = base_config()
    config["models"]["a"] = {"type": "intermediate", "uses": ["b"]}
    config["models"]["b"] = {"type": "intermediate", "uses": ["a"]}
    sql = base_sql() | {
        "a": "CREATE OR REPLACE TABLE mart.a AS SELECT * FROM mart.b",
        "b": "CREATE OR REPLACE TABLE mart.b AS SELECT * FROM mart.a",
    }
    assert has(problems(config, sql), "a -> b -> a")


# --------------------------------------------------------------------------- all at once

def test_every_problem_is_listed_at_once(tmp_path):
    config = base_config()
    config["period"] = "week"
    del config["alert_email"]
    config["sources"]["orders"]["ingest"]["uri"] = "/local/orders.parquet"
    config["models"]["order_shipments"]["steps"] = ["email"]
    config["models"]["delivery_report"]["uses"].append("unknown_table")
    with pytest.raises(ConfigError) as error:
        load_config(write_pipeline(tmp_path, config))
    found = error.value.problems
    assert len(found) >= 5
    for fragment in ["period", "alert_email", "gs://", "intermediate cannot have steps",
                     "unknown table 'unknown_table'"]:
        assert has(found, fragment), fragment
    assert "problems" in str(error.value)


def test_unreadable_yaml(tmp_path):
    path = tmp_path / "configs" / "broken.yaml"
    path.parent.mkdir()
    path.write_text("dag_id: [unclosed")
    with pytest.raises(ConfigError, match="cannot read config"):
        load_config(path)


# --------------------------------------------------------------------------- periods (UC9)

def test_daily_run_on_6_nov_covers_5_nov():
    assert period_bounds("day", date(2026, 11, 6)) == (date(2026, 11, 5), date(2026, 11, 6))


def test_monthly_run_on_1_nov_covers_october():
    assert period_bounds("month", date(2026, 11, 1)) == (date(2026, 10, 1), date(2026, 11, 1))


def test_monthly_run_on_1_jan_covers_december():
    assert period_bounds("month", date(2027, 1, 1)) == (date(2026, 12, 1), date(2027, 1, 1))


def test_partition_suffix():
    assert partition_suffix("day", date(2026, 11, 5)) == "20261105"
    assert partition_suffix("month", date(2026, 10, 1)) == "202610"
