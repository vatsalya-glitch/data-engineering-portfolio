"""One test per validation rule, plus the happy path."""

from datetime import date

import pytest

from conftest import DAGS_DIR, base_config, base_sql, write_pipeline
from report_framework.config import ConfigError, load_config, partition_suffix, period_bounds

OS = "transformation/order_shipments.sql"
DR = "reports/delivery_report.sql"


def has(problems: list[str], *fragments: str) -> bool:
    """True if one problem contains every fragment."""
    return any(all(f in p for f in fragments) for p in problems)


def report(config: dict) -> dict:
    return config["reports"]["delivery_report"]


# --------------------------------------------------------------------------- happy path

def test_base_config_is_valid(load):
    config = load(base_config())
    assert config.dag_id == "test_pipeline"
    orders = config.ingestion["orders"]
    assert orders.load.mode == "period" and orders.load.format == "PARQUET"
    assert orders.table == "raw.orders"
    assert config.ingestion["shipments"].wait_for.timeout_hours == 4
    assert config.transformation["order_shipments"].table == "mart.order_shipments"
    delivery = config.reports["delivery_report"]
    assert delivery.table == "reports.delivery_report"
    assert delivery.ingestion == ["customers"]
    assert delivery.transformation == ["order_shipments"]
    assert "CREATE OR REPLACE" in delivery.sql
    assert [(s.type, s.settings) for s in delivery.steps] == [
        ("approval", {"timeout_hours": 8}),
        ("export", {"bucket": "my-bucket"}),
        ("email", {"to": ["logistics@example.com"]}),
    ]
    assert config.graph["delivery_report"] == ["customers", "order_shipments"]


@pytest.mark.parametrize("name", ["daily_operations", "monthly_finance"])
def test_example_pipelines_are_valid(name):
    config = load_config(DAGS_DIR / "configs" / f"{name}.yaml")
    assert config.dag_id == name


def test_report_can_use_ingestion_tables_only(load):
    config = base_config()
    report(config)["ingestion"] = ["customers", "orders"]
    del report(config)["transformation"]
    config["transformation"] = {}
    config["ingestion"].pop("shipments")
    sql = {DR: "CREATE OR REPLACE TABLE reports.delivery_report AS "
               "SELECT * FROM raw.orders JOIN raw.customers USING (customer_id)"}
    assert load(config, sql).reports["delivery_report"].uses == ["customers", "orders"]


def test_transformation_can_use_another_transformation(load):
    config = base_config()
    config["transformation"]["enriched"] = {
        "sql": "transformation/enriched.sql",
        "ingestion": ["customers"],
        "transformation": ["order_shipments"],
    }
    report(config)["ingestion"] = []
    report(config)["transformation"] = ["enriched"]
    sql = base_sql() | {
        "transformation/enriched.sql": "CREATE OR REPLACE TABLE mart.enriched AS SELECT * "
                                       "FROM mart.order_shipments JOIN raw.customers USING (customer_id)",
        DR: "CREATE OR REPLACE TABLE reports.delivery_report AS SELECT * FROM mart.enriched",
    }
    assert load(config, sql).transformation["enriched"].uses == ["customers", "order_shipments"]


# --------------------------------------------------------------------------- structure

def test_unknown_section(problems):
    assert has(problems(base_config() | {"models": {}}), "config: unknown key 'models'")


def test_pipeline_section_required(problems):
    config = base_config()
    del config["pipeline"]
    assert has(problems(config), "pipeline is required")


def test_reports_section_required(problems):
    config = base_config()
    del config["reports"]
    assert has(problems(config), "reports is required")


def test_no_reports(problems):
    config = base_config()
    config["reports"] = {}
    assert has(problems(config), "the pipeline has no report")


def test_unknown_key_is_reported(problems):
    config = base_config()
    report(config)["uses"] = ["orders"]  # old-style key
    assert has(problems(config), "reports.delivery_report: unknown key 'uses'")


def test_name_defined_in_two_layers(problems):
    config = base_config()
    config["transformation"]["orders"] = {"sql": "transformation/orders.sql", "ingestion": ["customers"]}
    sql = base_sql() | {"transformation/orders.sql": "CREATE TABLE mart.orders AS SELECT * FROM raw.customers"}
    assert has(problems(config, sql), "'orders' is defined twice", "ingestion and transformation")


def test_name_defined_twice_in_the_same_layer(problems):
    text = """
pipeline: {dag_id: test_pipeline, schedule: "0 6 * * *", period: day, project: p,
           alert_email: [a@example.com]}
ingestion:
  orders:
    wait_for: {fresh_when: "SELECT TRUE"}
  orders:
    wait_for: {fresh_when: "SELECT FALSE"}
reports:
  r:
    sql: reports/r.sql
    ingestion: [orders]
"""
    found = problems(text, {"reports/r.sql": "CREATE OR REPLACE TABLE reports.r AS SELECT * FROM raw.orders"})
    assert has(found, "'orders' is defined twice")


# --------------------------------------------------------------------------- references between layers

def test_unknown_table(problems):
    config = base_config()
    report(config)["ingestion"].append("payments")
    assert has(problems(config), "reports.delivery_report.ingestion", "unknown table 'payments'")


def test_table_listed_under_the_wrong_layer(problems):
    config = base_config()
    report(config)["ingestion"] = ["customers", "order_shipments"]
    report(config)["transformation"] = []
    assert has(problems(config), "reports.delivery_report.ingestion",
               "'order_shipments' is defined under transformation")


def test_report_used_by_another_table(problems):
    config = base_config()
    config["reports"]["summary_report"] = {"sql": "reports/summary_report.sql",
                                           "transformation": ["delivery_report"]}
    sql = base_sql() | {"reports/summary_report.sql": "CREATE OR REPLACE TABLE reports.summary_report "
                                                      "AS SELECT * FROM reports.delivery_report"}
    assert has(problems(config, sql), "summary_report.transformation", "'delivery_report' is a report")


def test_table_using_nothing(problems):
    config = base_config()
    config["transformation"]["order_shipments"]["ingestion"] = []
    assert has(problems(config), "transformation.order_shipments: uses no tables")


def test_table_listed_twice(problems):
    config = base_config()
    report(config)["ingestion"] = ["customers", "customers"]
    assert has(problems(config), "'customers' is listed more than once")


def test_unused_ingestion_table(problems):
    config = base_config()
    config["ingestion"]["products"] = {"wait_for": {"fresh_when": "SELECT TRUE"}}
    assert has(problems(config), "ingestion.products: no report uses it")


def test_unused_transformation(problems):
    config = base_config()
    config["transformation"]["spare"] = {"sql": "transformation/spare.sql", "ingestion": ["orders"]}
    sql = base_sql() | {"transformation/spare.sql": "CREATE OR REPLACE TABLE mart.spare AS SELECT * FROM raw.orders"}
    assert has(problems(config, sql), "transformation.spare: no report uses it")


# --------------------------------------------------------------------------- SQL files

def test_sql_is_required(problems):
    config = base_config()
    del report(config)["sql"]
    assert has(problems(config), "reports.delivery_report.sql is required")


def test_sql_file_not_found(problems):
    sql = base_sql()
    del sql[DR]
    assert has(problems(base_config(), sql), "reports.delivery_report.sql: file not found")


def test_sql_must_be_a_sql_file(problems):
    config = base_config()
    report(config)["sql"] = "reports/delivery_report.txt"
    assert has(problems(config), "must be a .sql file")


def test_sql_file_not_used(problems):
    sql = base_sql() | {"reports/old_report.sql": "SELECT 1"}
    assert has(problems(base_config(), sql), "old_report.sql is not used")


# --------------------------------------------------------------------------- ingestion layer

def test_both_load_and_wait_for(problems):
    config = base_config()
    config["ingestion"]["orders"]["wait_for"] = {"fresh_when": "SELECT TRUE"}
    assert has(problems(config), "ingestion.orders", "exactly one of", "found both")


def test_neither_load_nor_wait_for(problems):
    config = base_config()
    config["ingestion"]["customers"] = {"checks": []}
    assert has(problems(config), "ingestion.customers", "exactly one of", "found neither")


def test_load_uri_must_be_gcs(problems):
    config = base_config()
    config["ingestion"]["orders"]["load"]["uri"] = "s3://landing/orders/*.parquet"
    assert has(problems(config), "ingestion.orders.load.uri", "gs://")


def test_load_mode_invalid(problems):
    config = base_config()
    config["ingestion"]["orders"]["load"]["mode"] = "append"
    assert has(problems(config), "ingestion.orders.load.mode", "'append'")


def test_load_mode_missing(problems):
    config = base_config()
    del config["ingestion"]["orders"]["load"]["mode"]
    assert has(problems(config), "ingestion.orders.load.mode", "None")


def test_load_format_cannot_be_guessed(problems):
    config = base_config()
    config["ingestion"]["orders"]["load"]["uri"] = "gs://landing/orders/{{ period_start }}/*"
    assert has(problems(config), "ingestion.orders.load.format is required")


def test_wait_for_needs_fresh_when(problems):
    config = base_config()
    config["ingestion"]["shipments"]["wait_for"] = {"timeout_hours": 2}
    assert has(problems(config), "ingestion.shipments.wait_for.fresh_when is required")


def test_wait_for_timeout_must_be_positive(problems):
    config = base_config()
    config["ingestion"]["shipments"]["wait_for"]["timeout_hours"] = 0
    assert has(problems(config), "wait_for.timeout_hours must be a positive number")


def test_wait_for_settings_are_configurable(load):
    config = base_config()
    config["ingestion"]["shipments"]["wait_for"] |= {"timeout_hours": 1.5, "poke_interval_minutes": 2}
    wait = load(config).ingestion["shipments"].wait_for
    assert (wait.timeout_hours, wait.poke_interval_minutes) == (1.5, 2)


# --------------------------------------------------------------------------- steps

def test_unknown_step(problems):
    config = base_config()
    report(config)["steps"] = ["export", "publish_to_slack"]
    assert has(problems(config), "delivery_report.steps", "unknown step 'publish_to_slack'")


def test_email_without_to(problems):
    config = base_config()
    report(config)["steps"] = ["email"]
    assert has(problems(config), "steps.email needs 'to'")


def test_email_to_must_be_addresses(problems):
    config = base_config()
    report(config)["steps"] = [{"email": {"to": "logistics"}}]
    assert has(problems(config), "steps.email.to must be a non-empty list of email addresses")


def test_export_without_bucket(problems):
    config = base_config()
    del config["pipeline"]["export_bucket"]
    assert has(problems(config), "steps.export", "needs 'bucket'")


def test_export_bucket_on_the_step(load):
    config = base_config()
    del config["pipeline"]["export_bucket"]
    report(config)["steps"] = [{"export": {"bucket": "other-bucket"}}]
    assert load(config).reports["delivery_report"].steps[0].settings == {"bucket": "other-bucket"}


def test_step_listed_twice(problems):
    config = base_config()
    report(config)["steps"] = ["export", "export"]
    assert has(problems(config), "step 'export' is listed more than once")


def test_unknown_step_setting(problems):
    config = base_config()
    report(config)["steps"] = [{"approval": {"timeout": 3}}]
    assert has(problems(config), "steps.approval: unknown key 'timeout'")


def test_approval_timeout_must_be_a_number(problems):
    config = base_config()
    report(config)["steps"] = [{"approval": {"timeout_hours": "soon"}}]
    assert has(problems(config), "approval.timeout_hours must be a positive number")


def test_badly_shaped_step(problems):
    config = base_config()
    report(config)["steps"] = [{"approval": {}, "export": {}}]
    assert has(problems(config), "each step is a name or `name: {settings}`")


def test_steps_must_be_a_list(problems):
    config = base_config()
    report(config)["steps"] = "export"
    assert has(problems(config), "steps must be a list")


def test_transformations_cannot_have_steps(problems):
    config = base_config()
    config["transformation"]["order_shipments"]["steps"] = ["export"]
    assert has(problems(config), "transformation.order_shipments: unknown key 'steps'")


# --------------------------------------------------------------------------- pipeline section

@pytest.mark.parametrize("period", ["week", None, "daily"])
def test_period_invalid(problems, period):
    config = base_config()
    config["pipeline"]["period"] = period
    assert has(problems(config), "pipeline.period must be one of")


def test_alert_email_missing(problems):
    config = base_config()
    del config["pipeline"]["alert_email"]
    assert has(problems(config), "pipeline.alert_email is required")


def test_alert_email_empty(problems):
    config = base_config()
    config["pipeline"]["alert_email"] = []
    assert has(problems(config), "pipeline.alert_email is required")


def test_project_required(problems):
    config = base_config()
    del config["pipeline"]["project"]
    assert has(problems(config), "pipeline.project is required")


# --------------------------------------------------------------------------- drift

def test_drift_table_listed_but_not_read(problems):
    config = base_config()
    report(config)["ingestion"].append("orders")
    assert has(problems(config), "delivery_report: drift", "lists ['raw.orders']", "does not read")


def test_drift_table_read_but_not_listed(problems):
    config = base_config()
    report(config)["ingestion"] = []
    assert has(problems(config), "delivery_report: drift", "reads ['raw.customers']", "doesn't list")


def test_drift_wrong_dataset(problems):
    sql = base_sql() | {DR: "CREATE OR REPLACE TABLE reports.delivery_report AS SELECT * "
                            "FROM raw.order_shipments JOIN raw.customers USING (customer_id)"}
    found = problems(base_config(), sql)
    assert has(found, "delivery_report", "reads ['raw.order_shipments']")
    assert has(found, "delivery_report", "lists ['mart.order_shipments']")


def test_drift_ignores_ctes_target_and_jinja(load):
    sql = base_sql() | {DR: """
        CREATE OR REPLACE TABLE reports.delivery_report AS
        WITH day AS (
          SELECT * FROM mart.order_shipments WHERE order_date = DATE('{{ period_start }}')
        )
        SELECT * FROM day JOIN raw.customers USING (customer_id)
    """}
    load(base_config(), sql)


def test_unparseable_sql_is_reported(problems):
    sql = base_sql() | {DR: "SELECT FROM WHERE ("}
    assert has(problems(base_config(), sql), "delivery_report", "cannot parse reports/delivery_report.sql")


# --------------------------------------------------------------------------- cycles

def test_cycle_is_reported_with_the_loop(problems):
    config = base_config()
    config["transformation"]["a"] = {"sql": "transformation/a.sql", "transformation": ["b"]}
    config["transformation"]["b"] = {"sql": "transformation/b.sql", "transformation": ["a"]}
    report(config)["transformation"].append("a")
    sql = base_sql() | {
        "transformation/a.sql": "CREATE OR REPLACE TABLE mart.a AS SELECT * FROM mart.b",
        "transformation/b.sql": "CREATE OR REPLACE TABLE mart.b AS SELECT * FROM mart.a",
        DR: "CREATE OR REPLACE TABLE reports.delivery_report AS SELECT * FROM mart.order_shipments "
            "JOIN raw.customers USING (customer_id) JOIN mart.a USING (order_id)",
    }
    assert has(problems(config, sql), "cycle in uses: a -> b -> a")


# --------------------------------------------------------------------------- all at once

def test_every_problem_is_listed_at_once(tmp_path):
    config = base_config()
    config["pipeline"]["period"] = "week"
    del config["pipeline"]["alert_email"]
    config["ingestion"]["orders"]["load"]["uri"] = "/local/orders.parquet"
    report(config)["steps"] = ["email"]
    report(config)["ingestion"].append("unknown_table")
    with pytest.raises(ConfigError) as error:
        load_config(write_pipeline(tmp_path, config))
    found = error.value.problems
    for fragment in ["period", "alert_email", "gs://", "needs 'to'", "unknown table 'unknown_table'"]:
        assert has(found, fragment), fragment
    assert "problems" in str(error.value)


def test_unreadable_yaml(tmp_path):
    path = tmp_path / "configs" / "broken.yaml"
    path.parent.mkdir()
    path.write_text("pipeline: [unclosed")
    with pytest.raises(ConfigError, match="cannot read config"):
        load_config(path)


# --------------------------------------------------------------------------- periods

def test_daily_run_on_6_nov_covers_5_nov():
    assert period_bounds("day", date(2026, 11, 6)) == (date(2026, 11, 5), date(2026, 11, 6))


def test_monthly_run_on_1_nov_covers_october():
    assert period_bounds("month", date(2026, 11, 1)) == (date(2026, 10, 1), date(2026, 11, 1))


def test_monthly_run_on_1_jan_covers_december():
    assert period_bounds("month", date(2027, 1, 1)) == (date(2026, 12, 1), date(2027, 1, 1))


def test_partition_suffix():
    assert partition_suffix("day", date(2026, 11, 5)) == "20261105"
    assert partition_suffix("month", date(2026, 10, 1)) == "202610"
