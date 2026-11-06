"""Framework defaults come from report_framework/defaults.yaml, and pipelines can override them."""

from datetime import date

import pytest
import yaml

from conftest import base_config, base_sql, write_pipeline
from report_framework.config import DEFAULTS_PATH, ConfigError, load_config, load_defaults

OS = "transformation/order_shipments.sql"
DR = "reports/delivery_report.sql"


def write_defaults(tmp_path, change):
    """A copy of the real defaults.yaml with `change(dict)` applied."""
    data = yaml.safe_load(DEFAULTS_PATH.read_text())
    change(data)
    path = tmp_path / "defaults.yaml"
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    return path


def test_shipped_defaults_are_valid():
    defaults = load_defaults()
    assert defaults.datasets == {"ingestion": "raw", "transformation": "mart", "reports": "reports"}
    assert defaults.pipeline["timezone"] == "UTC"
    assert defaults.pipeline["start_date"] == date(2026, 1, 1)
    assert defaults.pipeline["retries"] == 1
    assert defaults.wait_for == {"timeout_hours": 4, "poke_interval_minutes": 10}
    assert defaults.steps == {"approval": {"timeout_hours": 8}}


def test_pipeline_without_overrides_gets_the_defaults(load):
    config = load(base_config())
    assert config.timezone == "UTC"
    assert config.gcp_conn_id == "google_cloud_default"
    assert config.smtp_conn_id == "smtp_default"
    assert (config.retries, config.retry_delay_minutes, config.max_active_runs) == (1, 5, 1)
    wait = config.ingestion["shipments"].wait_for
    assert (wait.timeout_hours, wait.poke_interval_minutes) == (4, 10)
    assert config.reports["delivery_report"].steps[0].settings == {"timeout_hours": 8}


def test_pipeline_overrides_win(load):
    config = base_config()
    config["pipeline"] |= {"timezone": "Europe/Amsterdam", "retries": 3, "max_active_runs": 2}
    config["ingestion"]["shipments"]["wait_for"]["poke_interval_minutes"] = 2
    config["reports"]["delivery_report"]["steps"][0] = {"approval": {"timeout_hours": 24}}
    loaded = load(config)
    assert loaded.timezone == "Europe/Amsterdam"
    assert loaded.retries == 3
    assert loaded.max_active_runs == 2
    assert loaded.ingestion["shipments"].wait_for.poke_interval_minutes == 2
    assert loaded.reports["delivery_report"].steps[0].settings == {"timeout_hours": 24}


def test_changing_defaults_reaches_every_pipeline(tmp_path):
    def change(data):
        data["wait_for"]["timeout_hours"] = 6
        data["pipeline"]["retries"] = 0
        data["steps"]["approval"]["timeout_hours"] = 12

    defaults = write_defaults(tmp_path, change)
    config = load_config(write_pipeline(tmp_path, base_config()), defaults_path=defaults)
    assert config.ingestion["shipments"].wait_for.timeout_hours == 6
    assert config.retries == 0
    assert config.reports["delivery_report"].steps[0].settings == {"timeout_hours": 12}


def test_dataset_names_come_from_defaults(tmp_path):
    defaults = write_defaults(tmp_path, lambda d: d["datasets"].update(transformation="staging"))
    sql = {path: text.replace("mart.", "staging.") for path, text in base_sql().items()}
    loaded = load_config(write_pipeline(tmp_path, base_config(), sql), defaults_path=defaults)
    assert loaded.transformation["order_shipments"].table == "staging.order_shipments"
    # The drift check expects the new dataset, so the old SQL no longer matches.
    with pytest.raises(ConfigError, match="staging.order_shipments"):
        load_config(write_pipeline(tmp_path, base_config()), defaults_path=defaults)


@pytest.mark.parametrize(
    "key, value, message",
    [
        ("timezone", "Mars/Olympus", "pipeline.timezone must be an IANA time zone"),
        ("start_date", "soon", "pipeline.start_date must be a date"),
        ("retries", -1, "pipeline.retries must be a whole number, 0 or more"),
        ("retries", 1.5, "pipeline.retries must be a whole number, 0 or more"),
        ("retry_delay_minutes", 0, "pipeline.retry_delay_minutes must be a positive number"),
        ("max_active_runs", 0, "pipeline.max_active_runs must be a whole number, 1 or more"),
        ("gcp_conn_id", "", "pipeline.gcp_conn_id must be a non-empty string"),
    ],
)
def test_invalid_pipeline_override(problems, key, value, message):
    config = base_config()
    config["pipeline"][key] = value
    assert any(message in p for p in problems(config))


def test_broken_defaults_list_every_problem(tmp_path):
    def change(data):
        del data["pipeline"]["timezone"]
        data["wait_for"]["timeout_hours"] = -1
        data["wait_for"]["typo_key"] = 1
        data["datasets"]["reports"] = "not a dataset"
        data["steps"]["teleport"] = {}
        data["steps"]["approval"]["timeout"] = 1
        data["extra_section"] = {}

    with pytest.raises(ConfigError) as error:
        load_defaults(write_defaults(tmp_path, change))
    found = error.value.problems
    for fragment in ["pipeline.timezone is required", "wait_for.timeout_hours must be",
                     "wait_for: unknown key 'typo_key'", "datasets.reports must be a dataset name",
                     "steps: unknown step type 'teleport'", "steps.approval: unknown key 'timeout'",
                     "unknown section 'extra_section'"]:
        assert any(fragment in p for p in found), fragment


def test_missing_defaults_file(tmp_path):
    with pytest.raises(ConfigError, match="cannot read framework defaults"):
        load_defaults(tmp_path / "missing.yaml")
