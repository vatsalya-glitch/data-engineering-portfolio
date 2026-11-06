"""Framework defaults come from report_framework/defaults.yaml, and pipelines can override them."""

from datetime import date

import pytest
import yaml

from conftest import base_config, base_sql, write_pipeline
from report_framework.config import DEFAULTS_PATH, ConfigError, load_config, load_defaults


def write_defaults(tmp_path, change) -> str:
    """A copy of the real defaults.yaml with `change(dict)` applied."""
    data = yaml.safe_load(DEFAULTS_PATH.read_text())
    change(data)
    path = tmp_path / "defaults.yaml"
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    return path


def test_shipped_defaults_are_valid():
    defaults = load_defaults()
    assert defaults.datasets == {"source": "raw", "intermediate": "mart", "report": "reports"}
    assert defaults.pipeline["timezone"] == "UTC"
    assert defaults.pipeline["start_date"] == date(2026, 1, 1)
    assert defaults.pipeline["retries"] == 1
    assert defaults.source == {"freshness_timeout_hours": 4, "poke_interval_minutes": 10}
    assert defaults.report == {"approval_timeout_hours": 8}


def test_pipeline_without_overrides_gets_the_defaults(load):
    config = load(base_config())
    assert config.timezone == "UTC"
    assert config.gcp_conn_id == "google_cloud_default"
    assert config.smtp_conn_id == "smtp_default"
    assert config.retries == 1
    assert config.retry_delay_minutes == 5
    assert config.max_active_runs == 1
    assert config.sources["shipments"].freshness_timeout_hours == 4
    assert config.sources["shipments"].poke_interval_minutes == 10
    assert config.models["delivery_report"].options["approval_timeout_hours"] == 8


def test_pipeline_overrides_win(load):
    config = base_config()
    config |= {"timezone": "Europe/Amsterdam", "retries": 3, "max_active_runs": 2}
    config["sources"]["shipments"]["poke_interval_minutes"] = 2
    config["models"]["delivery_report"]["approval_timeout_hours"] = 24
    loaded = load(config)
    assert loaded.timezone == "Europe/Amsterdam"
    assert loaded.retries == 3
    assert loaded.max_active_runs == 2
    assert loaded.sources["shipments"].poke_interval_minutes == 2
    assert loaded.sources["orders"].poke_interval_minutes == 10
    assert loaded.models["delivery_report"].options["approval_timeout_hours"] == 24


def test_changing_defaults_reaches_every_pipeline(tmp_path):
    def change(data):
        data["source"]["freshness_timeout_hours"] = 6
        data["pipeline"]["retries"] = 0

    defaults = write_defaults(tmp_path, change)
    config = load_config(write_pipeline(tmp_path, base_config()), defaults_path=defaults)
    assert config.sources["shipments"].freshness_timeout_hours == 6
    assert config.retries == 0


def test_dataset_names_come_from_defaults(tmp_path):
    defaults = write_defaults(tmp_path, lambda d: d["datasets"].update(intermediate="staging"))
    config = base_config()
    sql = base_sql()
    sql["order_shipments"] = sql["order_shipments"].replace("mart.", "staging.")
    sql["delivery_report"] = sql["delivery_report"].replace("mart.", "staging.")
    loaded = load_config(write_pipeline(tmp_path, config, sql), defaults_path=defaults)
    assert loaded.models["order_shipments"].table == "staging.order_shipments"
    # The drift check expects the new dataset, so the old SQL no longer matches.
    with pytest.raises(ConfigError, match="staging.order_shipments"):
        load_config(write_pipeline(tmp_path, config, base_sql()), defaults_path=defaults)


@pytest.mark.parametrize(
    "key, value, message",
    [
        ("timezone", "Mars/Olympus", "timezone must be an IANA time zone"),
        ("start_date", "soon", "start_date must be a date"),
        ("retries", -1, "retries must be a whole number, 0 or more"),
        ("retries", 1.5, "retries must be a whole number, 0 or more"),
        ("retry_delay_minutes", 0, "retry_delay_minutes must be a positive number"),
        ("max_active_runs", 0, "max_active_runs must be a whole number, 1 or more"),
        ("gcp_conn_id", "", "gcp_conn_id must be a non-empty string"),
    ],
)
def test_invalid_pipeline_override(problems, key, value, message):
    config = base_config() | {key: value}
    assert any(message in p for p in problems(config))


def test_invalid_source_override(problems):
    config = base_config()
    config["sources"]["shipments"]["poke_interval_minutes"] = "often"
    assert any("sources.shipments.poke_interval_minutes must be a positive number" in p
               for p in problems(config))


def test_broken_defaults_list_every_problem(tmp_path):
    def change(data):
        del data["pipeline"]["timezone"]
        data["source"]["freshness_timeout_hours"] = -1
        data["source"]["typo_key"] = 1
        data["datasets"]["report"] = "not a dataset"
        data["extra_section"] = {}

    with pytest.raises(ConfigError) as error:
        load_defaults(write_defaults(tmp_path, change))
    found = error.value.problems
    for fragment in ["pipeline.timezone is required", "source.freshness_timeout_hours must be",
                     "source: unknown key 'typo_key'", "datasets.report must be a dataset name",
                     "unknown section 'extra_section'"]:
        assert any(fragment in p for p in found), fragment


def test_missing_defaults_file(tmp_path):
    with pytest.raises(ConfigError, match="cannot read framework defaults"):
        load_defaults(tmp_path / "missing.yaml")
