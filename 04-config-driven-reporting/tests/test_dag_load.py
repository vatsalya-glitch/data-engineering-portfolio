"""Load the example pipelines with Airflow and check the tasks they produce."""

from __future__ import annotations

import os
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow-home-"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")

pytest.importorskip("airflow", reason="Airflow is not installed")

from airflow.models.dagbag import DagBag  # noqa: E402

from conftest import DAGS_DIR, base_config, write_pipeline  # noqa: E402
from report_framework.builder import build_dag, failure_alert, make_period_macro, template  # noqa: E402
from report_framework.config import load_config  # noqa: E402

UTC = timezone.utc


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


@pytest.fixture(scope="module")
def daily(dagbag):
    return dagbag.dags["daily_operations"]


@pytest.fixture(scope="module")
def monthly(dagbag):
    return dagbag.dags["monthly_finance"]


def ancestors(dag, task_id: str) -> set[str]:
    return {t.task_id for t in dag.get_task(task_id).get_flat_relatives(upstream=True)}


def groups_upstream(dag, task_id: str) -> set[str]:
    """Names of the sources/models a task waits for, directly or indirectly."""
    return {tid.split(".")[0] for tid in ancestors(dag, task_id)}


def downstream_ids(dag, task_id: str) -> set[str]:
    return set(dag.get_task(task_id).downstream_task_ids)


# --------------------------------------------------------------------------- loading

def test_both_example_pipelines_load_without_errors(dagbag):
    assert dagbag.import_errors == {}
    assert {"daily_operations", "monthly_finance"} <= set(dagbag.dag_ids)


def test_framework_package_is_not_parsed_as_dags(dagbag):
    assert not any("report_framework" in path for path in dagbag.file_last_changed)


# --------------------------------------------------------------------------- sources (UC5, UC6)

def test_ingest_source_chain(daily):
    assert downstream_ids(daily, "orders.ingest") == {"orders.check_1", "orders.check_2"}
    assert "orders.wait_fresh" not in daily.task_ids


def test_freshness_source_chain(daily):
    assert downstream_ids(daily, "shipments.wait_fresh") == {"shipments.check_1"}
    assert "shipments.ingest" not in daily.task_ids


def test_source_without_checks_feeds_models_directly(daily):
    assert "returns_report.build" in downstream_ids(daily, "returns.ingest")


def test_period_mode_writes_one_partition_with_truncate(daily):
    load = daily.get_task("orders.ingest").configuration["load"]
    assert load["writeDisposition"] == "WRITE_TRUNCATE"
    assert "period_partition" in load["destinationTable"]["tableId"]
    assert load["timePartitioning"] == {"type": "DAY", "field": "order_date"}


def test_replace_mode_replaces_the_whole_table(daily):
    load = daily.get_task("customers.ingest").configuration["load"]
    assert load["writeDisposition"] == "WRITE_TRUNCATE"
    assert load["destinationTable"]["tableId"] == "customers"
    assert "timePartitioning" not in load


def test_monthly_period_uses_month_partitions(monthly):
    load = monthly.get_task("invoices.ingest").configuration["load"]
    assert load["timePartitioning"]["type"] == "MONTH"


def test_freshness_sensor_settings(daily):
    sensor = daily.get_task("shipments.wait_fresh")
    assert sensor.mode == "reschedule"
    assert sensor.timeout == timedelta(hours=4).total_seconds()
    assert sensor.poke_interval == timedelta(minutes=10).total_seconds()
    assert sensor.hook_params == {"use_legacy_sql": False}
    assert daily.get_task("tickets.wait_fresh").timeout == timedelta(hours=2).total_seconds()


def test_freshness_sensor_uses_a_standard_sql_bigquery_hook(daily, monkeypatch):
    monkeypatch.setenv("AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT", '{"conn_type": "google_cloud_platform"}')
    hook = daily.get_task("shipments.wait_fresh")._get_hook()
    assert type(hook).__name__ == "BigQueryHook"
    assert hook.use_legacy_sql is False


def test_default_freshness_timeout_is_4_hours(tmp_path):
    config = base_config()
    dag = build_dag(write_pipeline(tmp_path, config))
    assert dag.get_task("shipments.wait_fresh").timeout == 4 * 3600


# --------------------------------------------------------------------------- shared tables (UC1)

def test_each_table_is_built_once(daily):
    builds = [t for t in daily.task_ids if t.endswith((".ingest", ".wait_fresh", ".build"))]
    assert len(builds) == len(set(builds)) == 6 + 2 + 5  # sources + intermediates + reports


@pytest.mark.parametrize(
    "report, expected",
    [
        ("delivery_report", {"order_shipments", "orders", "shipments", "customers"}),
        ("daily_sales_report", {"order_lines", "orders", "products", "customers"}),
        ("returns_report", {"returns", "order_lines", "orders", "products"}),
        ("support_report", {"tickets", "customers"}),
        ("customer_experience_report", {"order_shipments", "orders", "shipments", "returns", "tickets"}),
    ],
)
def test_each_report_waits_only_for_what_it_uses(daily, report, expected):
    assert groups_upstream(daily, f"{report}.build") == expected


def test_priority_weight_is_number_of_reports_depending_on_it(daily):
    weights = {
        "orders.ingest": 4,       # delivery, sales, returns, customer experience (via intermediates)
        "orders.check_1": 4,
        "customers.ingest": 3,    # delivery, sales, support
        "shipments.wait_fresh": 2,
        "tickets.wait_fresh": 2,
        "order_lines.build": 2,
        "delivery_report.build": 1,
        "delivery_report.email": 1,
    }
    for task_id, weight in weights.items():
        task = daily.get_task(task_id)
        assert task.priority_weight == weight, task_id
        assert type(task.weight_rule).__name__ == "_AbsolutePriorityWeightStrategy", task_id


# --------------------------------------------------------------------------- checks (UC7)

def test_checks_run_in_parallel_and_guard_downstream(daily):
    assert daily.get_task("orders.check_1").upstream_task_ids == {"orders.ingest"}
    assert daily.get_task("orders.check_2").upstream_task_ids == {"orders.ingest"}
    upstream = daily.get_task("order_shipments.build").upstream_task_ids
    assert {"orders.check_1", "orders.check_2", "shipments.check_1"} == upstream


def test_a_failed_check_only_stops_its_dependents(daily):
    # orders' checks are not upstream of support_report, so it still runs if they fail.
    assert "orders.check_1" not in ancestors(daily, "support_report.build")
    assert "orders.check_1" in ancestors(daily, "delivery_report.build")
    for task in daily.tasks:
        assert str(task.trigger_rule) in ("all_success", "TriggerRule.ALL_SUCCESS"), task.task_id


def test_reports_not_using_a_freshness_source_dont_wait_for_it(daily):
    assert "shipments.wait_fresh" not in ancestors(daily, "daily_sales_report.build")
    assert "shipments.wait_fresh" in ancestors(daily, "delivery_report.build")


def test_checks_use_standard_sql(daily):
    for task in daily.tasks:
        if task.task_id.split(".")[-1].startswith("check_"):
            assert task.use_legacy_sql is False, task.task_id
        if task.task_id.endswith(".build"):
            assert task.configuration["query"]["useLegacySql"] is False


# --------------------------------------------------------------------------- steps (UC8)

def test_steps_run_in_order_after_checks(daily):
    assert daily.get_task("delivery_report.approval").upstream_task_ids == {"delivery_report.build"}
    assert downstream_ids(daily, "delivery_report.approval") == {"delivery_report.export_clear"}
    assert downstream_ids(daily, "delivery_report.export_clear") == {"delivery_report.export"}
    assert downstream_ids(daily, "delivery_report.export") == {"delivery_report.email"}
    assert downstream_ids(daily, "delivery_report.email") == set()


def test_steps_wait_for_report_checks(daily):
    upstream = daily.get_task("daily_sales_report.export_clear").upstream_task_ids
    assert upstream == {"daily_sales_report.check_1"}


def test_monthly_finance_steps(monthly):
    assert monthly.get_task("finance_report.approval").upstream_task_ids == {"finance_report.check_1"}
    assert downstream_ids(monthly, "finance_report.export") == {"finance_report.email"}
    assert groups_upstream(monthly, "finance_report.build") == {"invoice_payments", "invoices", "payments"}


def test_approval_rejects_on_timeout_without_failing(daily, monthly):
    approval = daily.get_task("delivery_report.approval")
    assert approval.execution_timeout == timedelta(hours=8)
    assert approval.defaults == ["Reject"]
    assert approval.fail_on_reject is False
    assert monthly.get_task("finance_report.approval").execution_timeout == timedelta(hours=48)


def test_export_destination(daily):
    export = daily.get_task("delivery_report.export")
    assert export.export_format == "CSV"
    assert export.source_project_dataset_table == "my-gcp-project.reports.delivery_report"
    (uri,) = export.destination_cloud_storage_uris
    assert "gs://my-reports-bucket/delivery_report/{{ period_start }}/" in uri


def test_email_mentions_export_location(daily):
    email = daily.get_task("delivery_report.email")
    assert email.to == ["logistics@example.com"]
    assert "gs://my-reports-bucket/delivery_report/" in email.html_content
    support = daily.get_task("support_report.email")
    assert "my-gcp-project.reports.support_report" in support.html_content


def test_new_step_type_needs_only_a_registered_function(tmp_path, monkeypatch):
    from airflow.providers.standard.operators.empty import EmptyOperator

    from report_framework import steps

    monkeypatch.setattr(steps, "STEPS", dict(steps.STEPS))

    @steps.register("notify_dashboard")
    def notify_dashboard(ctx):
        return [EmptyOperator(task_id="notify_dashboard")]

    config = base_config()
    config["models"]["delivery_report"]["steps"].append("notify_dashboard")
    dag = build_dag(write_pipeline(tmp_path, config))
    assert dag.get_task("delivery_report.notify_dashboard").upstream_task_ids == {
        "delivery_report.email"
    }


# --------------------------------------------------------------------------- periods (UC9)

def test_schedule_uses_data_interval_timetable(daily, monthly):
    assert type(daily.timetable).__name__ == "CronDataIntervalTimetable"
    assert type(monthly.timetable).__name__ == "CronDataIntervalTimetable"


def render(task, text: str, data_interval_end: datetime) -> str:
    return task.render_template(text, {"data_interval_end": data_interval_end, "dag_run": None})


def test_daily_run_on_6_nov_covers_5_nov(daily):
    run_after = datetime(2026, 11, 6, 6, 0, tzinfo=UTC)
    interval = daily.timetable.infer_manual_data_interval(run_after=run_after)
    assert interval.end == run_after

    task = daily.get_task("orders.ingest")
    assert render(task, template("{{ period_start }}"), interval.end) == "2026-11-05"
    assert render(task, template("{{ period_end }}"), interval.end) == "2026-11-06"
    load = task.render_template(task.configuration, {"data_interval_end": interval.end, "dag_run": None})
    assert load["load"]["destinationTable"]["tableId"] == "orders$20261105"
    assert load["load"]["sourceUris"] == ["gs://my-landing-bucket/orders/2026-11-05/*.parquet"]


def test_monthly_run_on_1_nov_covers_october(monthly):
    run_after = datetime(2026, 11, 1, 4, 0, tzinfo=UTC)
    interval = monthly.timetable.infer_manual_data_interval(run_after=run_after)
    task = monthly.get_task("invoices.ingest")
    assert render(task, template("{{ period_start }}"), interval.end) == "2026-10-01"
    assert render(task, template("{{ period_end }}"), interval.end) == "2026-11-01"
    load = task.render_template(task.configuration, {"data_interval_end": interval.end, "dag_run": None})
    assert load["load"]["destinationTable"]["tableId"] == "invoices$202610"


def test_scheduled_run_covers_the_completed_day(daily):
    from airflow.timetables.base import DataInterval, TimeRestriction

    last = DataInterval(datetime(2026, 11, 4, 6, tzinfo=UTC), datetime(2026, 11, 5, 6, tzinfo=UTC))
    info = daily.timetable.next_dagrun_info(
        last_automated_data_interval=last,
        restriction=TimeRestriction(earliest=None, latest=None, catchup=True),
    )
    assert info.run_after == datetime(2026, 11, 6, 6, tzinfo=UTC)
    start, _, _ = make_period_macro("day", "UTC")(info.data_interval.end)
    assert start == "2026-11-05"


def test_rendered_sensor_and_model_sql(daily):
    end = datetime(2026, 11, 6, 6, tzinfo=UTC)
    sensor = daily.get_task("shipments.wait_fresh")
    assert "TIMESTAMP('2026-11-06')" in render(sensor, sensor.sql, end)
    build = daily.get_task("order_shipments.build")
    sql = render(build, build.configuration["query"]["query"], end)
    assert "DATE('2026-11-05')" in sql and "{{" not in sql


def test_manual_run_without_data_interval_uses_run_after():
    macro = make_period_macro("day", "UTC")
    run = mock.Mock(run_after=datetime(2026, 11, 6, 10, tzinfo=UTC))
    assert macro(None, run)[0] == "2026-11-05"


def test_period_uses_the_pipeline_timezone():
    # 6 Nov 02:00 UTC is still 5 Nov in New York, so the day that ended is 4 Nov.
    macro = make_period_macro("day", "America/New_York")
    assert macro(datetime(2026, 11, 6, 2, tzinfo=UTC))[0] == "2026-11-04"


# --------------------------------------------------------------------------- alerts (UC11)

def test_every_task_has_the_failure_alert(daily):
    for task in daily.tasks:
        assert task.on_failure_callback, task.task_id


def test_failure_alert_email(daily):
    config = load_config(DAGS_DIR / "configs" / "daily_operations.yaml")
    ti = mock.Mock(dag_id="daily_operations", task_id="orders.ingest", try_number=2,
                   log_url="http://airflow/log/123")
    context = {"ti": ti, "data_interval_end": datetime(2026, 11, 6, 6, tzinfo=UTC),
               "dag_run": None, "exception": RuntimeError("load failed")}
    with mock.patch("airflow.providers.smtp.hooks.smtp.SmtpHook") as hook_class:
        failure_alert(config)(context)
    smtp = hook_class.return_value.__enter__.return_value
    kwargs = smtp.send_email_smtp.call_args.kwargs
    assert kwargs["to"] == ["data-team@example.com"]
    assert "orders.ingest" in kwargs["subject"]
    for text in ["daily_operations", "orders.ingest", "2026-11-05", "http://airflow/log/123", "load failed"]:
        assert text in kwargs["html_content"], text


# --------------------------------------------------------------------------- isolation (UC10)

DAG_FILE = '''# airflow dag file for the isolation test
from pathlib import Path
from report_framework import build_dag

dag = build_dag(Path(__file__).parent / "configs" / "{name}.yaml")
'''


def test_broken_config_only_breaks_its_own_dag(tmp_path):
    good = base_config() | {"dag_id": "good_pipeline"}
    broken = base_config() | {"dag_id": "broken_pipeline", "period": "week"}
    write_pipeline(tmp_path, good)
    write_pipeline(tmp_path, broken)
    (tmp_path / "good_pipeline_dag.py").write_text(DAG_FILE.format(name="good_pipeline"))
    (tmp_path / "broken_pipeline_dag.py").write_text(DAG_FILE.format(name="broken_pipeline"))
    shutil.copy(DAGS_DIR / ".airflowignore", tmp_path / ".airflowignore")

    bag = DagBag(dag_folder=str(tmp_path), include_examples=False)

    assert "good_pipeline" in bag.dag_ids
    assert "broken_pipeline" not in bag.dag_ids
    assert list(bag.import_errors) == [str(tmp_path / "broken_pipeline_dag.py")]
    assert "period must be one of" in bag.import_errors[str(tmp_path / "broken_pipeline_dag.py")]


def test_dag_file_without_the_magic_words_is_skipped(tmp_path):
    write_pipeline(tmp_path, base_config())
    (tmp_path / "pipeline.py").write_text(
        "from report_framework import build_dag\n"
        "pipeline = build_dag(__import__('pathlib').Path(__file__).parent / 'configs/test_pipeline.yaml')\n"
    )
    bag = DagBag(dag_folder=str(tmp_path), include_examples=False, safe_mode=True)
    assert bag.dag_ids == []


# --------------------------------------------------------------------------- defaults.yaml

def test_dag_settings_come_from_defaults(daily):
    assert daily.max_active_runs == 1
    assert daily.get_task("orders.ingest").retries == 1
    assert daily.get_task("orders.ingest").retry_delay == timedelta(minutes=5)
    assert daily.get_task("shipments.wait_fresh").retries == 0   # freshness waits never retry
    assert daily.get_task("delivery_report.approval").retries == 0


def test_pipeline_can_override_dag_settings(tmp_path):
    config = base_config() | {"retries": 3, "retry_delay_minutes": 1, "max_active_runs": 2}
    dag = build_dag(write_pipeline(tmp_path, config))
    assert dag.max_active_runs == 2
    assert dag.get_task("orders.ingest").retries == 3
    assert dag.get_task("orders.ingest").retry_delay == timedelta(minutes=1)
