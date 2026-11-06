"""Turn a validated pipeline config into an Airflow DAG.

    dag = build_dag("configs/daily_operations.yaml")

Every source, intermediate and report becomes a TaskGroup named after it:

    source loaded here:      <name>.ingest     -> <name>.check_1, <name>.check_2 ...
    source loaded elsewhere: <name>.wait_fresh -> <name>.check_1 ...
    model:                   <name>.build      -> <name>.check_1 ...
    report steps:            ... checks        -> <name>.approval -> <name>.export -> <name>.email

A model's group runs after the groups of everything in its `uses`, so the
order comes only from config (UC2). Checks of a table run in parallel, and
everything downstream waits for all of them (UC7).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.sdk import DAG, TaskGroup
from airflow.timetables.interval import CronDataIntervalTimetable

from . import graph as graph_lib
from . import steps as steps_lib
from .config import (
    Model,
    PipelineConfig,
    Source,
    load_config,
    partition_suffix,
    period_bounds,
)

DAGS_DIR = Path(__file__).resolve().parent.parent

# Prepended to every templated string, so `{{ period_start }}` and
# `{{ period_end }}` exist wherever Airflow renders a template.
PERIOD_PREAMBLE = (
    "{%- set period_start, period_end, period_partition = "
    "report_period(data_interval_end, dag_run) -%}"
)
# Airflow treats a templated string ending in one of an operator's
# `template_ext` (".sql", ".json") as a file path. A trailing Jinja comment
# keeps URIs such as gs://.../*.json from being read as files.
_FILE_LIKE_ENDINGS = (".sql", ".json")


def template(text: str) -> str:
    """Make the period variables available to a templated string."""
    text = PERIOD_PREAMBLE + text
    if text.rstrip().endswith(_FILE_LIKE_ENDINGS):
        text += "{# not a file #}"
    return text


def make_period_macro(period: str, timezone: str):
    """`report_period(data_interval_end, dag_run)` -> (period_start, period_end, partition).

    The run covers the last period completed when its data interval ends.
    With CronDataIntervalTimetable, the run starting 6 Nov 06:00 has a data
    interval ending 6 Nov 06:00, so it covers 5 Nov.
    """

    def report_period(data_interval_end: datetime | None, dag_run: Any = None) -> tuple[str, str, str]:
        moment = data_interval_end or getattr(dag_run, "run_after", None)
        if moment is None:
            raise ValueError("cannot work out the period: the run has no data interval")
        as_of = pendulum.instance(moment).in_timezone(timezone).date()
        start, end = period_bounds(period, date(as_of.year, as_of.month, as_of.day))
        return start.isoformat(), end.isoformat(), partition_suffix(period, start)

    return report_period


class BigQueryFreshnessSensor(SqlSensor):
    """SqlSensor that always talks to BigQuery with standard SQL.

    The plain SqlSensor picks a hook from the connection type, and the usual
    `google_cloud_platform` connection doesn't give a SQL hook.
    """

    def __init__(self, *, gcp_conn_id: str, location: str | None = None, **kwargs):
        super().__init__(conn_id=gcp_conn_id, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.location = location

    def _get_hook(self) -> BigQueryHook:
        return BigQueryHook(gcp_conn_id=self.gcp_conn_id, use_legacy_sql=False, location=self.location)


def resolve_config_path(config_path: str | Path) -> Path:
    path = Path(config_path)
    return path if path.is_absolute() else DAGS_DIR / path


def build_dag(config_path: str | Path, sql_dir: str | Path | None = None) -> DAG:
    """Load, validate and build one pipeline. Raises ConfigError if the config is invalid."""
    config = load_config(resolve_config_path(config_path), sql_dir=sql_dir)
    return DagBuilder(config).build()


def failure_alert(config: PipelineConfig):
    """on_failure_callback: email `alert_email` with the DAG, task, period and log link."""
    macro = make_period_macro(config.period, config.timezone)

    def alert(context: dict[str, Any]) -> None:
        from airflow.providers.smtp.hooks.smtp import SmtpHook

        ti = context["ti"]
        try:
            start, end, _ = macro(context.get("data_interval_end"), context.get("dag_run"))
            period = f"{start} to {end} (end excluded)"
        except Exception:  # noqa: BLE001 - the alert must not fail because of the period
            period = "unknown"
        log_url = getattr(ti, "log_url", "") or ""
        error = context.get("exception")
        subject = f"[{config.dag_id}] task failed: {ti.task_id} ({period.split(' ')[0]})"
        html = (
            f"<p><b>DAG:</b> {ti.dag_id}<br>"
            f"<b>Task:</b> {ti.task_id}<br>"
            f"<b>Period:</b> {period}<br>"
            f"<b>Try:</b> {getattr(ti, 'try_number', '?')}<br>"
            f"<b>Log:</b> <a href=\"{log_url}\">{log_url}</a></p>"
            f"<p><b>Error:</b> <code>{error!s}</code></p>"
        )
        with SmtpHook(smtp_conn_id=config.smtp_conn_id) as smtp:
            smtp.send_email_smtp(to=config.alert_email, subject=subject, html_content=html)

    return alert


class DagBuilder:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.graph = config.graph
        self.order = graph_lib.topological_order(self.graph)
        self.usage = graph_lib.report_usage_counts(self.graph, config.reports)
        self.groups: dict[str, TaskGroup] = {}

    # ---- helpers

    def weight(self, name: str) -> dict[str, Any]:
        """Tasks used by more reports run first when several are ready (UC1)."""
        return {"priority_weight": max(self.usage.get(name, 0), 1), "weight_rule": "absolute"}

    def bigquery_args(self) -> dict[str, Any]:
        args = {"gcp_conn_id": self.config.gcp_conn_id}
        if self.config.location:
            args["location"] = self.config.location
        return args

    def add_checks(self, name: str, checks: list[str], after) -> list:
        """Checks run in parallel after the table is loaded or built."""
        tasks = [
            BigQueryCheckOperator(
                task_id=f"check_{number}",
                sql=template(sql),
                use_legacy_sql=False,
                **self.bigquery_args(),
                **self.weight(name),
            )
            for number, sql in enumerate(checks, start=1)
        ]
        for task in tasks:
            after >> task
        return tasks or [after]

    # ---- sources

    def ingest_task(self, source: Source):
        ingest = source.ingest
        assert ingest is not None
        table_id = source.name
        load: dict[str, Any] = {
            "sourceUris": [template(ingest.uri)],
            "sourceFormat": ingest.format,
            "writeDisposition": "WRITE_TRUNCATE",  # rerun = same result, no duplicate rows
            "createDisposition": "CREATE_IF_NEEDED",
        }
        if ingest.mode == "period":
            # Partition decorator + WRITE_TRUNCATE replaces only this period's partition.
            table_id = template(f"{source.name}${{{{ period_partition }}}}")
            partitioning = {"type": "DAY" if self.config.period == "day" else "MONTH"}
            if ingest.partition_field:
                partitioning["field"] = ingest.partition_field
            load["timePartitioning"] = partitioning
        if ingest.format == "CSV":
            load["skipLeadingRows"] = 1
        if ingest.format in ("CSV", "NEWLINE_DELIMITED_JSON"):
            load["autodetect"] = True
        load["destinationTable"] = {
            "projectId": self.config.project,
            "datasetId": source.dataset,
            "tableId": table_id,
        }
        return BigQueryInsertJobOperator(
            task_id="ingest",
            configuration={"load": load},
            project_id=self.config.project,
            **self.bigquery_args(),
            **self.weight(source.name),
        )

    def freshness_task(self, source: Source):
        return BigQueryFreshnessSensor(
            task_id="wait_fresh",
            sql=template(source.freshness or ""),
            gcp_conn_id=self.config.gcp_conn_id,
            location=self.config.location,
            hook_params={"use_legacy_sql": False},
            mode="reschedule",  # don't hold a worker slot between pokes
            poke_interval=timedelta(minutes=source.poke_interval_minutes).total_seconds(),
            timeout=timedelta(hours=source.freshness_timeout_hours).total_seconds(),
            retries=0,  # the timeout is final: retrying would restart the clock
            **self.weight(source.name),
        )

    def add_source(self, source: Source) -> None:
        kind = "ingest" if source.ingest else "wait until fresh"
        with TaskGroup(group_id=source.name, tooltip=f"source {source.table} ({kind})") as group:
            first = self.ingest_task(source) if source.ingest else self.freshness_task(source)
            self.add_checks(source.name, source.checks, after=first)
        self.groups[source.name] = group

    # ---- models

    def add_model(self, model: Model) -> None:
        with TaskGroup(group_id=model.name, tooltip=f"{model.type} {model.table}") as group:
            build = BigQueryInsertJobOperator(
                task_id="build",
                configuration={"query": {"query": template(model.sql), "useLegacySql": False}},
                project_id=self.config.project,
                **self.bigquery_args(),
                **self.weight(model.name),
            )
            last = self.add_checks(model.name, model.checks, after=build)
            for step_name in model.steps:
                step_type = steps_lib.STEPS[step_name]
                ctx = steps_lib.StepContext(
                    pipeline=self.config,
                    report=model,
                    template=template,
                    priority_weight=self.weight(model.name)["priority_weight"],
                )
                tasks = step_type.build(ctx)
                for previous in last:
                    previous >> tasks[0]
                for upstream_task, downstream_task in zip(tasks, tasks[1:]):
                    upstream_task >> downstream_task
                last = [tasks[-1]]
        for used in model.uses:
            self.groups[used] >> group
        self.groups[model.name] = group

    # ---- DAG

    def build(self) -> DAG:
        config = self.config
        tz = pendulum.timezone(config.timezone)
        start = config.start_date
        dag = DAG(
            dag_id=config.dag_id,
            description=config.description or None,
            doc_md=self.doc_md(),
            schedule=CronDataIntervalTimetable(config.schedule, timezone=tz),
            start_date=pendulum.datetime(start.year, start.month, start.day, tz=tz),
            catchup=False,  # backfills are out of scope
            max_active_runs=config.max_active_runs,
            user_defined_macros={"report_period": make_period_macro(config.period, config.timezone)},
            default_args={
                "owner": "report-framework",
                "retries": config.retries,
                "retry_delay": timedelta(minutes=config.retry_delay_minutes),
                "on_failure_callback": failure_alert(config),
            },
            tags=["report-framework", f"period:{config.period}"],
        )
        with dag:
            for name in self.order:
                if name in config.sources:
                    self.add_source(config.sources[name])
                else:
                    self.add_model(config.models[name])
        return dag

    def doc_md(self) -> str:
        config = self.config
        lines = [
            f"# {config.dag_id}",
            "",
            config.description,
            "",
            f"Built by the report framework from `{config.path.name}`. "
            f"Each run covers the {config.period} that has just ended.",
            "",
            "| Report | Uses (directly or indirectly) | Steps |",
            "|---|---|---|",
        ]
        for report in config.reports:
            uses = ", ".join(sorted(graph_lib.upstream(self.graph, report)))
            steps = " → ".join(config.models[report].steps) or "-"
            lines.append(f"| `{config.models[report].table}` | {uses} | {steps} |")
        return "\n".join(lines)
