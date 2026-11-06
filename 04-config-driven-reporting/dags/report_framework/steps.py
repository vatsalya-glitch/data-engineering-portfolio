"""Steps that run after a report is built: one function per step type, plus a registry.

Adding a step type = writing one function and decorating it with
`@register(...)`. Existing pipelines don't change.

This module imports Airflow only inside the step functions, so `config.py`
can read the registry (step names and the keys each step needs) without
Airflow installed.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from airflow.sdk import BaseOperator

    from .config import Model, PipelineConfig


@dataclass
class StepContext:
    """What a step function gets to build its tasks."""

    pipeline: PipelineConfig
    report: Model
    template: Callable[[str], str]  # makes {{ period_start }} / {{ period_end }} available
    priority_weight: int

    def operator_args(self) -> dict[str, Any]:
        return {"priority_weight": self.priority_weight, "weight_rule": "absolute"}

    def export_uri(self) -> str:
        """Folder the export step writes to, as a template."""
        bucket = self.pipeline.options["export_bucket"]
        return f"gs://{bucket}/{self.report.name}/{{{{ period_start }}}}/"


StepFunction = Callable[[StepContext], "list[BaseOperator]"]


@dataclass(frozen=True)
class StepType:
    name: str
    build: StepFunction
    required_report_keys: tuple[str, ...] = ()
    optional_report_keys: tuple[str, ...] = ()
    required_pipeline_keys: tuple[str, ...] = ()
    optional_pipeline_keys: tuple[str, ...] = ()
    doc: str = field(default="", compare=False)


STEPS: dict[str, StepType] = {}


def register(
    name: str,
    *,
    required_report_keys: tuple[str, ...] = (),
    optional_report_keys: tuple[str, ...] = (),
    required_pipeline_keys: tuple[str, ...] = (),
    optional_pipeline_keys: tuple[str, ...] = (),
) -> Callable[[StepFunction], StepFunction]:
    """Register a step type. The function returns its tasks in the order they run."""

    def decorator(function: StepFunction) -> StepFunction:
        if name in STEPS:
            raise ValueError(f"step type {name!r} is registered twice")
        STEPS[name] = StepType(
            name=name,
            build=function,
            required_report_keys=required_report_keys,
            optional_report_keys=optional_report_keys,
            required_pipeline_keys=required_pipeline_keys,
            optional_pipeline_keys=optional_pipeline_keys,
            doc=(function.__doc__ or "").strip(),
        )
        return function

    return decorator


def validate_options(options: dict[str, Any]) -> list[str]:
    """Type checks for step options set on a report. Returns problems."""
    problems = []
    email_to = options.get("email_to")
    if email_to is not None and not (
        isinstance(email_to, list) and email_to and all(isinstance(e, str) and e for e in email_to)
    ):
        problems.append("email_to must be a non-empty list of email addresses")
    return problems


# --------------------------------------------------------------------------- step types

@register("approval", optional_report_keys=("approval_timeout_hours",))
def approval(ctx: StepContext) -> list[BaseOperator]:
    """Wait for a person to approve in the Airflow UI.

    Reject, or no answer before the timeout, skips the remaining steps of
    this report. Neither fails the pipeline.
    """
    from airflow.providers.standard.operators.hitl import ApprovalOperator

    hours = float(ctx.report.options["approval_timeout_hours"])  # default in defaults.yaml
    task = ApprovalOperator(
        task_id="approval",
        subject=ctx.template(f"Approve {ctx.report.name} for {{{{ period_start }}}}"),
        body=ctx.template(
            f"Report table `{ctx.pipeline.project}.{ctx.report.table}` is built and its checks "
            f"passed for the period {{{{ period_start }}}} to {{{{ period_end }}}} (end excluded).\n\n"
            f"Approve to run the remaining steps: "
            f"{', '.join(ctx.report.steps[ctx.report.steps.index('approval') + 1:]) or 'none'}."
        ),
        defaults="Reject",  # what happens when nobody answers before the timeout
        execution_timeout=timedelta(hours=hours),  # the approval operator's answer deadline
        ignore_downstream_trigger_rules=True,  # on reject, skip every remaining step
        fail_on_reject=False,                  # reject skips; it doesn't fail the pipeline
        retries=0,
        **ctx.operator_args(),
    )
    return [task]


@register("export", required_pipeline_keys=("export_bucket",))
def export(ctx: StepContext) -> list[BaseOperator]:
    """Export the report table as CSV to gs://<export_bucket>/<report>/<period_start>/.

    The folder is emptied first, so a rerun leaves exactly one copy.
    """
    from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
    from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

    bucket = ctx.pipeline.options["export_bucket"]
    prefix = f"{ctx.report.name}/{{{{ period_start }}}}/"
    clear = GCSDeleteObjectsOperator(
        task_id="export_clear",
        bucket_name=bucket,
        prefix=ctx.template(prefix),
        gcp_conn_id=ctx.pipeline.gcp_conn_id,
        **ctx.operator_args(),
    )
    write = BigQueryToGCSOperator(
        task_id="export",
        source_project_dataset_table=f"{ctx.pipeline.project}.{ctx.report.table}",
        destination_cloud_storage_uris=[ctx.template(ctx.export_uri() + "part-*.csv")],
        export_format="CSV",
        print_header=True,
        gcp_conn_id=ctx.pipeline.gcp_conn_id,
        project_id=ctx.pipeline.project,
        location=ctx.pipeline.location,
        **ctx.operator_args(),
    )
    return [clear, write]


@register("email", required_report_keys=("email_to",))
def email(ctx: StepContext) -> list[BaseOperator]:
    """Email `email_to` that the report is ready, with the export location."""
    from airflow.providers.smtp.operators.smtp import EmailOperator

    if "export" in ctx.report.steps:
        where = f"Exported to <code>{ctx.export_uri()}</code> as CSV."
    else:
        where = f"Table: <code>{ctx.pipeline.project}.{ctx.report.table}</code>."
    task = EmailOperator(
        task_id="email",
        to=ctx.report.options["email_to"],
        subject=ctx.template(f"[{ctx.pipeline.dag_id}] {ctx.report.name} is ready for {{{{ period_start }}}}"),
        html_content=ctx.template(
            f"<p>The report <b>{ctx.report.name}</b> for {{{{ period_start }}}} is ready.</p>"
            f"<p>{where}</p>"
        ),
        conn_id=ctx.pipeline.smtp_conn_id,
        **ctx.operator_args(),
    )
    return [task]
