"""Steps that run after a report is built: one function per step type, plus a registry.

A report lists its steps in order, each with its own settings:

    steps:
      - approval: {timeout_hours: 8}
      - export
      - email: {to: [logistics@example.com]}

Adding a step type = writing one function and decorating it with
`@register(name, settings={...})`. Config validation picks up the new name
and its settings; existing pipelines don't change.

This module imports Airflow only inside the step functions, so `config.py`
can read the registry without Airflow installed.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from .rules import POSITIVE, TEXT, Rule, is_email_list

if TYPE_CHECKING:  # pragma: no cover
    from airflow.sdk import BaseOperator

    from .config import PipelineConfig, Report


@dataclass
class StepContext:
    """What a step function gets to build its tasks."""

    pipeline: PipelineConfig
    report: Report
    settings: dict[str, Any]          # this step's settings, defaults filled in
    template: Callable[[str], str]    # makes {{ period_start }} / {{ period_end }} available
    priority_weight: int

    def operator_args(self) -> dict[str, Any]:
        return {"priority_weight": self.priority_weight, "weight_rule": "absolute"}

    def step_names(self) -> list[str]:
        return [step.type for step in self.report.steps]

    def export_uri(self) -> str | None:
        """Folder the report's export step writes to, as a template (None without export)."""
        for step in self.report.steps:
            if step.type == "export":
                bucket = step.settings["bucket"]
                return f"gs://{bucket}/{self.report.name}/{{{{ period_start }}}}/"
        return None


StepFunction = Callable[[StepContext], "list[BaseOperator]"]
# (step settings, pipeline config) -> extra problems, for checks that span both
CrossCheck = Callable[[dict[str, Any], "PipelineConfig"], "list[str]"]


@dataclass(frozen=True)
class StepType:
    name: str
    build: StepFunction
    settings: dict[str, Rule] = field(default_factory=dict)
    cross_check: CrossCheck | None = None
    doc: str = field(default="", compare=False)


STEPS: dict[str, StepType] = {}


def register(
    name: str,
    *,
    settings: dict[str, Rule] | None = None,
    cross_check: CrossCheck | None = None,
) -> Callable[[StepFunction], StepFunction]:
    """Register a step type. The function returns its tasks in the order they run."""

    def decorator(function: StepFunction) -> StepFunction:
        if name in STEPS:
            raise ValueError(f"step type {name!r} is registered twice")
        STEPS[name] = StepType(
            name=name,
            build=function,
            settings=dict(settings or {}),
            cross_check=cross_check,
            doc=(function.__doc__ or "").strip(),
        )
        return function

    return decorator


# --------------------------------------------------------------------------- step types

@register("approval", settings={"timeout_hours": Rule(POSITIVE.check, POSITIVE.expected, required=True)})
def approval(ctx: StepContext) -> list[BaseOperator]:
    """Wait for a person to approve in the Airflow UI.

    Reject, or no answer before `timeout_hours`, skips the remaining steps
    of this report. Neither fails the pipeline.
    """
    from airflow.providers.standard.operators.hitl import ApprovalOperator

    names = ctx.step_names()
    remaining = names[names.index("approval") + 1:]
    task = ApprovalOperator(
        task_id="approval",
        subject=ctx.template(f"Approve {ctx.report.name} for {{{{ period_start }}}}"),
        body=ctx.template(
            f"Report table `{ctx.pipeline.project}.{ctx.report.table}` is built and its checks "
            f"passed for the period {{{{ period_start }}}} to {{{{ period_end }}}} (end excluded).\n\n"
            f"Approve to run the remaining steps: {', '.join(remaining) or 'none'}."
        ),
        defaults="Reject",  # what happens when nobody answers before the timeout
        execution_timeout=timedelta(hours=float(ctx.settings["timeout_hours"])),  # answer deadline
        ignore_downstream_trigger_rules=True,  # on reject, skip every remaining step
        fail_on_reject=False,                  # reject skips; it doesn't fail the pipeline
        retries=0,
        **ctx.operator_args(),
    )
    return [task]


def _export_needs_a_bucket(settings: dict[str, Any], pipeline: PipelineConfig) -> list[str]:
    if "bucket" not in settings:
        return ["needs 'bucket', or 'export_bucket' under pipeline:"]
    return []


@register("export", settings={"bucket": TEXT}, cross_check=_export_needs_a_bucket)
def export(ctx: StepContext) -> list[BaseOperator]:
    """Export the report table as CSV to gs://<bucket>/<report>/<period_start>/.

    `bucket` defaults to the pipeline's `export_bucket`. The folder is
    emptied first, so a rerun leaves exactly one copy.
    """
    from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
    from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

    bucket = ctx.settings["bucket"]
    clear = GCSDeleteObjectsOperator(
        task_id="export_clear",
        bucket_name=bucket,
        prefix=ctx.template(f"{ctx.report.name}/{{{{ period_start }}}}/"),
        gcp_conn_id=ctx.pipeline.gcp_conn_id,
        **ctx.operator_args(),
    )
    write = BigQueryToGCSOperator(
        task_id="export",
        source_project_dataset_table=f"{ctx.pipeline.project}.{ctx.report.table}",
        destination_cloud_storage_uris=[
            ctx.template(f"gs://{bucket}/{ctx.report.name}/{{{{ period_start }}}}/part-*.csv")
        ],
        export_format="CSV",
        print_header=True,
        gcp_conn_id=ctx.pipeline.gcp_conn_id,
        project_id=ctx.pipeline.project,
        location=ctx.pipeline.location,
        **ctx.operator_args(),
    )
    return [clear, write]


@register("email", settings={"to": Rule(is_email_list, "a non-empty list of email addresses",
                                         required=True)})
def email(ctx: StepContext) -> list[BaseOperator]:
    """Email `to` that the report is ready, with the export location."""
    from airflow.providers.smtp.operators.smtp import EmailOperator

    export_uri = ctx.export_uri()
    if export_uri:
        where = f"Exported to <code>{export_uri}</code> as CSV."
    else:
        where = f"Table: <code>{ctx.pipeline.project}.{ctx.report.table}</code>."
    task = EmailOperator(
        task_id="email",
        to=ctx.settings["to"],
        subject=ctx.template(f"[{ctx.pipeline.dag_id}] {ctx.report.name} is ready for {{{{ period_start }}}}"),
        html_content=ctx.template(
            f"<p>The report <b>{ctx.report.name}</b> for {{{{ period_start }}}} is ready.</p>"
            f"<p>{where}</p>"
        ),
        conn_id=ctx.pipeline.smtp_conn_id,
        **ctx.operator_args(),
    )
    return [task]
