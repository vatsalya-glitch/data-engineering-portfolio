"""Load a pipeline config, attach its SQL, and validate it. No Airflow imports.

`load_config(path)` returns a `PipelineConfig` or raises `ConfigError`
listing every problem found, not only the first one (UC12).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import yaml

from . import graph as graph_lib
from . import sql_parse
from . import steps as steps_lib

PERIODS = ("day", "month")
MODEL_TYPES = ("intermediate", "report")
INGEST_MODES = ("replace", "period")
INGEST_FORMATS = ("PARQUET", "CSV", "NEWLINE_DELIMITED_JSON", "AVRO", "ORC")

DEFAULT_FRESHNESS_TIMEOUT_HOURS = 4.0
DEFAULT_POKE_INTERVAL_MINUTES = 10.0

SOURCE_DATASET = "raw"
DATASETS = {"intermediate": "mart", "report": "reports"}

_PIPELINE_KEYS = {
    "dag_id", "description", "schedule", "period", "timezone", "start_date",
    "gcp_conn_id", "smtp_conn_id", "project", "location", "alert_email",
    "sources", "models",
}
_SOURCE_KEYS = {"description", "ingest", "freshness", "checks",
                "freshness_timeout_hours", "poke_interval_minutes"}
_INGEST_KEYS = {"uri", "mode", "format", "partition_field"}
_MODEL_KEYS = {"description", "type", "uses", "checks", "steps"}


class ConfigError(ValueError):
    """A pipeline config is invalid. `problems` lists every problem found."""

    def __init__(self, path: Path | str, problems: list[str]):
        self.path = str(path)
        self.problems = problems
        lines = "\n".join(f"  - {problem}" for problem in problems)
        noun = "problem" if len(problems) == 1 else "problems"
        super().__init__(f"{self.path}: {len(problems)} {noun}\n{lines}")


@dataclass
class Ingest:
    uri: str
    mode: str                      # replace | period
    format: str                    # BigQuery source format
    partition_field: str | None    # column partitioning; None = ingestion-time partitioning


@dataclass
class Source:
    name: str
    ingest: Ingest | None = None
    freshness: str | None = None
    checks: list[str] = field(default_factory=list)
    freshness_timeout_hours: float = DEFAULT_FRESHNESS_TIMEOUT_HOURS
    poke_interval_minutes: float = DEFAULT_POKE_INTERVAL_MINUTES
    description: str = ""

    @property
    def table(self) -> str:
        return f"{SOURCE_DATASET}.{self.name}"


@dataclass
class Model:
    name: str
    type: str                      # intermediate | report
    uses: list[str]
    sql: str = ""
    checks: list[str] = field(default_factory=list)
    steps: list[str] = field(default_factory=list)
    options: dict[str, Any] = field(default_factory=dict)  # step options, e.g. email_to
    description: str = ""

    @property
    def table(self) -> str:
        return f"{DATASETS.get(self.type, 'mart')}.{self.name}"


@dataclass
class PipelineConfig:
    path: Path
    dag_id: str
    schedule: str
    period: str
    timezone: str
    start_date: date
    gcp_conn_id: str
    smtp_conn_id: str
    project: str
    location: str | None
    alert_email: list[str]
    options: dict[str, Any]        # pipeline-level step options, e.g. export_bucket
    sources: dict[str, Source]
    models: dict[str, Model]
    description: str = ""

    @property
    def graph(self) -> dict[str, list[str]]:
        """node -> the nodes it uses."""
        nodes: dict[str, list[str]] = {name: [] for name in self.sources}
        nodes.update({name: list(model.uses) for name, model in self.models.items()})
        return nodes

    @property
    def reports(self) -> list[str]:
        return [name for name, model in self.models.items() if model.type == "report"]

    def table(self, name: str) -> str:
        node = self.sources.get(name) or self.models[name]
        return node.table


# --------------------------------------------------------------------------- periods

def period_bounds(period: str, as_of: date) -> tuple[date, date]:
    """The last period completed on `as_of`: (first date, first date after).

    `as_of` is the date the run's data interval ends. A daily run on 6 Nov
    covers 5 Nov; a monthly run on 1 Nov covers October.
    """
    if period == "day":
        return as_of - timedelta(days=1), as_of
    if period == "month":
        end = as_of.replace(day=1)
        last_month = end - timedelta(days=1)
        return last_month.replace(day=1), end
    raise ValueError(f"unknown period {period!r}")


def partition_suffix(period: str, period_start: date) -> str:
    """BigQuery partition decorator value for a period: 20261105 (day) or 202610 (month)."""
    return period_start.strftime("%Y%m%d" if period == "day" else "%Y%m")


# --------------------------------------------------------------------------- YAML

class _Loader(yaml.SafeLoader):
    """SafeLoader that records duplicate keys instead of silently keeping the last one."""

    def __init__(self, stream):
        super().__init__(stream)
        self.duplicates: list[str] = []


def _construct_mapping(loader: _Loader, node: yaml.MappingNode, deep: bool = False):
    seen: set = set()
    for key_node, _ in node.value:
        key = loader.construct_object(key_node, deep=True)
        if key in seen:
            loader.duplicates.append(
                f"key {key!r} is defined twice (line {key_node.start_mark.line + 1})"
            )
        seen.add(key)
    return yaml.SafeLoader.construct_mapping(loader, node, deep=deep)


_Loader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping)


def _read_yaml(path: Path) -> tuple[Any, list[str]]:
    loader = _Loader(path.read_text())
    try:
        return loader.get_single_data(), loader.duplicates
    finally:
        loader.dispose()


# --------------------------------------------------------------------------- loading

def default_sql_dir(config_path: Path, dag_id: str) -> Path:
    """dags/configs/x.yaml -> dags/sql/<dag_id>/"""
    return config_path.resolve().parent.parent / "sql" / dag_id


def load_config(path: str | Path, sql_dir: str | Path | None = None) -> PipelineConfig:
    """Load and validate one pipeline config. Raises ConfigError listing every problem."""
    path = Path(path)
    problems: list[str] = []
    try:
        raw, duplicates = _read_yaml(path)
    except (OSError, yaml.YAMLError) as error:
        raise ConfigError(path, [f"cannot read config: {error}"]) from None
    problems.extend(duplicates)
    if not isinstance(raw, dict):
        raise ConfigError(path, problems + ["config must be a YAML mapping"])

    validator = _Validator(path, raw, problems, Path(sql_dir) if sql_dir else None)
    config = validator.run()
    if problems:
        raise ConfigError(path, problems)
    return config


class _Validator:
    def __init__(self, path: Path, raw: dict, problems: list[str], sql_dir: Path | None):
        self.path = path
        self.raw = raw
        self.problems = problems
        self.sql_dir = sql_dir

    def error(self, message: str) -> None:
        self.problems.append(message)

    # ---- small helpers

    def _unknown_keys(self, where: str, data: dict, allowed: set[str]) -> None:
        for key in sorted(set(data) - allowed, key=str):
            self.error(f"{where}: unknown key {key!r}")

    def _str(self, where: str, value: Any, required: bool = True) -> str | None:
        if value is None:
            if required:
                self.error(f"{where} is required")
            return None
        if not isinstance(value, str) or not value.strip():
            self.error(f"{where} must be a non-empty string")
            return None
        return value

    def _str_list(self, where: str, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            value = [value]
        if not isinstance(value, list) or not all(isinstance(v, str) and v.strip() for v in value):
            self.error(f"{where} must be a list of non-empty strings")
            return []
        return value

    def _number(self, where: str, value: Any, default: float) -> float:
        if value is None:
            return default
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            self.error(f"{where} must be a positive number")
            return default
        return float(value)

    def _mapping(self, where: str, value: Any) -> dict:
        if value is None:
            return {}
        if not isinstance(value, dict):
            self.error(f"{where} must be a mapping")
            return {}
        return value

    # ---- main

    def run(self) -> PipelineConfig:
        raw = self.raw
        step_types = steps_lib.STEPS
        pipeline_step_keys = {k for s in step_types.values()
                              for k in s.required_pipeline_keys + s.optional_pipeline_keys}
        self._unknown_keys("config", raw, _PIPELINE_KEYS | pipeline_step_keys)

        dag_id = self._str("dag_id", raw.get("dag_id")) or self.path.stem
        schedule = self._str("schedule", raw.get("schedule")) or ""
        period = raw.get("period")
        if period not in PERIODS:
            self.error(f"period must be one of {list(PERIODS)}, got {period!r}")
        alert_email = self._str_list("alert_email", raw.get("alert_email"))
        if not alert_email:
            self.error("alert_email is required: list who gets an email when a task fails")
        start_date = raw.get("start_date", date(2026, 1, 1))
        if not isinstance(start_date, date):
            self.error(f"start_date must be a date (YYYY-MM-DD), got {start_date!r}")
            start_date = date(2026, 1, 1)

        sources = self._sources(self._mapping("sources", raw.get("sources")))
        models = self._models(self._mapping("models", raw.get("models")))

        config = PipelineConfig(
            path=self.path,
            dag_id=dag_id,
            description=raw.get("description") or "",
            schedule=schedule,
            period=period if period in PERIODS else "day",
            timezone=self._str("timezone", raw.get("timezone"), required=False) or "UTC",
            start_date=start_date,
            gcp_conn_id=self._str("gcp_conn_id", raw.get("gcp_conn_id"), required=False)
            or "google_cloud_default",
            smtp_conn_id=self._str("smtp_conn_id", raw.get("smtp_conn_id"), required=False)
            or "smtp_default",
            project=self._str("project", raw.get("project")) or "",
            location=self._str("location", raw.get("location"), required=False),
            alert_email=alert_email,
            options={k: raw[k] for k in pipeline_step_keys if k in raw},
            sources=sources,
            models=models,
        )

        self._names(config)
        self._model_rules(config)
        self._steps(config)
        self._attach_sql(config, self.sql_dir or default_sql_dir(self.path, dag_id))
        self._drift(config)
        self._cycles(config)
        return config

    def _sources(self, raw_sources: dict) -> dict[str, Source]:
        sources: dict[str, Source] = {}
        for name, data in raw_sources.items():
            where = f"sources.{name}"
            if not isinstance(data, dict):
                self.error(f"{where} must be a mapping")
                continue
            self._unknown_keys(where, data, _SOURCE_KEYS)
            has_ingest, has_freshness = "ingest" in data, "freshness" in data
            if has_ingest == has_freshness:
                self.error(
                    f"{where}: needs exactly one of 'ingest' (loaded by this pipeline) "
                    f"or 'freshness' (loaded elsewhere), found "
                    f"{'both' if has_ingest else 'neither'}"
                )
            ingest = self._ingest(where, data["ingest"]) if has_ingest else None
            freshness = self._str(f"{where}.freshness", data["freshness"]) if has_freshness else None
            sources[name] = Source(
                name=name,
                ingest=ingest,
                freshness=freshness,
                checks=self._str_list(f"{where}.checks", data.get("checks")),
                freshness_timeout_hours=self._number(
                    f"{where}.freshness_timeout_hours", data.get("freshness_timeout_hours"),
                    DEFAULT_FRESHNESS_TIMEOUT_HOURS),
                poke_interval_minutes=self._number(
                    f"{where}.poke_interval_minutes", data.get("poke_interval_minutes"),
                    DEFAULT_POKE_INTERVAL_MINUTES),
                description=data.get("description") or "",
            )
        return sources

    def _ingest(self, where: str, data: Any) -> Ingest | None:
        where = f"{where}.ingest"
        if not isinstance(data, dict):
            self.error(f"{where} must be a mapping with 'uri' and 'mode'")
            return None
        self._unknown_keys(where, data, _INGEST_KEYS)
        uri = self._str(f"{where}.uri", data.get("uri")) or ""
        if uri and not uri.startswith("gs://"):
            self.error(f"{where}.uri must start with 'gs://', got {uri!r}")
        mode = data.get("mode")
        if mode not in INGEST_MODES:
            self.error(f"{where}.mode must be one of {list(INGEST_MODES)}, got {mode!r}")
        fmt = data.get("format") or _format_from_uri(uri)
        if fmt is None:
            self.error(f"{where}.format is required: cannot tell the file format from {uri!r}")
        elif str(fmt).upper() not in INGEST_FORMATS:
            self.error(f"{where}.format must be one of {list(INGEST_FORMATS)}, got {fmt!r}")
        return Ingest(
            uri=uri,
            mode=mode if mode in INGEST_MODES else "replace",
            format=str(fmt or "PARQUET").upper(),
            partition_field=self._str(f"{where}.partition_field", data.get("partition_field"),
                                      required=False),
        )

    def _models(self, raw_models: dict) -> dict[str, Model]:
        step_keys = {k for s in steps_lib.STEPS.values()
                     for k in s.required_report_keys + s.optional_report_keys}
        models: dict[str, Model] = {}
        for name, data in raw_models.items():
            where = f"models.{name}"
            if not isinstance(data, dict):
                self.error(f"{where} must be a mapping")
                continue
            self._unknown_keys(where, data, _MODEL_KEYS | step_keys)
            model_type = data.get("type")
            if model_type is None:
                self.error(f"{where}: type is required ({' or '.join(MODEL_TYPES)})")
            elif model_type not in MODEL_TYPES:
                self.error(f"{where}: type must be one of {list(MODEL_TYPES)}, got {model_type!r}")
            uses = self._str_list(f"{where}.uses", data.get("uses"))
            for duplicate in sorted({u for u in uses if uses.count(u) > 1}):
                self.error(f"{where}: uses {duplicate!r} more than once")
            models[name] = Model(
                name=name,
                type=model_type if model_type in MODEL_TYPES else "",
                uses=list(dict.fromkeys(uses)),
                checks=self._str_list(f"{where}.checks", data.get("checks")),
                steps=self._str_list(f"{where}.steps", data.get("steps")),
                options={k: data[k] for k in step_keys if k in data},
                description=data.get("description") or "",
            )
        return models

    def _names(self, config: PipelineConfig) -> None:
        for name in sorted(set(config.sources) & set(config.models)):
            self.error(f"name {name!r} is defined twice: as a source and as a model")
        for name in [*config.sources, *config.models]:
            if not isinstance(name, str) or not name.isidentifier():
                self.error(f"name {name!r} must be a valid identifier (letters, digits, _)")
        known = set(config.sources) | set(config.models)
        for model in config.models.values():
            for used in model.uses:
                if used not in known:
                    self.error(f"models.{model.name}: uses unknown table {used!r}")

    def _model_rules(self, config: PipelineConfig) -> None:
        for model in config.models.values():
            if model.type == "intermediate" and model.steps:
                self.error(f"models.{model.name}: an intermediate cannot have steps "
                           f"(make it type: report, or move the steps to a report)")
            for used in model.uses:
                target = config.models.get(used)
                if target is not None and target.type == "report":
                    self.error(f"models.{model.name}: uses {used!r}, which is a report; "
                               f"reports cannot be used by other models (make it an intermediate)")
        if config.models and not config.reports:
            self.error("models: the pipeline has no report")

    def _steps(self, config: PipelineConfig) -> None:
        for model in config.models.values():
            where = f"models.{model.name}"
            for duplicate in sorted({s for s in model.steps if model.steps.count(s) > 1}):
                self.error(f"{where}: step {duplicate!r} is listed more than once")
            for step in dict.fromkeys(model.steps):
                step_type = steps_lib.STEPS.get(step)
                if step_type is None:
                    self.error(f"{where}: unknown step {step!r} "
                               f"(known: {', '.join(sorted(steps_lib.STEPS))})")
                    continue
                for key in step_type.required_report_keys:
                    if not model.options.get(key):
                        self.error(f"{where}: step {step!r} needs {key!r}")
                for key in step_type.required_pipeline_keys:
                    if not config.options.get(key):
                        self.error(f"{where}: step {step!r} needs {key!r} at the top of the config")
            problems = steps_lib.validate_options(model.options)
            self.problems.extend(f"{where}: {problem}" for problem in problems)

    def _attach_sql(self, config: PipelineConfig, sql_dir: Path) -> None:
        files = {p.stem: p for p in sql_dir.glob("*.sql")} if sql_dir.is_dir() else {}
        for name, model in config.models.items():
            sql_file = files.get(name)
            if sql_file is None:
                self.error(f"models.{name}: no SQL file {sql_dir / (name + '.sql')}")
            else:
                model.sql = sql_file.read_text()
        for stem in sorted(set(files) - set(config.models)):
            self.error(f"SQL file {files[stem]} has no model in the config")

    def _drift(self, config: PipelineConfig) -> None:
        known = set(config.sources) | set(config.models)
        for name, model in config.models.items():
            if not model.sql:
                continue
            try:
                reads = sql_parse.tables_read(model.sql)
            except sql_parse.SqlParseError as error:
                self.error(f"models.{name}: cannot parse SQL: {error}")
                continue
            expected = {config.table(u) for u in model.uses if u in known}
            missing = sorted(expected - reads)
            extra = sorted(reads - expected)
            if missing:
                self.error(f"models.{name}: drift: uses {missing} but the SQL does not read them")
            if extra:
                self.error(f"models.{name}: drift: the SQL reads {extra} but they are not in uses")

    def _cycles(self, config: PipelineConfig) -> None:
        known = set(config.sources) | set(config.models)
        graph = {name: [u for u in uses if u in known] for name, uses in config.graph.items()}
        try:
            graph_lib.topological_order(graph)
        except graph_lib.GraphCycleError as error:
            self.error(str(error))


def _format_from_uri(uri: str) -> str | None:
    lowered = uri.lower().rstrip("*")
    for suffix, fmt in ((".parquet", "PARQUET"), (".csv", "CSV"), (".json", "NEWLINE_DELIMITED_JSON"),
                        (".jsonl", "NEWLINE_DELIMITED_JSON"), (".avro", "AVRO"), (".orc", "ORC")):
        if lowered.endswith(suffix):
            return fmt
    return None
