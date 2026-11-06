"""Load a pipeline config, attach its SQL, and validate it. No Airflow imports.

A pipeline config has four sections, one per layer plus the pipeline itself:

    pipeline:        schedule, period, project, alerts ...
    ingestion:       raw tables: loaded by this DAG (`load`) or elsewhere (`wait_for`)
    transformation:  shared tables built with SQL from ingestion/transformation tables
    reports:         final tables built with SQL, followed by their steps

Every transformation and report says which tables it uses, per layer:

    ingestion: [customers]
    transformation: [order_shipments]

`load_config(path)` returns a `PipelineConfig` or raises `ConfigError`
listing every problem found, not only the first one.

Framework defaults (dataset names, timeouts, retries, connection ids) live
in `defaults.yaml` next to this file.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import yaml

from . import graph as graph_lib
from . import sql_parse
from . import steps as steps_lib
from .rules import DATASET, POSITIVE, TEXT, Rule, is_date, is_int, is_timezone

PERIODS = ("day", "month")
LOAD_MODES = ("replace", "period")
LOAD_FORMATS = ("PARQUET", "CSV", "NEWLINE_DELIMITED_JSON", "AVRO", "ORC")

# The layers, in the order they run. A table can only use tables of an earlier
# layer, or (for transformations) other transformations.
INGESTION, TRANSFORMATION, REPORTS = "ingestion", "transformation", "reports"
LAYERS = (INGESTION, TRANSFORMATION, REPORTS)
CAN_USE = {TRANSFORMATION: (INGESTION, TRANSFORMATION), REPORTS: (INGESTION, TRANSFORMATION)}

DEFAULTS_PATH = Path(__file__).with_name("defaults.yaml")

PIPELINE_SETTINGS = {
    "timezone": Rule(is_timezone, "an IANA time zone such as UTC or Europe/Amsterdam"),
    "start_date": Rule(is_date, "a date (YYYY-MM-DD)"),
    "gcp_conn_id": TEXT,
    "smtp_conn_id": TEXT,
    "retries": Rule(is_int(0), "a whole number, 0 or more"),
    "retry_delay_minutes": POSITIVE,
    "max_active_runs": Rule(is_int(1), "a whole number, 1 or more"),
}
WAIT_FOR_SETTINGS = {"timeout_hours": POSITIVE, "poke_interval_minutes": POSITIVE}

_PIPELINE_KEYS = {"dag_id", "description", "schedule", "period", "project", "location",
                  "alert_email", "export_bucket", *PIPELINE_SETTINGS}
_INGESTION_KEYS = {"description", "load", "wait_for", "checks"}
_LOAD_KEYS = {"uri", "mode", "format", "partition_field"}
_WAIT_FOR_KEYS = {"fresh_when", *WAIT_FOR_SETTINGS}
_TRANSFORMATION_KEYS = {"description", "sql", INGESTION, TRANSFORMATION, "checks"}
_REPORT_KEYS = _TRANSFORMATION_KEYS | {"steps"}


class ConfigError(ValueError):
    """A pipeline config is invalid. `problems` lists every problem found."""

    def __init__(self, path: Path | str, problems: list[str]):
        self.path = str(path)
        self.problems = problems
        lines = "\n".join(f"  - {problem}" for problem in problems)
        noun = "problem" if len(problems) == 1 else "problems"
        super().__init__(f"{self.path}: {len(problems)} {noun}\n{lines}")


# --------------------------------------------------------------------------- defaults

@dataclass(frozen=True)
class Defaults:
    """Framework defaults from defaults.yaml."""

    datasets: dict[str, str]
    pipeline: dict[str, Any]
    wait_for: dict[str, Any]
    steps: dict[str, dict[str, Any]]


def _check_section(problems: list[str], where: str, values: Any, rules: dict[str, Rule],
                   require_all: bool) -> dict[str, Any]:
    if not isinstance(values, dict):
        problems.append(f"{where} must be a mapping")
        return {}
    for key in sorted(set(values) - set(rules), key=str):
        problems.append(f"{where}: unknown key {key!r}")
    for key, rule in rules.items():
        if key not in values:
            if require_all:
                problems.append(f"{where}.{key} is required")
        elif problem := rule.problem(f"{where}.{key}", values[key]):
            problems.append(problem)
    return {k: v for k, v in values.items() if k in rules}


@functools.lru_cache(maxsize=None)
def load_defaults(path: str | Path = DEFAULTS_PATH) -> Defaults:
    """Load and validate defaults.yaml. Raises ConfigError listing every problem."""
    path = Path(path)
    try:
        raw, problems = _read_yaml(path)
    except (OSError, yaml.YAMLError) as error:
        raise ConfigError(path, [f"cannot read framework defaults: {error}"]) from None
    if not isinstance(raw, dict):
        raise ConfigError(path, problems + ["framework defaults must be a YAML mapping"])
    sections = {"datasets", "pipeline", "wait_for", "steps"}
    for section in sorted(set(raw) - sections, key=str):
        problems.append(f"unknown section {section!r}")

    datasets = _check_section(problems, "datasets", raw.get("datasets"),
                              dict.fromkeys(LAYERS, DATASET), require_all=True)
    pipeline = _check_section(problems, "pipeline", raw.get("pipeline"), PIPELINE_SETTINGS,
                              require_all=True)
    wait_for = _check_section(problems, "wait_for", raw.get("wait_for"), WAIT_FOR_SETTINGS,
                              require_all=True)
    steps: dict[str, dict[str, Any]] = {}
    raw_steps = raw.get("steps") or {}
    if not isinstance(raw_steps, dict):
        problems.append("steps must be a mapping of step type -> settings")
        raw_steps = {}
    for step_name, values in raw_steps.items():
        step_type = steps_lib.STEPS.get(step_name)
        if step_type is None:
            problems.append(f"steps: unknown step type {step_name!r}")
            continue
        steps[step_name] = _check_section(problems, f"steps.{step_name}", values,
                                          step_type.settings, require_all=False)
    if problems:
        raise ConfigError(path, problems)
    return Defaults(datasets=datasets, pipeline=pipeline, wait_for=wait_for, steps=steps)


# --------------------------------------------------------------------------- config objects

@dataclass
class Load:
    """A raw table this DAG loads from GCS files."""

    uri: str
    mode: str                      # replace | period
    format: str                    # BigQuery source format
    partition_field: str | None    # column partitioning; None = ingestion-time partitioning


@dataclass
class WaitFor:
    """A raw table loaded outside this DAG: wait until it covers the period."""

    fresh_when: str                # SQL that returns true once the data covers the period
    timeout_hours: float
    poke_interval_minutes: float


@dataclass
class Ingestion:
    name: str
    dataset: str
    load: Load | None = None
    wait_for: WaitFor | None = None
    checks: list[str] = field(default_factory=list)
    description: str = ""
    layer: str = INGESTION

    @property
    def table(self) -> str:
        return f"{self.dataset}.{self.name}"

    @property
    def uses(self) -> list[str]:
        return []


@dataclass
class Transformation:
    name: str
    dataset: str
    sql_path: str                                         # relative to the pipeline's SQL folder
    ingestion: list[str] = field(default_factory=list)    # ingestion tables it reads
    transformation: list[str] = field(default_factory=list)  # transformation tables it reads
    checks: list[str] = field(default_factory=list)
    description: str = ""
    sql: str = ""                                         # contents of sql_path
    layer: str = TRANSFORMATION

    @property
    def table(self) -> str:
        return f"{self.dataset}.{self.name}"

    @property
    def uses(self) -> list[str]:
        return [*self.ingestion, *self.transformation]


@dataclass
class Step:
    type: str
    settings: dict[str, Any]       # defaults from defaults.yaml, overridden by the report


@dataclass
class Report(Transformation):
    steps: list[Step] = field(default_factory=list)
    layer: str = REPORTS


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
    retries: int
    retry_delay_minutes: float
    max_active_runs: int
    project: str
    location: str | None
    alert_email: list[str]
    export_bucket: str | None
    sql_dir: Path
    ingestion: dict[str, Ingestion]
    transformation: dict[str, Transformation]
    reports: dict[str, Report]
    description: str = ""

    def tables(self) -> dict[str, Ingestion | Transformation | Report]:
        """Every table of every layer, by name."""
        return {**self.ingestion, **self.transformation, **self.reports}

    @property
    def graph(self) -> dict[str, list[str]]:
        """table -> the tables it uses."""
        return {name: list(table.uses) for name, table in self.tables().items()}

    def table(self, name: str) -> str:
        return self.tables()[name].table


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


def load_config(
    path: str | Path,
    sql_dir: str | Path | None = None,
    defaults_path: str | Path = DEFAULTS_PATH,
) -> PipelineConfig:
    """Load and validate one pipeline config. Raises ConfigError listing every problem."""
    path = Path(path)
    defaults = load_defaults(Path(defaults_path))
    try:
        raw, problems = _read_yaml(path)
    except (OSError, yaml.YAMLError) as error:
        raise ConfigError(path, [f"cannot read config: {error}"]) from None
    if not isinstance(raw, dict):
        raise ConfigError(path, problems + ["config must be a YAML mapping"])

    config = _Validator(path, raw, problems, Path(sql_dir) if sql_dir else None, defaults).run()
    if problems:
        raise ConfigError(path, problems)
    return config


class _Validator:
    def __init__(self, path: Path, raw: dict, problems: list[str], sql_dir: Path | None,
                 defaults: Defaults):
        self.path = path
        self.raw = raw
        self.problems = problems
        self.sql_dir = sql_dir
        self.defaults = defaults

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
        for duplicate in sorted({v for v in value if value.count(v) > 1}):
            self.error(f"{where}: {duplicate!r} is listed more than once")
        return list(dict.fromkeys(value))

    def _mapping(self, where: str, value: Any, required: bool = False) -> dict:
        if value is None:
            if required:
                self.error(f"{where} is required")
            return {}
        if not isinstance(value, dict):
            self.error(f"{where} must be a mapping")
            return {}
        return value

    def _settings(self, where: str, data: dict, defaults: dict[str, Any],
                  rules: dict[str, Rule]) -> dict[str, Any]:
        """The defaults, overridden by whatever `data` sets. Invalid values are reported."""
        values = dict(defaults)
        for key, rule in rules.items():
            if key in data:
                if problem := rule.problem(f"{where}.{key}", data[key]):
                    self.error(problem)
                else:
                    values[key] = data[key]
        return values

    # ---- main

    def run(self) -> PipelineConfig:
        raw = self.raw
        self._unknown_keys("config", raw, {"pipeline", *LAYERS})
        pipeline = self._mapping("pipeline", raw.get("pipeline"), required=True)
        self._unknown_keys("pipeline", pipeline, _PIPELINE_KEYS)

        dag_id = self._str("pipeline.dag_id", pipeline.get("dag_id")) or self.path.stem
        schedule = self._str("pipeline.schedule", pipeline.get("schedule")) or ""
        period = pipeline.get("period")
        if period not in PERIODS:
            self.error(f"pipeline.period must be one of {list(PERIODS)}, got {period!r}")
        alert_email = self._str_list("pipeline.alert_email", pipeline.get("alert_email"))
        if not alert_email:
            self.error("pipeline.alert_email is required: list who gets an email when a task fails")
        settings = self._settings("pipeline", pipeline, self.defaults.pipeline, PIPELINE_SETTINGS)
        export_bucket = self._str("pipeline.export_bucket", pipeline.get("export_bucket"),
                                  required=False)

        config = PipelineConfig(
            path=self.path,
            dag_id=dag_id,
            description=pipeline.get("description") or "",
            schedule=schedule,
            period=period if period in PERIODS else "day",
            timezone=settings["timezone"],
            start_date=settings["start_date"],
            gcp_conn_id=settings["gcp_conn_id"],
            smtp_conn_id=settings["smtp_conn_id"],
            retries=settings["retries"],
            retry_delay_minutes=float(settings["retry_delay_minutes"]),
            max_active_runs=settings["max_active_runs"],
            project=self._str("pipeline.project", pipeline.get("project")) or "",
            location=self._str("pipeline.location", pipeline.get("location"), required=False),
            alert_email=alert_email,
            export_bucket=export_bucket,
            sql_dir=self.sql_dir or default_sql_dir(self.path, dag_id),
            ingestion={},
            transformation={},
            reports={},
        )
        config.ingestion = self._ingestion(self._mapping(INGESTION, raw.get(INGESTION)))
        config.transformation = self._sql_tables(
            TRANSFORMATION, self._mapping(TRANSFORMATION, raw.get(TRANSFORMATION)), config)
        config.reports = self._sql_tables(
            REPORTS, self._mapping(REPORTS, raw.get(REPORTS), required=True), config)
        if not config.reports and isinstance(raw.get(REPORTS), dict):
            self.error("reports: the pipeline has no report")

        self._names(config)
        self._references(config)
        self._unused(config)
        self._attach_sql(config)
        self._drift(config)
        self._cycles(config)
        return config

    # ---- ingestion layer

    def _ingestion(self, raw_tables: dict) -> dict[str, Ingestion]:
        tables: dict[str, Ingestion] = {}
        for name, data in raw_tables.items():
            where = f"ingestion.{name}"
            if not isinstance(data, dict):
                self.error(f"{where} must be a mapping")
                continue
            self._unknown_keys(where, data, _INGESTION_KEYS)
            has_load, has_wait = "load" in data, "wait_for" in data
            if has_load == has_wait:
                self.error(
                    f"{where}: needs exactly one of 'load' (loaded by this DAG) "
                    f"or 'wait_for' (loaded elsewhere), found {'both' if has_load else 'neither'}"
                )
            tables[name] = Ingestion(
                name=name,
                dataset=self.defaults.datasets[INGESTION],
                load=self._load(where, data["load"]) if has_load else None,
                wait_for=self._wait_for(where, data["wait_for"]) if has_wait else None,
                checks=self._str_list(f"{where}.checks", data.get("checks")),
                description=data.get("description") or "",
            )
        return tables

    def _load(self, where: str, data: Any) -> Load | None:
        where = f"{where}.load"
        if not isinstance(data, dict):
            self.error(f"{where} must be a mapping with 'uri' and 'mode'")
            return None
        self._unknown_keys(where, data, _LOAD_KEYS)
        uri = self._str(f"{where}.uri", data.get("uri")) or ""
        if uri and not uri.startswith("gs://"):
            self.error(f"{where}.uri must start with 'gs://', got {uri!r}")
        mode = data.get("mode")
        if mode not in LOAD_MODES:
            self.error(f"{where}.mode must be one of {list(LOAD_MODES)}, got {mode!r}")
        fmt = data.get("format") or _format_from_uri(uri)
        if fmt is None:
            self.error(f"{where}.format is required: cannot tell the file format from {uri!r}")
        elif str(fmt).upper() not in LOAD_FORMATS:
            self.error(f"{where}.format must be one of {list(LOAD_FORMATS)}, got {fmt!r}")
        return Load(
            uri=uri,
            mode=mode if mode in LOAD_MODES else "replace",
            format=str(fmt or "PARQUET").upper(),
            partition_field=self._str(f"{where}.partition_field", data.get("partition_field"),
                                      required=False),
        )

    def _wait_for(self, where: str, data: Any) -> WaitFor | None:
        where = f"{where}.wait_for"
        if not isinstance(data, dict):
            self.error(f"{where} must be a mapping with 'fresh_when'")
            return None
        self._unknown_keys(where, data, _WAIT_FOR_KEYS)
        settings = self._settings(where, data, self.defaults.wait_for, WAIT_FOR_SETTINGS)
        return WaitFor(
            fresh_when=self._str(f"{where}.fresh_when", data.get("fresh_when")) or "",
            timeout_hours=float(settings["timeout_hours"]),
            poke_interval_minutes=float(settings["poke_interval_minutes"]),
        )

    # ---- transformation and report layers

    def _sql_tables(self, layer: str, raw_tables: dict, config: PipelineConfig) -> dict:
        tables: dict = {}
        for name, data in raw_tables.items():
            where = f"{layer}.{name}"
            if not isinstance(data, dict):
                self.error(f"{where} must be a mapping")
                continue
            self._unknown_keys(where, data, _REPORT_KEYS if layer == REPORTS else _TRANSFORMATION_KEYS)
            sql_path = self._str(f"{where}.sql", data.get("sql")) or ""
            common = dict(
                name=name,
                dataset=self.defaults.datasets[layer],
                sql_path=sql_path,
                ingestion=self._str_list(f"{where}.ingestion", data.get(INGESTION)),
                transformation=self._str_list(f"{where}.transformation", data.get(TRANSFORMATION)),
                checks=self._str_list(f"{where}.checks", data.get("checks")),
                description=data.get("description") or "",
            )
            if layer == REPORTS:
                tables[name] = Report(**common, steps=self._steps(where, data.get("steps"), config))
            else:
                tables[name] = Transformation(**common)
            if not common["ingestion"] and not common["transformation"]:
                self.error(f"{where}: uses no tables: list them under 'ingestion' and/or "
                           f"'transformation'")
        return tables

    def _steps(self, where: str, raw_steps: Any, config: PipelineConfig) -> list[Step]:
        where = f"{where}.steps"
        if raw_steps is None:
            return []
        if not isinstance(raw_steps, list):
            self.error(f"{where} must be a list, e.g. [approval, export, email: {{to: [...]}}]")
            return []
        steps: list[Step] = []
        seen: set[str] = set()
        for item in raw_steps:
            if isinstance(item, str):
                name, values = item, {}
            elif isinstance(item, dict) and len(item) == 1:
                name, values = next(iter(item.items()))
                values = values if values is not None else {}
            else:
                self.error(f"{where}: each step is a name or `name: {{settings}}`, got {item!r}")
                continue
            step_type = steps_lib.STEPS.get(name)
            if step_type is None:
                self.error(f"{where}: unknown step {name!r} "
                           f"(known: {', '.join(sorted(steps_lib.STEPS))})")
                continue
            if name in seen:
                self.error(f"{where}: step {name!r} is listed more than once")
                continue
            seen.add(name)
            step_where = f"{where}.{name}"
            if not isinstance(values, dict):
                self.error(f"{step_where} settings must be a mapping")
                values = {}
            self._unknown_keys(step_where, values, set(step_type.settings))
            defaults = dict(self.defaults.steps.get(name, {}))
            if name == "export" and config.export_bucket:
                defaults.setdefault("bucket", config.export_bucket)
            settings = self._settings(step_where, values, defaults, step_type.settings)
            for key, rule in step_type.settings.items():
                if rule.required and key not in settings:
                    self.error(f"{step_where} needs {key!r}")
            if step_type.cross_check:
                for problem in step_type.cross_check(settings, config):
                    self.error(f"{step_where} {problem}")
            steps.append(Step(type=name, settings=settings))
        return steps

    # ---- whole-pipeline rules

    def _names(self, config: PipelineConfig) -> None:
        layers_of: dict[str, list[str]] = {}
        for layer in LAYERS:
            for name in getattr(config, layer):
                layers_of.setdefault(name, []).append(layer)
                if not isinstance(name, str) or not name.isidentifier():
                    self.error(f"{layer}.{name}: the name must be a valid identifier "
                               f"(letters, digits, _)")
        for name, layers in layers_of.items():
            if len(layers) > 1:
                self.error(f"name {name!r} is defined twice: under {' and '.join(layers)}")

    def _references(self, config: PipelineConfig) -> None:
        """Each used table exists, in the layer it is listed under."""
        for layer in (TRANSFORMATION, REPORTS):
            for table in getattr(config, layer).values():
                for used_layer in CAN_USE[layer]:
                    for used in getattr(table, used_layer):
                        if used in getattr(config, used_layer):
                            continue
                        where = f"{layer}.{table.name}.{used_layer}"
                        actual = [l for l in LAYERS if used in getattr(config, l)]
                        if not actual:
                            self.error(f"{where}: unknown table {used!r}")
                        elif actual[0] == REPORTS:
                            self.error(f"{where}: {used!r} is a report; reports can't be used by "
                                       f"other tables (move it under transformation)")
                        else:
                            self.error(f"{where}: {used!r} is defined under {actual[0]}, "
                                       f"so list it under '{actual[0]}'")

    def _unused(self, config: PipelineConfig) -> None:
        """Every ingestion and transformation table serves at least one report."""
        graph = {n: [u for u in uses if u in config.graph] for n, uses in config.graph.items()}
        needed = set(config.reports)
        for report in config.reports:
            needed |= graph_lib.upstream(graph, report)
        for layer in (INGESTION, TRANSFORMATION):
            for name in getattr(config, layer):
                if name not in needed:
                    self.error(f"{layer}.{name}: no report uses it, directly or through a "
                               f"transformation (remove it, or add it to a report)")

    def _attach_sql(self, config: PipelineConfig) -> None:
        sql_dir = config.sql_dir
        referenced: set[Path] = set()
        for layer in (TRANSFORMATION, REPORTS):
            for table in getattr(config, layer).values():
                if not table.sql_path:
                    continue
                path = (sql_dir / table.sql_path).resolve()
                if not table.sql_path.endswith(".sql"):
                    self.error(f"{layer}.{table.name}.sql must be a .sql file, got {table.sql_path!r}")
                elif not path.is_file():
                    self.error(f"{layer}.{table.name}.sql: file not found: {sql_dir / table.sql_path}")
                else:
                    table.sql = path.read_text()
                referenced.add(path)
        if sql_dir.is_dir():
            for path in sorted(sql_dir.rglob("*.sql")):
                if path.resolve() not in referenced:
                    self.error(f"SQL file {path} is not used by any transformation or report")

    def _drift(self, config: PipelineConfig) -> None:
        """The tables a SQL file reads must equal the tables its config lists."""
        tables = config.tables()
        for layer in (TRANSFORMATION, REPORTS):
            for table in getattr(config, layer).values():
                if not table.sql:
                    continue
                where = f"{layer}.{table.name}"
                try:
                    reads = sql_parse.tables_read(table.sql)
                except sql_parse.SqlParseError as error:
                    self.error(f"{where}: cannot parse {table.sql_path}: {error}")
                    continue
                expected = {tables[u].table for u in table.uses if u in tables}
                if missing := sorted(expected - reads):
                    self.error(f"{where}: drift: the config lists {missing} but "
                               f"{table.sql_path} does not read them")
                if extra := sorted(reads - expected):
                    self.error(f"{where}: drift: {table.sql_path} reads {extra} but the config "
                               f"doesn't list them")

    def _cycles(self, config: PipelineConfig) -> None:
        graph = {n: [u for u in uses if u in config.graph] for n, uses in config.graph.items()}
        try:
            graph_lib.topological_order(graph)
        except graph_lib.GraphCycleError as error:
            self.error(f"transformation: {error}")


def _format_from_uri(uri: str) -> str | None:
    lowered = uri.lower().rstrip("*")
    for suffix, fmt in ((".parquet", "PARQUET"), (".csv", "CSV"), (".json", "NEWLINE_DELIMITED_JSON"),
                        (".jsonl", "NEWLINE_DELIMITED_JSON"), (".avro", "AVRO"), (".orc", "ORC")):
        if lowered.endswith(suffix):
            return fmt
    return None
