"""Config-driven reporting pipelines on Airflow.

One shared framework, many configs: one YAML config = one pipeline = one DAG.

`build_dag` is imported lazily, so `config`, `graph` and `sql_parse` can be
used (and tested) without Airflow installed.
"""

from .config import ConfigError, load_config

__all__ = ["ConfigError", "build_dag", "load_config"]


def build_dag(config_path, sql_dir=None):
    """Build the Airflow DAG for one pipeline config. See builder.build_dag."""
    from .builder import build_dag as _build_dag

    return _build_dag(config_path, sql_dir=sql_dir)
