"""Find the tables a SQL query reads. No Airflow imports.

Used by the drift check (UC4): the tables a model's SQL reads must equal
the `uses` list in its config.

Rules:
- Parsed with sqlglot, BigQuery dialect.
- Jinja expressions (`{{ ... }}`) are replaced with a placeholder first,
  because sqlglot cannot parse them. Jinja statements (`{% ... %}`) and
  comments (`{# ... #}`) are removed.
- Tables a statement writes (CREATE ... AS, INSERT INTO, MERGE INTO) are
  not reads.
- CTE names are not reads.
- Names are returned as `dataset.table` (any project prefix is dropped),
  or as `table` when the SQL does not qualify them.
"""

from __future__ import annotations

import re

import sqlglot
from sqlglot import exp

TEMPLATE_PLACEHOLDER = "__template__"

_JINJA_EXPRESSION = re.compile(r"\{\{.*?\}\}", re.DOTALL)
_JINJA_STATEMENT_OR_COMMENT = re.compile(r"\{%.*?%\}|\{#.*?#\}", re.DOTALL)


class SqlParseError(ValueError):
    """The SQL could not be parsed."""


def mask_templates(sql: str) -> str:
    """Hide Airflow/Jinja template syntax so sqlglot can parse the SQL."""
    sql = _JINJA_STATEMENT_OR_COMMENT.sub(" ", sql)
    return _JINJA_EXPRESSION.sub(TEMPLATE_PLACEHOLDER, sql)


def _qualified_name(table: exp.Table) -> str:
    return f"{table.db}.{table.name}" if table.db else table.name


def _written_table(statement: exp.Expression) -> exp.Table | None:
    """The table a statement writes, if any."""
    if isinstance(statement, (exp.Create, exp.Insert, exp.Merge)):
        target = statement.this
        if isinstance(target, exp.Schema):  # CREATE TABLE t (col ...) / INSERT INTO t (cols)
            target = target.this
        if isinstance(target, exp.Table):
            return target
    return None


def tables_read(sql: str) -> set[str]:
    """Return the tables a SQL script reads, as `dataset.table` names."""
    try:
        statements = sqlglot.parse(mask_templates(sql), read="bigquery")
    except sqlglot.errors.ParseError as error:
        raise SqlParseError(str(error).splitlines()[0]) from error

    reads: set[str] = set()
    for statement in statements:
        if statement is None:  # empty statement, e.g. a trailing ";"
            continue
        written = _written_table(statement)
        cte_names = {cte.alias_or_name for cte in statement.find_all(exp.CTE)}
        for table in statement.find_all(exp.Table):
            if table is written or not table.name:
                continue
            if not table.db and table.name in cte_names:
                continue
            reads.add(_qualified_name(table))
    return reads
