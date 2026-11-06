"""Tests for graph.py. They don't need Airflow: run them with `pytest tests`.

The first tests use the real config.yaml. The rest build a tiny config in a
temporary folder and break one rule at a time.
"""

import sys
from pathlib import Path

import pytest

# Make graph.py (one folder up) importable.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from graph import build_graph, build_order, load_config, reports_using  # noqa: E402


# --------------------------------------------------------------------------- the real config

def test_config_builds_a_valid_graph():
    graph = build_graph(load_config())
    assert graph["ingestion.orders"] == set()
    assert graph["transformation.order_revenue"] == {"ingestion.orders", "ingestion.order_items"}
    assert graph["reports.revenue_report"] == {"transformation.net_revenue"}


def test_every_task_runs_after_what_it_needs():
    graph = build_graph(load_config())
    order = build_order(graph)
    for node, needs in graph.items():
        for need in needs:
            assert order.index(need) < order.index(node), (need, node)


def test_reports_using_counts_through_transformations():
    counts = reports_using(build_graph(load_config()))
    assert counts["ingestion.orders"] == 4          # every report, through order_revenue
    assert counts["transformation.order_revenue"] == 4
    assert counts["ingestion.refunds"] == 3         # net_revenue (2 reports) + refunds_report
    assert counts["transformation.net_revenue"] == 2
    assert counts["ingestion.shipping"] == 1
    assert counts["reports.revenue_report"] == 1


def test_most_shared_tables_go_first():
    order = build_order(build_graph(load_config()))
    assert order[:3] == ["ingestion.order_items", "ingestion.orders", "ingestion.refunds"]
    assert order.index("transformation.order_revenue") < order.index("transformation.net_revenue")


# --------------------------------------------------------------------------- mistakes in the config

def config(**layers):
    """A tiny config with one ingestion table; tests add the layers they need."""
    return {"ingestion": ["orders"], "transformation": {}, "reports": {}} | layers


def write_scripts(tmp_path, *nodes):
    """Create an empty <layer>/<name>.sql script for each node."""
    for node in nodes:
        layer, name = node.split(".")
        (tmp_path / layer).mkdir(exist_ok=True)
        (tmp_path / layer / f"{name}.sql").write_text("SELECT 1")


def test_unknown_table(tmp_path):
    write_scripts(tmp_path, "ingestion.orders", "reports.r")
    with pytest.raises(ValueError, match="reports.r uses ingestion.payments, which is not in the config"):
        build_graph(config(reports={"r": {"ingestion": ["payments"]}}), tmp_path)


def test_missing_script(tmp_path):
    write_scripts(tmp_path, "ingestion.orders")
    with pytest.raises(ValueError, match="reports.r has no script reports/r.sql"):
        build_graph(config(reports={"r": {"ingestion": ["orders"]}}), tmp_path)


def test_table_using_nothing(tmp_path):
    write_scripts(tmp_path, "ingestion.orders", "reports.r")
    with pytest.raises(ValueError, match="reports.r uses no tables"):
        build_graph(config(reports={"r": {}}), tmp_path)


def test_cycle(tmp_path):
    write_scripts(tmp_path, "ingestion.orders", "transformation.a", "transformation.b", "reports.r")
    bad = config(
        transformation={"a": {"transformation": ["b"]}, "b": {"transformation": ["a"]}},
        reports={"r": {"transformation": ["a"]}},
    )
    with pytest.raises(ValueError, match="cycle"):
        build_graph(bad, tmp_path)
