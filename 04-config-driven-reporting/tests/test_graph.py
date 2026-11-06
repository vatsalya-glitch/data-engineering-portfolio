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


# --------------------------------------------------------------------------- the config format

def test_shared_table_is_one_node():
    graph = build_graph(load_config())
    # order_revenue is used by all four reports, and is one node.
    assert sum(node == "transformation.order_revenue" for node in graph) == 1
    assert reports_using(graph)["transformation.order_revenue"] == 4


def test_names_not_in_a_section_are_raw_tables():
    graph = build_graph(load_config())
    raw = {node for node in graph if node.startswith("ingestion.")}
    assert raw == {f"ingestion.{name}" for name in
                   ["orders", "order_items", "invoices", "refunds", "shipping"]}
    assert all(graph[node] == set() for node in raw)


# --------------------------------------------------------------------------- mistakes in the config

def write_scripts(tmp_path, *nodes):
    """Create an empty <layer>/<name>.sql script for each node."""
    for node in nodes:
        layer, name = node.split(".")
        (tmp_path / layer).mkdir(exist_ok=True)
        (tmp_path / layer / f"{name}.sql").write_text("SELECT 1")


def problems(config, tmp_path, *scripts):
    """The error message build_graph gives for `config`, with the given scripts present."""
    write_scripts(tmp_path, *scripts)
    with pytest.raises(ValueError) as error:
        build_graph(config, tmp_path)
    return str(error.value)


def test_missing_script(tmp_path):
    config = {"reports": {"r": ["orders"]}}
    assert "ingestion.orders has no script ingestion/orders.sql" in problems(config, tmp_path, "reports.r")


def test_table_listing_nothing(tmp_path):
    config = {"reports": {"r": []}}
    assert "reports.r lists no tables" in problems(config, tmp_path, "reports.r")


def test_report_used_by_another_table(tmp_path):
    config = {"reports": {"r1": ["orders"], "r2": ["r1"]}}
    assert "r1 is a report" in problems(config, tmp_path, "reports.r1", "reports.r2", "ingestion.orders")


def test_name_in_two_sections(tmp_path):
    config = {"transformation": {"t": ["orders"]}, "reports": {"t": ["orders"]}}
    assert "t is both a transformation and a report" in problems(
        config, tmp_path, "transformation.t", "reports.t", "ingestion.orders")


def test_cycle(tmp_path):
    config = {"transformation": {"a": ["b"], "b": ["a"]}, "reports": {"r": ["a"]}}
    assert "cycle" in problems(config, tmp_path, "transformation.a", "transformation.b", "reports.r")
