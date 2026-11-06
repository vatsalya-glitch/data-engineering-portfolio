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
    # order_revenue is written under all four reports, but it is one node.
    assert sum(node == "transformation.order_revenue" for node in graph) == 1
    assert reports_using(graph)["transformation.order_revenue"] == 4


def test_nested_transformation_needs_its_inner_transformation():
    graph = build_graph(load_config())
    assert graph["transformation.net_revenue"] == {"ingestion.refunds", "transformation.order_revenue"}
    assert graph["reports.refunds_report"] == {"ingestion.refunds", "transformation.order_revenue"}


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
    config = {"reports": {"r": {"ingestion": ["orders"]}}}
    assert "ingestion.orders has no script ingestion/orders.sql" in problems(config, tmp_path, "reports.r")


def test_report_using_nothing(tmp_path):
    config = {"reports": {"r": {}}}
    assert "reports.r uses no tables" in problems(config, tmp_path, "reports.r")


def test_transformation_using_nothing(tmp_path):
    config = {"reports": {"r": {"transformation": {"t": {}}}}}
    assert "reports.r.transformation.t uses no tables" in problems(
        config, tmp_path, "reports.r", "transformation.t")


def test_shared_table_described_differently(tmp_path):
    config = {"reports": {
        "r1": {"transformation": {"t": {"ingestion": ["orders"]}}},
        "r2": {"transformation": {"t": {"ingestion": ["orders", "refunds"]}}},
    }}
    message = problems(config, tmp_path, "reports.r1", "reports.r2", "transformation.t",
                       "ingestion.orders", "ingestion.refunds")
    assert "transformation.t is described differently in reports.r1.transformation.t" in message
    assert "reports.r2.transformation.t" in message


def test_unknown_key(tmp_path):
    config = {"reports": {"r": {"ingestion": ["orders"], "steps": ["email"]}}}
    assert "reports.r has an unknown key 'steps'" in problems(
        config, tmp_path, "reports.r", "ingestion.orders")


def test_ingestion_must_be_a_list(tmp_path):
    config = {"reports": {"r": {"ingestion": "orders"}}}
    assert "reports.r.ingestion must be a list" in problems(config, tmp_path, "reports.r")


def test_no_reports(tmp_path):
    assert "needs a 'reports:' key" in problems({"ingestion": ["orders"]}, tmp_path)
