import pytest

from report_framework.graph import (
    GraphCycleError,
    build_order,
    report_usage_counts,
    topological_order,
    upstream,
)

GRAPH = {
    "orders": [],
    "shipments": [],
    "customers": [],
    "order_shipments": ["orders", "shipments"],
    "enriched": ["order_shipments", "customers"],
    "delivery_report": ["enriched"],
    "sales_report": ["orders", "customers"],
    "ops_report": ["order_shipments"],
}
REPORTS = ["delivery_report", "sales_report", "ops_report"]


def test_every_node_comes_after_what_it_uses():
    order = topological_order(GRAPH)
    assert sorted(order) == sorted(GRAPH)
    for node, uses in GRAPH.items():
        for used in uses:
            assert order.index(used) < order.index(node), (used, node)


def test_order_is_stable():
    assert topological_order(GRAPH) == topological_order(dict(reversed(list(GRAPH.items()))))


def test_two_node_cycle_message():
    with pytest.raises(GraphCycleError) as error:
        topological_order({"a": ["b"], "b": ["a"]})
    assert str(error.value) == "cycle in uses: a -> b -> a"
    assert error.value.cycle == ["a", "b", "a"]


def test_indirect_cycle_message_reads_as_uses():
    # a uses b, b uses c, c uses a
    with pytest.raises(GraphCycleError) as error:
        topological_order({"c": ["a"], "a": ["b"], "b": ["c"], "x": []})
    assert str(error.value) == "cycle in uses: a -> b -> c -> a"


def test_self_cycle():
    with pytest.raises(GraphCycleError, match="a -> a"):
        topological_order({"a": ["a"]})


def test_upstream_is_transitive():
    assert upstream(GRAPH, "delivery_report") == {
        "enriched", "order_shipments", "orders", "shipments", "customers"
    }
    assert upstream(GRAPH, "orders") == set()


def test_usage_counts_go_through_intermediates():
    counts = report_usage_counts(GRAPH, REPORTS)
    assert counts["orders"] == 3          # delivery (via 2 intermediates), sales, ops
    assert counts["shipments"] == 2       # delivery, ops
    assert counts["customers"] == 2       # delivery (via enriched), sales
    assert counts["order_shipments"] == 2
    assert counts["enriched"] == 1
    assert counts["delivery_report"] == 1  # a report counts itself


def test_unused_node_counts_zero():
    counts = report_usage_counts({"a": [], "r": [], "unused": []}, ["r"])
    assert counts["unused"] == 0


def test_priority_breaks_ties_between_ready_nodes():
    graph = {"a": [], "b": [], "c": [], "x": ["a", "b", "c"]}
    assert topological_order(graph, {"c": 3, "b": 2}) == ["c", "b", "a", "x"]


def test_build_order_puts_the_most_shared_tables_first():
    order = build_order(GRAPH, REPORTS)
    # orders feeds 3 reports, customers and shipments 2 each.
    assert order[:3] == ["orders", "customers", "shipments"]
    # Dependencies still win over sharing.
    for node, uses in GRAPH.items():
        for used in uses:
            assert order.index(used) < order.index(node)
