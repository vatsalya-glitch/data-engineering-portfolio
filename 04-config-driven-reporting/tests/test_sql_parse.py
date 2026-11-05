import pytest

from report_framework.sql_parse import SqlParseError, mask_templates, tables_read


def test_simple_select():
    assert tables_read("SELECT * FROM raw.orders") == {"raw.orders"}


def test_joins_and_subqueries():
    sql = """
        SELECT * FROM raw.orders o
        JOIN raw.customers c ON o.customer_id = c.customer_id
        WHERE o.product_id IN (SELECT product_id FROM raw.products)
    """
    assert tables_read(sql) == {"raw.orders", "raw.customers", "raw.products"}


def test_create_or_replace_target_is_not_a_read():
    sql = "CREATE OR REPLACE TABLE reports.daily AS SELECT * FROM mart.order_shipments"
    assert tables_read(sql) == {"mart.order_shipments"}


def test_create_with_partition_and_options():
    sql = """
        CREATE OR REPLACE TABLE reports.daily
        PARTITION BY order_date CLUSTER BY region
        OPTIONS (description = 'x')
        AS SELECT * FROM raw.orders
    """
    assert tables_read(sql) == {"raw.orders"}


def test_ctes_are_not_reads():
    sql = """
        CREATE OR REPLACE TABLE mart.x AS
        WITH day_orders AS (SELECT * FROM raw.orders),
             latest AS (SELECT * FROM day_orders)
        SELECT * FROM latest JOIN raw.shipments USING (order_id)
    """
    assert tables_read(sql) == {"raw.orders", "raw.shipments"}


def test_a_qualified_table_named_like_a_cte_is_still_a_read():
    sql = "WITH orders AS (SELECT * FROM raw.orders) SELECT * FROM orders"
    assert tables_read(sql) == {"raw.orders"}


def test_jinja_is_hidden_before_parsing():
    sql = """
        {# a comment #}
        CREATE OR REPLACE TABLE mart.x AS
        SELECT * FROM raw.orders
        WHERE order_date >= DATE('{{ period_start }}')
          AND order_date < {{ period_end }}
        {% if true %} AND 1 = 1 {% endif %}
    """
    assert tables_read(sql) == {"raw.orders"}


def test_mask_templates():
    assert "{{" not in mask_templates("SELECT '{{ period_start }}', {{ x | y }}")
    assert "{%" not in mask_templates("{% set a = 1 %}SELECT 1")


def test_project_prefix_is_dropped():
    assert tables_read("SELECT * FROM `my-project.raw.orders`") == {"raw.orders"}


def test_unqualified_tables_are_returned_as_is():
    assert tables_read("SELECT * FROM orders") == {"orders"}


def test_insert_and_merge_targets_are_not_reads():
    sql = """
        INSERT INTO mart.t (a) SELECT a FROM raw.a;
        MERGE mart.t T USING raw.b S ON T.a = S.a WHEN MATCHED THEN DELETE;
    """
    assert tables_read(sql) == {"raw.a", "raw.b"}


def test_several_statements():
    sql = "SELECT 1 FROM raw.a; SELECT 2 FROM raw.b;"
    assert tables_read(sql) == {"raw.a", "raw.b"}


def test_unparseable_sql():
    with pytest.raises(SqlParseError):
        tables_read("SELECT FROM WHERE (")
