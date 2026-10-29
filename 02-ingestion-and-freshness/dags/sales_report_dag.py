"""
SALES REPORT DAG - needs raw_orders + raw_customers.

Each "task" is a plain function so the project runs without Airflow.
See the README for the same DAG written for Airflow + BigQuery.
"""

from freshness.ensure_fresh import ensure_fresh


def run(con, run_date):
    # Upstream tasks: make sure every input table is fresh
    for table in ["raw_orders", "raw_customers"]:
        ensure_fresh(con, table, run_date)

    # Downstream task: the DAG's own logic
    con.execute("""
        CREATE OR REPLACE TABLE sales_report AS
        SELECT o.order_date, c.country, count(*) AS orders, sum(o.amount) AS revenue
        FROM raw_orders o JOIN raw_customers c USING (customer_id)
        WHERE o.order_date = CAST($run_date AS DATE)
        GROUP BY ALL
    """, {"run_date": run_date})
    print("    ✓ sales_report built")
