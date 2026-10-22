"""
FINANCE DAG - needs raw_orders + raw_payments.

Shares raw_orders with the sales report DAG.
"""

from common.ensure_fresh import ensure_fresh


def run(con, run_date):
    for table in ["raw_orders", "raw_payments"]:
        ensure_fresh(con, table, run_date)

    con.execute("""
        CREATE OR REPLACE TABLE finance_reconciliation AS
        SELECT o.order_id, o.amount AS order_amount, p.amount AS paid_amount,
               o.amount - coalesce(p.amount, 0) AS outstanding
        FROM raw_orders o LEFT JOIN raw_payments p USING (order_id)
        WHERE o.order_date = CAST($run_date AS DATE)
    """, {"run_date": run_date})
    print("    ✓ finance_reconciliation built")
