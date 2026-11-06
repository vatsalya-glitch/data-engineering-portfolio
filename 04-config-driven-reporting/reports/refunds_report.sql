-- Refunds issued during the day, per reason, against the revenue of the refunded orders.
CREATE OR REPLACE TABLE reports.refunds_report AS
SELECT
  DATE('{{ data_interval_start | ds }}') AS report_date,
  f.reason,
  COUNT(*) AS refunds,
  SUM(f.amount) AS refunded,
  SUM(r.gross_revenue) AS revenue_of_refunded_orders
FROM raw.refunds AS f
JOIN mart.order_revenue AS r USING (order_id)
WHERE f.refund_date = DATE('{{ data_interval_start | ds }}')
GROUP BY f.reason;
