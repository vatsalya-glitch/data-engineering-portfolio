-- The day's orders whose invoiced amount doesn't match their net revenue.
CREATE OR REPLACE TABLE reports.invoice_reconciliation_report AS
WITH invoiced AS (
  SELECT order_id, SUM(amount) AS invoiced
  FROM raw.invoices
  WHERE status != 'void'
  GROUP BY order_id
)
SELECT
  DATE('{{ data_interval_start | ds }}') AS report_date,
  n.order_id,
  n.currency,
  n.net_revenue,
  COALESCE(i.invoiced, 0) AS invoiced,
  n.net_revenue - COALESCE(i.invoiced, 0) AS difference
FROM mart.net_revenue AS n
LEFT JOIN invoiced AS i USING (order_id)
WHERE n.order_date = DATE('{{ data_interval_start | ds }}')
  AND ABS(n.net_revenue - COALESCE(i.invoiced, 0)) >= 0.01;
