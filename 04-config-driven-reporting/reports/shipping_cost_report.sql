-- Shipping cost of the day's shipments, per carrier, as a share of order revenue.
CREATE OR REPLACE TABLE reports.shipping_cost_report AS
SELECT
  DATE('{{ data_interval_start | ds }}') AS report_date,
  s.carrier,
  COUNT(*) AS shipments,
  SUM(s.shipping_cost) AS shipping_cost,
  SUM(r.gross_revenue) AS revenue_shipped,
  SAFE_DIVIDE(SUM(s.shipping_cost), SUM(r.gross_revenue)) AS cost_share
FROM raw.shipping AS s
JOIN mart.order_revenue AS r USING (order_id)
WHERE s.shipped_date = DATE('{{ data_interval_start | ds }}')
GROUP BY s.carrier;
