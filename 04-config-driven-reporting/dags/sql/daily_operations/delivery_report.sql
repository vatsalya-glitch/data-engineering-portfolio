-- On-time delivery by region and carrier for the day's orders.
CREATE OR REPLACE TABLE reports.delivery_report AS
SELECT
  DATE('{{ period_start }}') AS period_start,
  c.region,
  os.carrier,
  COUNT(*) AS orders,
  COUNTIF(os.shipped_at IS NOT NULL) AS shipped,
  COUNTIF(os.delivered_at IS NOT NULL) AS delivered,
  COUNTIF(os.is_late) AS late
FROM mart.order_shipments AS os
JOIN raw.customers AS c USING (customer_id)
GROUP BY c.region, os.carrier;
