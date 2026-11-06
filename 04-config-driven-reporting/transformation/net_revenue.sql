-- Net revenue per order: gross revenue minus everything refunded on it.
CREATE OR REPLACE TABLE mart.net_revenue AS
WITH refunded AS (
  SELECT order_id, SUM(amount) AS refunded
  FROM raw.refunds
  GROUP BY order_id
)
SELECT
  r.order_id,
  r.customer_id,
  r.order_date,
  r.currency,
  r.gross_revenue,
  COALESCE(f.refunded, 0) AS refunded,
  r.gross_revenue - COALESCE(f.refunded, 0) AS net_revenue
FROM mart.order_revenue AS r
LEFT JOIN refunded AS f USING (order_id);
