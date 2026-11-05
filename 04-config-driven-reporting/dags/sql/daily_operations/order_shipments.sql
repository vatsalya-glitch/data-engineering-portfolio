-- The day's orders with their (latest) shipment. One row per order.
CREATE OR REPLACE TABLE mart.order_shipments AS
WITH day_orders AS (
  SELECT order_id, customer_id, MIN(order_date) AS order_date, SUM(amount) AS order_amount
  FROM raw.orders
  WHERE order_date >= DATE('{{ period_start }}') AND order_date < DATE('{{ period_end }}')
  GROUP BY order_id, customer_id
),
latest_shipment AS (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT order_id, carrier, shipped_at, delivered_at, promised_at,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_time DESC) AS rn
    FROM raw.shipments
  )
  WHERE rn = 1
)
SELECT
  o.order_id,
  o.customer_id,
  o.order_date,
  o.order_amount,
  s.carrier,
  s.shipped_at,
  s.delivered_at,
  s.delivered_at > s.promised_at AS is_late
FROM day_orders AS o
LEFT JOIN latest_shipment AS s USING (order_id);
