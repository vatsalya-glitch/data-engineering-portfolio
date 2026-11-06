-- Gross revenue per order: the order with the sum of its items.
CREATE OR REPLACE TABLE mart.order_revenue AS
SELECT
  o.order_id,
  o.customer_id,
  o.order_date,
  o.currency,
  SUM(i.quantity * i.unit_price) AS gross_revenue
FROM raw.orders AS o
JOIN raw.order_items AS i USING (order_id)
WHERE o.status != 'cancelled'
GROUP BY o.order_id, o.customer_id, o.order_date, o.currency;
