-- The day's order lines with product details.
CREATE OR REPLACE TABLE mart.order_lines AS
SELECT
  o.order_id,
  o.customer_id,
  o.order_date,
  o.product_id,
  p.category,
  o.quantity,
  o.amount
FROM raw.orders AS o
JOIN raw.products AS p USING (product_id)
WHERE o.order_date >= DATE('{{ period_start }}') AND o.order_date < DATE('{{ period_end }}');
