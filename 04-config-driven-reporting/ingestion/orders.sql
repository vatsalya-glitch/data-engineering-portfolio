-- Copy orders from the shop database into raw.orders (full copy: safe to rerun).
CREATE OR REPLACE TABLE raw.orders AS
SELECT *
FROM EXTERNAL_QUERY(
  '{{ params.source_connection }}',
  "SELECT order_id, customer_id, order_date, status, currency FROM orders"
);
