-- Copy order_items from the shop database into raw.order_items (full copy: safe to rerun).
CREATE OR REPLACE TABLE raw.order_items AS
SELECT *
FROM EXTERNAL_QUERY(
  '{{ params.source_connection }}',
  "SELECT order_item_id, order_id, product_id, quantity, unit_price FROM order_items"
);
