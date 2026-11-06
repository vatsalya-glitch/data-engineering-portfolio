-- Copy shipping from the shop database into raw.shipping (full copy: safe to rerun).
CREATE OR REPLACE TABLE raw.shipping AS
SELECT *
FROM EXTERNAL_QUERY(
  '{{ params.source_connection }}',
  "SELECT shipment_id, order_id, carrier, shipped_date, shipping_cost FROM shipping"
);
