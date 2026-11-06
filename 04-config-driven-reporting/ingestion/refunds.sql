-- Copy refunds from the shop database into raw.refunds (full copy: safe to rerun).
CREATE OR REPLACE TABLE raw.refunds AS
SELECT *
FROM EXTERNAL_QUERY(
  '{{ params.source_connection }}',
  "SELECT refund_id, order_id, refund_date, amount, reason FROM refunds"
);
