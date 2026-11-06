-- Copy invoices from the shop database into raw.invoices (full copy: safe to rerun).
CREATE OR REPLACE TABLE raw.invoices AS
SELECT *
FROM EXTERNAL_QUERY(
  '{{ params.source_connection }}',
  "SELECT invoice_id, order_id, invoice_date, amount, status FROM invoices"
);
