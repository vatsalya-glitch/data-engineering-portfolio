-- WRITE step for an incremental load: only rows changed since the last watermark.
CREATE OR REPLACE TEMP TABLE stg_raw_orders AS
SELECT order_id, customer_id, order_date, amount, updated_at
FROM read_csv($source_file)
WHERE updated_at > CAST($watermark AS TIMESTAMP);
