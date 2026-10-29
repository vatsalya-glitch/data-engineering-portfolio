-- WRITE step for a file drop: read the vendor's file for one day.
CREATE OR REPLACE TEMP TABLE stg_raw_payments AS
SELECT payment_id, order_id, payment_date, amount, method
FROM read_csv($source_file);
