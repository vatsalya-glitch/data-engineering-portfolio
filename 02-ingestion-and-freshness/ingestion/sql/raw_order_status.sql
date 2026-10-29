-- WRITE step for CDC: one change-log file, keeping only the LATEST change per order.
-- (1001 created -> paid -> shipped becomes one row: shipped)
CREATE OR REPLACE TEMP TABLE stg_raw_order_status AS
SELECT op, order_id, status, changed_at
FROM read_csv($source_file)
QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY changed_at DESC) = 1;
