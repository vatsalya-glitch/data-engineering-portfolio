-- WRITE step for a snapshot: read the whole source into staging.
CREATE OR REPLACE TEMP TABLE stg_raw_customers AS
SELECT customer_id, name, country
FROM read_csv($source_file);
