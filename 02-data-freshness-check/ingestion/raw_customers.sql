-- Ingest the full customer snapshot (a dimension table, not split by day).
-- Idempotent: the whole table is replaced on every run.
CREATE OR REPLACE TABLE raw_customers AS
SELECT customer_id, name, country, now() AS _loaded_at
FROM read_csv('data/source/customers.csv');
