-- Ingest one day of orders from the external source.
-- Idempotent: delete the day first, then insert it, so a rerun never duplicates rows.
CREATE TABLE IF NOT EXISTS raw_orders (
    order_id INTEGER, customer_id INTEGER, order_date DATE,
    amount DECIMAL(10, 2), _loaded_at TIMESTAMP
);

DELETE FROM raw_orders WHERE order_date = CAST($run_date AS DATE);

INSERT INTO raw_orders
SELECT order_id, customer_id, order_date, amount, now()
FROM read_csv('data/source/orders.csv')
WHERE order_date = CAST($run_date AS DATE);
