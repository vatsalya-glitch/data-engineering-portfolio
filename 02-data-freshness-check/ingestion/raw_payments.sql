-- Ingest one day of payments from the external source.
-- Idempotent: delete the day first, then insert it, so a rerun never duplicates rows.
CREATE TABLE IF NOT EXISTS raw_payments (
    payment_id INTEGER, order_id INTEGER, payment_date DATE,
    amount DECIMAL(10, 2), method VARCHAR, _loaded_at TIMESTAMP
);

DELETE FROM raw_payments WHERE payment_date = CAST($run_date AS DATE);

INSERT INTO raw_payments
SELECT payment_id, order_id, payment_date, amount, method, now()
FROM read_csv('data/source/payments.csv')
WHERE payment_date = CAST($run_date AS DATE);
