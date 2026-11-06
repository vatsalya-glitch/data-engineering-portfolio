CREATE OR REPLACE TABLE mart.order_shipments AS
SELECT o.order_id, s.carrier FROM raw.orders AS o
JOIN raw.shipments AS s USING (order_id)
WHERE o.order_date = DATE('{{ period_start }}')
