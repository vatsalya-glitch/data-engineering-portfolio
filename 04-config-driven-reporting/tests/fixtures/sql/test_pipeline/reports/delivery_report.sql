CREATE OR REPLACE TABLE reports.delivery_report AS
SELECT c.region, COUNT(*) AS orders FROM mart.order_shipments AS os
JOIN raw.customers AS c USING (customer_id) GROUP BY c.region
