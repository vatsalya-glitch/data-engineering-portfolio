-- Revenue by region, segment and product category for the day.
CREATE OR REPLACE TABLE reports.daily_sales_report AS
SELECT
  DATE('{{ period_start }}') AS period_start,
  c.region,
  c.segment,
  ol.category,
  COUNT(DISTINCT ol.order_id) AS orders,
  SUM(ol.quantity) AS units,
  SUM(ol.amount) AS revenue
FROM mart.order_lines AS ol
JOIN raw.customers AS c USING (customer_id)
GROUP BY c.region, c.segment, ol.category;
