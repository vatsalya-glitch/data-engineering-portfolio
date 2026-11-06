-- Gross, refunded and net revenue of the day's orders, per currency.
CREATE OR REPLACE TABLE reports.revenue_report AS
SELECT
  DATE('{{ data_interval_start | ds }}') AS report_date,
  currency,
  COUNT(*) AS orders,
  SUM(gross_revenue) AS gross_revenue,
  SUM(refunded) AS refunded,
  SUM(net_revenue) AS net_revenue
FROM mart.net_revenue
WHERE order_date = DATE('{{ data_interval_start | ds }}')
GROUP BY currency;
