-- Returns registered during the day, by product category and reason.
CREATE OR REPLACE TABLE reports.returns_report AS
WITH day_returns AS (
  SELECT return_id, order_id, product_id, reason, refund_amount
  FROM raw.returns
  WHERE return_date >= DATE('{{ period_start }}') AND return_date < DATE('{{ period_end }}')
)
SELECT
  DATE('{{ period_start }}') AS period_start,
  ol.category,
  r.reason,
  COUNT(*) AS returns,
  SUM(r.refund_amount) AS refunded
FROM day_returns AS r
LEFT JOIN mart.order_lines AS ol USING (order_id, product_id)
GROUP BY ol.category, r.reason;
