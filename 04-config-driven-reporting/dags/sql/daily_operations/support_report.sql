-- Support tickets opened during the day, by region and category.
CREATE OR REPLACE TABLE reports.support_report AS
SELECT
  DATE('{{ period_start }}') AS period_start,
  c.region,
  t.category,
  COUNT(*) AS tickets_opened,
  COUNTIF(t.status = 'resolved') AS resolved_same_day
FROM raw.tickets AS t
JOIN raw.customers AS c USING (customer_id)
WHERE t.created_at >= TIMESTAMP('{{ period_start }}') AND t.created_at < TIMESTAMP('{{ period_end }}')
GROUP BY c.region, t.category;
