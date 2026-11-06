-- For the day's orders: how many were late, returned, or caused a ticket.
CREATE OR REPLACE TABLE reports.customer_experience_report AS
WITH returned AS (
  SELECT DISTINCT order_id FROM raw.returns
  WHERE return_date >= DATE('{{ period_start }}') AND return_date < DATE('{{ period_end }}')
),
ticketed AS (
  SELECT DISTINCT order_id FROM raw.tickets
  WHERE created_at >= TIMESTAMP('{{ period_start }}') AND created_at < TIMESTAMP('{{ period_end }}')
)
SELECT
  DATE('{{ period_start }}') AS period_start,
  COUNT(*) AS orders,
  COUNTIF(os.is_late) AS late_orders,
  COUNTIF(r.order_id IS NOT NULL) AS returned_orders,
  COUNTIF(t.order_id IS NOT NULL) AS orders_with_ticket
FROM mart.order_shipments AS os
LEFT JOIN returned AS r USING (order_id)
LEFT JOIN ticketed AS t USING (order_id);
