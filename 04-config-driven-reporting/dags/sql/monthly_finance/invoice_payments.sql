-- Each invoice of the month with what has been paid against it up to the month end.
CREATE OR REPLACE TABLE mart.invoice_payments AS
WITH month_invoices AS (
  SELECT invoice_id, customer_id, currency, amount
  FROM raw.invoices
  WHERE invoice_month = DATE('{{ period_start }}')
),
paid AS (
  SELECT invoice_id, SUM(amount) AS paid
  FROM raw.payments
  WHERE paid_at < TIMESTAMP('{{ period_end }}')
  GROUP BY invoice_id
)
SELECT
  i.invoice_id,
  i.customer_id,
  i.currency,
  i.amount AS invoiced,
  COALESCE(p.paid, 0) AS paid
FROM month_invoices AS i
LEFT JOIN paid AS p USING (invoice_id);
