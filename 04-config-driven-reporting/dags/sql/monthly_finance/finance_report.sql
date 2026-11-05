-- Invoiced, paid and outstanding for the month, by currency.
CREATE OR REPLACE TABLE reports.finance_report AS
SELECT
  DATE('{{ period_start }}') AS period_start,
  currency,
  COUNT(*) AS invoices,
  SUM(invoiced) AS invoiced,
  SUM(paid) AS paid,
  SUM(invoiced - paid) AS outstanding,
  COUNTIF(paid < invoiced) AS unpaid_invoices
FROM mart.invoice_payments
GROUP BY currency;
