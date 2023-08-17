SELECT IFNULL(invoice.policy_locator, invoice_schedule.policy_locator) AS policy_locator,
      invoice.display_id AS invoice_identifier,
      invoice.total_due AS invoice_amount,
      IFNULL(SUM(invoice_payment.amount), 0.00) AS paid_amount,
      COALESCE(invoice.issue_timestamp, invoice_schedule.issue_timestamp, invoice.created_timestamp) AS issued,
      IFNULL(invoice.start_timestamp, invoice_schedule.start_timestamp) AS period_start,
      IFNULL(invoice.end_timestamp, invoice_schedule.end_timestamp) AS period_end,
      IFNULL(invoice.due_timestamp, invoice_schedule.due_timestamp) AS period_due,
      IF (invoice_schedule.invoice_locator IS NULL,
        "futureInvoice",
        invoice.settlement_status
      ) AS invoice_status
FROM invoice_schedule
RIGHT JOIN invoice ON invoice_schedule.invoice_locator = invoice.locator
LEFT JOIN invoice_payment ON invoice.locator = invoice_payment.invoice_locator
GROUP BY invoice_schedule.id