# Data Mart Reporting

A library for common [Data Mart](https://docs.socotra.com/production/data/datamartguide.html) reporting needs.

## Reports

Reports prefaced with `platform_` have [analogues in the Socotra platform](https://docs.socotra.com/production/data/reporting.html).

You can run reports by passing database credentials to the report class instance, then calling the appropriate execution method.

Example:

```python
from socotra_datamart_reports import TransactionFinancialImpactReport

creds = {
    'user': 'my-datamart-username',
    'password': 'my-datamart-password',
    'port': 'my-datamart-port',
    'host': 'my-datamart-host',
    'database': 'my-datamart-database'
}

tfir = TransactionFinancialImpactReport(creds)
tfir.write_transaction_financial_impact_report(
    'personal-auto', 1659326400000, 1664596800000, 'tx_report.csv')
```
