import os
from dotenv import load_dotenv
from reports.lib.platform_on_risk_report import OnRiskReport
from reports.lib.transaction_financial_impact_report import TransactionFinancialImpactReport

load_dotenv()

if __name__ == "__main__":
    creds = {
        'user': os.environ.get('REPORT_USER'),
        'password': os.environ.get('REPORT_PASSWORD'),
        'port': os.environ.get('REPORT_PORT'),
        'host': os.environ.get('REPORT_HOST'),
        'database': os.environ.get('REPORT_DATABASE')
    }

    orr = OnRiskReport(creds)
    txr = TransactionFinancialImpactReport(creds)

    print('Writing an on-risk report...')
    orr.write_on_risk_report('personal-auto', 1667278800000, 'on_risk_report.csv')

    print('Writing a transactions financial impact report...')
    txr.write_transaction_financial_impact_report(
        'personal-auto', 1635739200000, 1698811200000, 'tx_report_results.csv')


