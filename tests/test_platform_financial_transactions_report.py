import csv
import pytest
from unittest.mock import MagicMock
from socotra_datamart_reports import FinancialTransactionsReport

MOCK_DATA_PATH = \
    'tests/test_data/financial_transactions_query_sample_result.csv'


def test_financial_transactions_report():
    p = FinancialTransactionsReport({})

    with open(MOCK_DATA_PATH, 'r') as mock_data_csv_file:
        mock_db_response = list(csv.DictReader(mock_data_csv_file))

    p.fetch_all_results_for_query = MagicMock(return_value=mock_db_response)

    results = p.get_financial_transactions_report(0, 10000)

    # simple test since we're just forwarding the results from the query
    assert results == mock_db_response


def test_financial_transactions_report_validation():
    p = FinancialTransactionsReport({})
    with pytest.raises(ValueError):
        p.get_financial_transactions_report(0, 0)

    with pytest.raises(ValueError):
        p.get_financial_transactions_report(10, 2)
