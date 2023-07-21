from socotra_datamart_reports.lib import queries_platform as queries
from socotra_datamart_reports.lib.base_report import BaseReport


class FinancialTransactionsReport(BaseReport):
    """
    Summarizes financial transactions posted in a given time range
    """

    def __init__(self, creds):
        super().__init__(creds)

    def get_financial_transactions_report(
            self, start_timestamp: int, end_timestamp: int):
        """
        Returns financial transaction report results, one dict per row
        :param start_timestamp: timestamp (Unix epoch, milliseconds)
        :param end_timestamp: timestamp (Unix epoch, milliseconds)
        :return:
        """
        if end_timestamp <= start_timestamp:
            raise ValueError("start_timestamp must be < end_timestamp")

        query = queries.get_financial_transactions_query(
            start_timestamp, end_timestamp)

        return self.fetch_all_results_for_query(query)

    def write_financial_transactions_report(
            self, start_timestamp: int, end_timestamp: int,
            report_file_path: str):
        """
        Writes a financial transactions report, including flattened field
        values, to specified file path (CSV)
        :param start_timestamp: timestamp (Unix epoch, milliseconds)
        :param end_timestamp: timestamp (Unix epoch, milliseconds)
        :param report_file_path: path/name of the file to which to write
                results
        :return:
        """
        results = self.get_financial_transactions_report(
            start_timestamp, end_timestamp)
        self.write_report_results(results, report_file_path)
