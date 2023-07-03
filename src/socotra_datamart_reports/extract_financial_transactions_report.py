from socotra_datamart_reports.lib import queries_extract as queries
from socotra_datamart_reports.lib.base_report import \
    BaseReport, write_report_results


class FinancialTransactionsReport(BaseReport):
    """
    Summarizes financial transactions posted in a given time range
    """
    def __init__(self, creds):
        super().__init__(creds)
        self.name = "financial_transactions"
        self.argument_specs = [
            {
                "name": "start_timestamp",
                "required": False,
                "default": False
            },
            {
                "name": "end_timestamp",
                "required": True,
                "default": True                
            }
        ]

    def get(
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

    def write(
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
        results = self.get(
            start_timestamp, end_timestamp)
        write_report_results(results, report_file_path)
