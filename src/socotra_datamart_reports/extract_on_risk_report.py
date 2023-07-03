from socotra_datamart_reports.lib import queries_platform as queries, get_flattened_fields
from socotra_datamart_reports.lib.base_report import \
    BaseReport, write_report_results


class OnRiskReport(BaseReport):
    """
    Fetches on-risk results from Data Mart, with ability to write a report with
    "flattened" fields. "Flattened" fields are denormalized; since each row of
    the report corresponds to a single peril characteristic, we show relevant
    fields by putting each one in its own labeled column, for all three levels
    (Policy, Exposure, Peril). Repeated fields or field groups receive
    numerical qualifiers.
    """

    def __init__(self, creds):
        super().__init__(creds)
        self.argument_specs = [
            {
                "name": "product_name",
                "required": False,
                "default": False
            },
            {
                "name": "as_of_timestamp",
                "required": True,
                "default": True                
            }
        ]

    def fetch(self, product_name: str, as_of_timestamp: int):
        """
        Fetches raw on-risk data, with field values aggregated into JSON at
        each of the peril, exposure, policy levels
        :param product_name: name of the product
        :param as_of_timestamp: timestamp (Unix epoch, milliseconds)
        :return:
        """
        query = queries.get_on_risk_query(product_name, as_of_timestamp)
        return self.fetch_all_results_for_query(query)

    def get(
            self, product_name: str, as_of_timestamp: int):
        """
        Returns result set with field values "flattened", i.e., made into
        columns in the row, with numerical designators to disambiguate repeated
        fields
        :param product_name: name of the product
        :param as_of_timestamp: timestamp (Unix epoch, milliseconds)
        :return:
        """
        return get_flattened_fields.get_flattened_results(
            self.fetch_on_risk_data(product_name, as_of_timestamp))

    def write(self, product_name: str, as_of_timestamp: int,
                             report_file_path: str):
        """
        Writes an on-risk report set, including flattened field values,
        to specified file path (CSV)
        :param product_name: name of the product
        :param as_of_timestamp: timestamp (Unix epoch, milliseconds)
        :param report_file_path: path/name of the file to which to write
            results
        :return:
        """
        results = self.get(
            product_name, as_of_timestamp)

        write_report_results(results, report_file_path)
