from socotra_datamart_reports.lib import queries, get_flattened_fields
from socotra_datamart_reports.lib.base_report import\
    BaseReport, write_report_results


class AllPoliciesReport(BaseReport):
    """
    Fetches all-policies results from Data Mart, with ability to write a
    report with "flattened" fields. "Flattened" fields are denormalized; since
    each row of the report corresponds to a single peril characteristic,
    we show relevant fields by putting each one in its own labeled column,
    for all three levels (Policy, Exposure, Peril). Repeated fields or
    field groups receive numerical qualifiers.
    """

    def __init__(self, creds):
        super().__init__(creds)

    def fetch_all_policies_data(
            self, product_name: str, start_timestamp: int, end_timestamp: int):
        """
        Fetches raw all-policies data, with field values aggregated into JSON
        at each of the peril, exposure, and policy levels
        :param product_name: name of the product
        :param start_timestamp: timestamp (Unix epoch, milliseconds)
        :param end_timestamp: timestamp (Unix epoch, milliseconds)
        :return:
        """
        query = queries.get_all_policies_query(
            product_name, start_timestamp, end_timestamp)
        return self.fetch_all_results_for_query(query)

    def get_all_policies_report_with_flattened_fields(
            self, product_name: str, start_timestamp: int, end_timestamp: int):
        """
        Returns result set with field values "flattened", i.e., made into
        columns in the row, with numerical designators to disambiguate repeated
        fields
        :param product_name: name of the product
        :param start_timestamp: timestamp (Unix epoch, milliseconds)
        :param end_timestamp: timestamp (Unix epoch, milliseconds)
        :return:
        """
        return get_flattened_fields.get_flattened_results(
            self.fetch_all_policies_data(
                product_name, start_timestamp, end_timestamp))

    def write_all_policies_report(
            self, product_name: str, start_timestamp: str,
            end_timestamp: str, report_file_path):
        """
        Writes an all-policies report set, including flattened field values,
        to specified file path (CSV)
        :param product_name: name of the product
        :param start_timestamp: timestamp (Unix epoch, milliseconds)
        :param end_timestamp: timestamp (Unix epoch, milliseconds)
        :param report_file_path: path/name of the file to which to write
                results
        :return:
        """
        results = self.get_all_policies_report_with_flattened_fields(
            product_name, start_timestamp, end_timestamp)
        write_report_results(results, report_file_path)
