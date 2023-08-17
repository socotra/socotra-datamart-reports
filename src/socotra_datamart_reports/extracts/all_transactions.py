from socotra_datamart_reports.lib import queries_extract as queries, get_flattened_fields
from socotra_datamart_reports.lib.base_report import BaseReport
import pandas as pd


class AllTransactionsReport(BaseReport):
    """
    Fetches all-policies results from Data Mart, with ability to write a
    report with "flattened" fields. "Flattened" fields are denormalized; since
    each row of the report corresponds to a single peril characteristic,
    we show relevant fields by putting each one in its own labeled column,
    for all three levels (Policy, Exposure, Peril). Repeated fields or
    field groups receive numerical qualifiers.
    """
    visible = True

    def __init__(self, creds):
        super().__init__(creds)
        self.name = "all_transactions"
        self.data = []
        self.records = []
        self.argument_specs = [
            {
                "name": "product_name",
                "required": False,
                "default": False
            },
            {
                "name": "start_timestamp",
                "required": True,
                "default": True
            },
            {
                "name": "end_timestamp",
                "required": True,
                "default": True
            }
        ]
        self.record_specs = [
            {
                "name": "coverage_start",
                "type": "datetime"
            },
            {
                "name": "coverage_end",
                "type": "datetime"
            },
            {
                "name": "premium",
                "type": "float"
            },
        ]

        self.column_settings = [
            # Not all columns require settings
            {
                "name": "policy_start_time"
            }
        ]

    def prepare(self):
        self.arguments["start_timestamp"] = int(
            self.arguments["start_timestamp"])
        self.arguments["end_timestamp"] = int(self.arguments["end_timestamp"])
        self.arguments["product_statement"] = f' '
        if self.arguments["product_name"] != False:
            self.arguments["product_statement"] = f'policy.product_name = "{self.arguments["product_name"]}" AND '

    def fetch(self):
        import pathlib
        path = pathlib.Path(__file__).parent.resolve()
        query = ''
        with open(f"{path}/{self.name}.sql", 'r') as file:
            query = file.read()
        self.data = self.fetch_all_results_for_query(query, self.arguments)

    def build(self):
        results = get_flattened_fields.get_flattened_results(
            self.data,
            "extract"
        )
        self.records = pd.DataFrame(results)
        return self.records

    def write(self):
        self.write_report_records(
            self.arguments["report_file_path"], fields_to_omit=[])
