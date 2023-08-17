from socotra_datamart_reports.lib import queries_extract as queries, get_flattened_fields
from socotra_datamart_reports.lib.base_report import BaseReport
import pandas as pd


class ClaimsCreatedReport(BaseReport):
    """
    Fetches all claims and associates fields
    """
    visible = True
    name = "claims_created"
    argument_specs = [
        {
            "name": "product_name",
            "required": False,
            "default": False
        },
        {
            "name": "start_timestamp",
            "required": True,
            "default": True
        }
    ]
    record_specs = [
        {
            "name": "claim_locator",
            "type": "string",
            "index": True
        },
        {
            "name": "policy_locator",
            "type": "string"
        },
        {
            "name": "created",
            "type": "datetime"
        },
        {
            "name": "product_name",
            "type": "string"
        }
    ]

    def __init__(self, creds, arguments):
        super().__init__(creds, self.name, arguments)
        self.data = []
        self.records = []

        self.column_settings = [
            # Not all columns require settings
            {
                "name": "policy_start_time"
            }
        ]

    def prepare(self):
        self.arguments["start_timestamp"] = int(
            self.arguments["start_timestamp"]
        )
        self.arguments["product_statement"] = ''
        if self.arguments["product_name"] is not False:
            self.arguments[
                "product_statement"
            ] = f'AND claim.product_name = "{self.arguments["product_name"]}"'

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
        self.massage_to_spec()
        return self.records
