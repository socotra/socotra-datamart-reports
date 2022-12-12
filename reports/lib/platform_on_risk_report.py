import csv
import mysql.connector
import reports.lib.common.queries
import reports.lib.common.get_flattened_fields


class OnRiskReport:
    """
    Fetches on-risk results from Data Mart, with ability to write a report with "flattened" fields.
    "Flattened" fields are denormalized; since each row of the report corresponds to a single peril characteristic,
    we show relevant fields by putting each one in its own labeled column,
    for all three levels (Policy, Exposure, Peril). Repeated fields or field groups receive numerical qualifiers.
    """
    def __init__(self, creds):
        self.creds = creds

    def _fetch_all_results_for_query(self, query):
        connection_port = self.creds.get('port')
        if connection_port is None:
            connection_port = 3306
        cnx = mysql.connector.connect(user=self.creds.get('user'),
                                      password=self.creds.get('password'),
                                      host=self.creds.get('host'),
                                      database=self.creds.get('database'),
                                      port=connection_port)
        cursor = cnx.cursor(dictionary=True)
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        cnx.close()
        return results

    def fetch_on_risk_data(self, product_name, as_of_timestamp):
        query = reports.lib.common.queries.get_on_risk_query(product_name, as_of_timestamp)
        results = self._fetch_all_results_for_query(query)
        return results

    def write_on_risk_report(self, product_name, as_of_timestamp, report_file_path):
        results = reports.lib.common.get_flattened_fields.get_flattened_results(
            self.fetch_on_risk_data(product_name, as_of_timestamp))

        # since we're flattening all fields, we will omit the columns with raw field data
        fields_to_omit = ['policy_fields', 'exposure_fields', 'peril_fields']
        fields_to_write = [f for f in results[0].keys() if f not in fields_to_omit]

        with open(report_file_path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fields_to_write, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(results)
