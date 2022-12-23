import csv
import mysql.connector


def write_report_results(
        results, report_file_path,
        fields_to_omit=['policy_fields', 'exposure_fields', 'peril_fields']):
    fields_to_write = []
    if len(results) > 0:
        fields_to_write = [
            f for f in results[0].keys() if f not in fields_to_omit]
    with open(report_file_path, 'w') as csv_file:
        writer = csv.DictWriter(
            csv_file, fields_to_write, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)


class BaseReport:
    """
    Base report class responsible for establishing db connection and fetching
    raw results. Also knows how to write a given set of results
    (list of dicts) to a given filepath.
    """
    def __init__(self, creds):
        self.creds = creds

    def fetch_all_results_for_query(self, query: str):
        connection_port = self.creds.get('port')
        if connection_port is None:
            connection_port = 3306
        cnx = mysql.connector.connect(user=self.creds.get('user'),
                                      password=self.creds.get('password'),
                                      host=self.creds.get('host'),
                                      database=self.creds.get('database'),
                                      port=connection_port)
        # TODO with context here, error handling
        cursor = cnx.cursor(dictionary=True)
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        cnx.close()
        return results
