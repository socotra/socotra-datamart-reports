import csv
import mysql.connector
import pandas as pd
from pathlib import Path


class BaseReport:
    """
    Base report class responsible for establishing db connection and fetching
    raw results. Also knows how to write a given set of results
    (list of dicts) to a given filepath.
    """
    visible = False

    def __init__(self, creds):
        self.name = "base_report"
        self.creds = creds
        self.datamart = self.datamart_connect()
        self.record_spec = []

    def set_timezone(self, timezone=False):
        timezone = timezone if timezone else self.creds.get('timezone')
        query = "SET time_zone='{zstring}';".format(zstring=timezone)
        cursor = self.datamart.cursor(dictionary=True)
        cursor.execute(query)
        self.log("set DataMart timezone to {zstring}".format(zstring=timezone))

    def log(self, message, level="NOTICE"):
        print(
            "{level} [{name}]: {message}".format(
                level=level,
                name=self.name,
                message=message
            ))

    def prepare(self):
        pass

    def generate(self):
        pd.real_sql_query()

    def write(self):
        pass

    def datamart_connect(self):
        port = self.creds.get('port')
        if port is None:
            port = 3306
        connection = mysql.connector.connect(
            user=self.creds.get('user'),
            password=self.creds.get('password'),
            host=self.creds.get('host'),
            database=self.creds.get('database'),
            port=port
        )
        return connection

    def close(self):
        if self.datamart.close():
            return True
        else:
            return False

    def massage_to_spec(self):
        for spec in self.record_spec:
            if spec.type == "datetime":
                self.records[spec.name] = self.records[spec.name].astype(
                    'datetime64[ns]')
            if spec.type == "float":
                self.records[spec.name] = self.records[spec.name].astype(
                    'float64')
        self.log("massaged data types")

    def fetch_all_results_for_query(self, query: str, params: dict):
        # TODO with context here, error handling
        cursor = self.datamart.cursor(dictionary=True)
        query = query.format(**params)
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results

    def write_report_records(
        self, report_file_path,
        fields_to_omit=['policy_fields', 'exposure_fields', 'peril_fields']
    ):
        for field in fields_to_omit:
            if field in self.records.columns:
                self.records.drop(field, axis=1)
        self.records.to_parquet(report_file_path, index=True, engine="pyarrow")
        self.records.to_csv(Path(report_file_path).with_suffix('.csv'))
