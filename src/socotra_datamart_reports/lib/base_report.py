import os
import mysql.connector
import pandas as pd
from pathlib import Path
from .utils import timezone_code_from_name


class BaseReport:
    """
    Base report class responsible for establishing db connection and fetching
    raw results. Also knows how to write a given set of results
    (list of dicts) to a given filepath.
    """
    visible = False

    def __init__(self, creds, name, arguments={}):
        self.name = name
        self.creds = creds
        self.arguments = arguments if arguments else {}
        self.argument_specs = self.argument_specs if self.argument_specs else {}
        self.record_specs = self.record_specs if self.record_specs else []
        self.datamart = self.datamart_connect()
        self.set_timezone()
        self.assign_arguments()
        self.prepare()
        # Set directory and file
        self.report_filename = self.arguments["report_filename"] if "report_filename" in self.arguments else self.name
        self.data_directory = self.arguments["report_path"] if "report_path" in self.arguments else ""

    def assign_arguments(self, arguments=False):
        if arguments:
            self.arguments = arguments
        for spec in self.argument_specs:
            if spec["name"] in self.arguments:
                self.arguments[spec["name"]] = self.arguments[spec["name"]]
            elif "default" in spec:
                self.arguments[spec["name"]] = spec["default"]
            else:
                print(
                    f'Required parameter \"{spec["name"]}\" for report \"{self["name"]}\" is not found')

    def set_timezone(self, timezone=False):
        timezone = timezone if timezone else self.creds.get('timezone')
        self.timezone_name = timezone
        self.timezone_code = timezone_code_from_name(timezone)
        query = "SET time_zone='{zstring}';".format(zstring=timezone)
        cursor = self.datamart.cursor(dictionary=True)
        cursor.execute(query)
        self.log("set DataMart timezone to {zstring}".format(zstring=timezone))

    def log(self, message, level="NOTICE"):
        level = level.upper()
        print(
            "{level} [{name}]: {message}".format(
                level=level,
                name=self.name,
                message=message
            ))
        if level == "ERROR":
            raise Exception(
                f'{self.name} failed to process due to last logged error.'
            )

    def prepare(self):
        pass

    def generate(self):
        pd.real_sql_query()

    def format_to_extension(self, format):
        format = format.lower()
        return f'.{format}'

    def write(self, format=['parquet']):
        for fmat in format:
            self.write_report_records(
                os.path.join(
                    self.data_directory,
                    self.report_filename+self.format_to_extension(fmat),
                ),
                format=fmat
            )

    def load_prerequisites(self, data_directory=False, format=["parquet"]):
        data_directory = self.data_directory if data_directory == False else data_directory
        for item in self.prerequisites:
            filename = f'{item}{self.format_to_extension(format)}'
            report_path = Path(os.path.join(data_directory, filename))
            if report_path.is_file():
                if format == "parquet":
                    self.sources[item] = pd.read_parquet(
                        report_path, engine='auto'
                    )
            else:
                self.log(
                    f'data source {item} unavailable at {report_path}',
                    "ERROR"
                )

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
        for spec in self.record_specs:
            if spec["name"] in self.records.columns:
                if "index" in spec and spec["index"] == True:
                    self.records = self.records.set_index(spec["name"])
                    self.log("set index to {field}".format(field=spec["name"]))
                if spec["type"] == "datetime":
                    # self.records[spec["name"]] = self.records[spec["name"]].astype(
                    #     'datetime64[ns]')
                    self.records[spec["name"]] = pd.to_datetime(
                        self.records[spec["name"]], unit='ms', utc=True).dt.tz_convert(self.timezone_name)
                if spec["type"] == "float":
                    self.records[spec["name"]] = self.records[spec["name"]].astype(
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
        fields_to_omit=['policy_fields', 'exposure_fields', 'peril_fields'],
        format=['parquet']
    ):
        for field in fields_to_omit:
            if field in self.records.columns:
                self.records = self.records.drop(field, axis=1)
        if isinstance(format, str):
            format = [format]
        for ftype in format:
            if ftype == "csv":
                self.records.to_csv(Path(report_file_path).with_suffix('.csv'))
                self.log("output .csv format")
            if ftype == "parquet":
                # table = pa.Table.from_pandas(self.records)
                # pq.write_table(table, Path(report_file_path).with_suffix(
                #     '.parquet'))
                self.records.to_parquet(Path(report_file_path).with_suffix(
                    '.parquet'), index=None, engine="auto")
                self.log("output .parquet format")
