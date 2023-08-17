import re
import inspect
import sys
import datetime
import os

import socotra_datamart_reports as reports
from dateutil import tz
from dotenv import load_dotenv

from sync_interface import AppUI
from sync_interface import PrimaryWindow

load_dotenv()


def report_range(startDate, endDate):
    start_date_obj = datetime.datetime.strptime(startDate, '%m/%d/%Y')
    if (endDate):
        end_date_obj = datetime.datetime.strptime(endDate, '%m/%d/%Y')
        # Incremement by one day
        end_date_obj += datetime.timedelta(days=1)
    else:
        end_date_obj = datetime.datetime.now()
    return [
        datetime.datetime.strftime(start_date_obj, "%m%d%y"),
        start_date_obj.timestamp() * 1000,
        ((end_date_obj.timestamp() * 1000) - 1)
    ]


# Define standard params needed for reports

creds = {
    'host': os.getenv('DATAMART_HOST'),
    'port': os.getenv('DATAMART_PORT'),
    'database': os.getenv('DATAMART_DATABASE'),
    'user': os.getenv('DATAMART_USER'),
    'password': os.getenv('DATAMART_PASSWORD'),
    'timezone': os.getenv('TIMEZONE')
}

[date_string, report_start_timestamp, report_end_timestamp] = report_range(
    "01/01/2023", "06/29/2023"
)

# Implement UI Code Here

# Implement Report Runner Code Here

report_params = {
    "start_timestamp": report_start_timestamp,
    "end_timestamp": report_end_timestamp,
    "as_of_timestamp": report_start_timestamp,
    "report_path": "data"
}
report_core = reports.Engine(creds, report_params, "data")
report_list = report_core.report_list()
print(report_list)
for report in report_list:
    report_core.run(report)
