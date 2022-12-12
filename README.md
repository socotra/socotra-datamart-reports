# Data Mart Reporting

A collection of sample queries and ready-to-run scripts for common Data Mart reporting needs.

## Sample Queries

The `sample_queries` directory features a selection of basic queries to help you familiarize yourself with Data Mart.

## Reports

`reports` features a set of reports that can be run against a Data Mart instance. 
Those reports prefaced with `platform_` have analogues in the Socotra platform.

### Running a demo

Initialize a virtual environment, install dependencies (`requirements.txt`), set your credentials in a `.env` 
file (root dir, as a peer of `main.py`) with the following contents:

```
REPORT_USER="your_datamart_username"
REPORT_PASSWORD="your_datamart_password"
REPORT_HOST="your_datamart_host"
REPORT_PORT="your_datamart_port"
REPORT_DATABASE="your_datamart_database"
```

and then run `main.py`