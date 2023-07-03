from collections import OrderedDict

class ReportEngine:
  def __init__(self, creds, arguments, base_directory="."):
    self.creds = creds
    self.base_directory = base_directory
    self.arguments = arguments

  def run(self, report):
    reportInstance = report(self.creds)
    print(f"Processing: {reportInstance.name}")
    report_arguments = OrderedDict()
    for spec in reportInstance.argument_specs:
      if spec["name"] in self.arguments:
        report_arguments[spec["name"]] = self.arguments[spec["name"]]
      elif "default" in spec:
        report_arguments[spec["name"]] = spec["default"]
      else:
        print(f'Required parameter \"{spec["name"]}\" for report \"{self["name"]}\" is not found')

    report_arguments["report_file_path"] = f'{self.base_directory}/{reportInstance.name}.csv'

    reportInstance.write(**report_arguments)
