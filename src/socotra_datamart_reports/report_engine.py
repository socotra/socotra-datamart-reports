from collections import OrderedDict


def find_submodules(local, filter_hidden):
    import glob
    import importlib
    import inspect
    import os

    submodules = []
    current_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    current_module_name = ".".join([
        os.path.splitext(
            os.path.basename(current_dir)
        )[0], local
    ])
    for file in glob.glob("/".join([current_dir, local, "*.py"])):
        name = os.path.splitext(os.path.basename(file))[0]

        # Ignore __ files
        if name.startswith("__"):
            continue
        module = importlib.import_module(
            "." + name, package=current_module_name)

        for member in dir(module):
            handler_class = getattr(module, member)

            if handler_class and inspect.isclass(handler_class):
                if handler_class.visible or not filter_hidden:
                    submodules.append(handler_class)

    return submodules


class ReportEngine:
    def __init__(self, creds, arguments, base_directory="."):
        self.creds = creds
        self.base_directory = base_directory
        self.arguments = arguments

    def report_list(self, filter_hidden=True):
        return find_submodules("extracts", filter_hidden)

    def run(self, report, format=['parquet', 'csv']):
        report_instance = report(self.creds, self.arguments)
        print(f"Processing: {report_instance.name}")
        # report_arguments = OrderedDict()
        # for spec in report_instance.argument_specs:
        #     if spec["name"] in self.arguments:
        #         report_arguments[spec["name"]] = self.arguments[spec["name"]]
        #     elif "default" in spec:
        #         report_arguments[spec["name"]] = spec["default"]
        #     else:
        #         print(
        #             f'Required parameter \"{spec["name"]}\" for report \"{self["name"]}\" is not found')

        # report_arguments["report_path"] = self.base_directory
        # report_arguments["report_filename"] = report_instance.name
        # report_instance.arguments = report_arguments
        report_instance.fetch()
        report_instance.build()
        report_instance.write(format=format)
        report_instance.close()
