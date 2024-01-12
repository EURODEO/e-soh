import importlib
import glob

from base_formatter import EDR_formatter


def get_EDR_formatters() -> dict:
    """
    This method should grab all available formatters and make them reachable in a dict
    This way we can dynamicly grab all available formats and skip configuring this.
    Should aliases be made available, and how do one make formatters present in openapi doc?
    """
    # Get all .py files in local folder, ignore files that start with _
    formatter_files = [i.strip(".py") for i in glob.glob("[!_]*.py")]

    available_formatters = {}

    formatters = [importlib.import_module(i) for i in formatter_files]

    for formatter in formatters:
        if hasattr(formatter, "formatter_name"):
            obj = getattr(formatter, formatter.formatter_name)
            if issubclass(obj, EDR_formatter):
                # Make instance of formatter and save
                available_formatters[formatter.name] = formatter()

    return available_formatters
