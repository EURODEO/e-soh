from pydantic import BaseModel, validator
import importlib
import pkgutil

import formatters


def get_EDR_formatters() -> dict:
    """
    This method should grab all available formatters and make them reachable in a dict
    This way we can dynamicly grab all available formats and skip configuring this.
    Should aliases be made available, and how do one make formatters present in openapi doc?
    """
    available_formatters = {}

    formatter_plugins = [importlib.import_module("formatters."+i.name) for i in pkgutil.iter_modules(
        formatters.__path__) if i.name != "base_formatter"]
    print(formatter_plugins)
    for formatter_module in formatter_plugins:
        # Make instance of formatter and save
        available_formatters[formatter_module.__name__.split(".")[-1]] = getattr(
            formatter_module, formatter_module.formatter_name)

    # Should also setup dict for alias discovery
    class edr_formatters_model(BaseModel):
        f: str

        @validator("f")
        def f_must_be_available_formatter(cls, f):
            named_formatters = list(available_formatters.keys())
            if f not in named_formatters:
                raise ValueError(f"f must be a provided formatter, one of {named_formatters}")
            return f

    return available_formatters, edr_formatters_model
