import importlib

from . import rypipe_adapter  # noqa: F401

__all__ = [
    "PropertiesSource",
    "PropertiesAdapter",
    "CastTypes",
    "FilterRows",
    "RenameFields",
    "DropFields",
    "collect",
    "to_arrow",
    "to_pandas",
    "to_polars",
    "to_parquet",
    "to_csv",
]

_modules = {
    "PropertiesSource": ".source",
    "PropertiesAdapter": ".rypipe_adapter",
    "CastTypes": ".stages",
    "FilterRows": ".stages",
    "RenameFields": ".stages",
    "DropFields": ".stages",
    "collect": ".sinks",
    "to_arrow": ".sinks",
    "to_pandas": ".sinks",
    "to_polars": ".sinks",
    "to_parquet": ".sinks",
    "to_csv": ".sinks",
}


def __getattr__(name):
    if name in _modules:
        mod = importlib.import_module(_modules[name], __package__)
        return getattr(mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return __all__
