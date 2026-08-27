import importlib

__all__ = [
    "RenameFields",
    "CastTypes",
    "FilterRows",
    "FilterRowsAny",
    "FilterRowsAll",
    "FilterRowsNot",
    "DropFields",
]

_modules = {
    "RenameFields": ".rename",
    "CastTypes": ".cast",
    "FilterRows": ".filter",
    "FilterRowsAny": ".filter",
    "FilterRowsAll": ".filter",
    "FilterRowsNot": ".filter",
    "DropFields": ".drop",
}


def __getattr__(name):
    if name in _modules:
        mod = importlib.import_module(_modules[name], __package__)
        return getattr(mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return __all__
