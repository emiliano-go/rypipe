import importlib

__all__ = ["RenameFields", "CastTypes", "FilterRows", "DropFields"]

_modules = {
    "RenameFields": ".rename",
    "CastTypes": ".cast",
    "FilterRows": ".filter",
    "DropFields": ".drop",
}


def __getattr__(name):
    if name in _modules:
        mod = importlib.import_module(_modules[name], __package__)
        return getattr(mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return __all__
