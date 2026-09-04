# Python Adapter Wiring { #python-wiring }

This page explains how to wire your Rust adapter to Python — the Source
subclass, adapter class, registration, and repacked stages.

## The crxml formula { #the-crxml-formula }

The reference adapter (**crxml**) defines the standard pattern. Every adapter
follows this structure:

```python
# Users import everything from the adapter package { #users-import-everything-from-the-adapter-package }
from my_adapter import MySource, CastTypes, FilterRows
```

### Directory layout { #directory-layout }

```
my_adapter/
├── __init__.py            # re-exports, lazy loading
├── rypipe_adapter.py      # MyAdapter + registration
├── source.py              # MySource(Source)
└── stages/
    ├── __init__.py        # lazy re-exports
    ├── cast.py            # CastTypes
    ├── filter.py          # FilterRows
    ├── rename.py          # RenameFields
    └── drop.py            # DropFields
```

## Source subclass { #source-subclass }

The Source subclass is the pipeline-capable entry point. It implements
`_read_arrow()` and forwards plan kwargs from fused stages:

```python
# my_adapter/source.py { #my_adaptersourcepy }
from __future__ import annotations
from typing import Any

import _rypipe_myfmt
from rypipe import Source


class MySource(Source):
    """Pipeline-capable source for MyFormat files."""

    def _read_arrow(self, plan_overrides: dict[str, Any] | None = None) -> Any:
        # Start with construction-time kwargs (field_mapping, drop_fields, etc.)
        plan = self._build_plan_kwargs()
        # Fused pipeline stages override construction-time kwargs
        if plan_overrides:
            plan.update(plan_overrides)
        # Pass the merged plan to the Rust reader
        return _rypipe_myfmt.read(str(self._path), **plan)
```

### How _read_arrow works { #how-read-arrow-works }

When a user writes `src | RenameFields(...) | FilterRows(...)`, the pipeline
collects stages into a plan. When `.to_arrow()` is called, the pipeline calls
`_read_arrow(plan_overrides=...)` on your source.

`plan_overrides` contains the fused stage kwargs:

```python
{
    "field_mapping": {"old_name": "new_name"},
    "drop_fields": ["internal_id"],
    "filter": {"field": "status", "op": "==", "value": "active"},
    "field_types": {"amount": "float64"},
}
```

You must merge these with your construction kwargs and pass them to your
Rust reader. If you ignore `plan_overrides`, fused stages silently fall back
to Python execution — 10–50× slower.

/// warning

Never ignore `plan_overrides`. Fused stages silently fall back to Python
execution over a full table when plan kwargs are not forwarded, turning a
microsecond Rust path into a millisecond Python loop.

///

## Adapter class { #adapter-class }

The adapter is a thin, stateless wrapper. It delegates to the Source for
actual parsing:

```python
# my_adapter/rypipe_adapter.py { #my_adapterrypipe_adapterpy }
from __future__ import annotations
from typing import Any

from .source import MySource


class MyAdapter:
    """rypipe-compatible adapter for MyFormat files."""

    def read(self, path: str, **kwargs: Any) -> Any:
        """Parse ``path`` and return a ``pyarrow.Table``."""
        return MySource(path, **kwargs).to_arrow()

    def iter_record_batches(
        self, path: str, memory: str | int = "64MiB",
        batch_size: int | None = None, **kwargs: Any,
    ):
        """Yield ``pyarrow.RecordBatch`` objects with constant memory."""
        yield from MySource(path, **kwargs).iter_record_batches(
            memory=memory, batch_size=batch_size
        )
```

/// note

The adapter's `read()` method returns a `pyarrow.Table`, not a Source.
This is by design — `rypipe.read()` calls `adapter.read()` and expects a
table. Users who want pipelines use the Source directly.

///

## Registration { #registration }

Register the adapter at import time. Users get the adapter by importing
your package:

```python
# my_adapter/rypipe_adapter.py (continued) { #my_adapterrypipe_adapterpy }

def _register() -> None:
    try:
        import rypipe
    except Exception:  # pragma: no cover — rypipe is optional
        return
    rypipe.register_adapter("myfmt", MyAdapter(), extensions=[".myfmt"])


_register()  # runs on import
```

### __init__.py { #init-py }

The `__init__.py` triggers registration and lazily loads public names:

```python
# my_adapter/__init__.py { #my_adapter__init__py }
import importlib

# Side-effect import: registers the adapter with rypipe on import { #side-effect-import-registers-the-adapter-with-rypipe-on-import }
from . import rypipe_adapter  # noqa: F401

__all__ = [
    "MySource",
    "MyAdapter",
    "CastTypes",
    "FilterRows",
    "RenameFields",
    "DropFields",
]

_modules = {
    "MySource": ".source",
    "CastTypes": ".stages",
    "FilterRows": ".stages",
    "RenameFields": ".stages",
    "DropFields": ".stages",
}


def __getattr__(name):
    if name in _modules:
        mod = importlib.import_module(_modules[name], __package__)
        return getattr(mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return __all__
```

After registration:

* `rypipe.read("file.myfmt")` auto-detects the `.myfmt` extension.
* `rypipe.read("file.myfmt", format="myfmt")` works explicitly.
* `rypipe.read("file.txt", format="myfmt")` works with explicit format.

## Repacked stages { #repacked-stages }

Adapters include their own copies of the pipeline stage classes. This
makes the adapter self-contained — users never import from **rypipe**.

The stage implementations are identical to **rypipe**'s. See the
[Quick Start](./quickstart.md) for full code, or copy from
`rypipe/rypipe/stages/`. Each stage has three methods:

* `apply(record)` — transform a single dict (fused path).
* `__call__(stream)` — transform an iterable of dicts (unfused path).
* `_plan_kwargs()` — return pushdown kwargs for the Rust engine.

### CastTypes { #casttypes }

```python
from typing import Callable

_PY_TO_RUST_TYPE = {
    int: "int64",
    float: "float64",
    str: None,
    bool: "bool",
}


class CastTypes:
    __slots__ = ("_mapping",)

    def __init__(self, mapping: dict[str, Callable]):
        self._mapping = mapping

    def apply(self, record: dict) -> dict:
        for field, cast_fn in self._mapping.items():
            try:
                record[field] = cast_fn(record[field])
            except KeyError:
                pass  # field not in this row — skip silently
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"CastTypes: cannot cast field '{field}' "
                    f"value {record[field]!r}: {e}"
                ) from e
        return record

    def __call__(self, stream):
        return map(self.apply, stream)

    def _plan_kwargs(self) -> dict | None:
        ft = {}
        for field, fn in self._mapping.items():
            rust_type = _PY_TO_RUST_TYPE.get(fn)
            if rust_type is None:
                if fn is str:
                    continue  # str cast is a no-op
                return None  # unsupported type — can't fuse
            ft[field] = rust_type
        return {"field_types": ft} if ft else None
```

### FilterRows { #filterrows }

```python
class FilterRows:
    __slots__ = ("_predicate", "_filter_spec")

    def __init__(self, predicate=None, *, field=None, op=None, value=None,
                 field_a=None, field_b=None):
        if predicate is not None:
            self._predicate = predicate
            self._filter_spec = None
        elif field is not None and op is not None and value is not None:
            self._filter_spec = {"field": field, "op": op, "value": value}
            self._predicate = lambda r: (
                r.get(field) == value if op in ("==", "eq")
                else r.get(field) != value
            )
        elif field_a is not None and op is not None and field_b is not None:
            self._filter_spec = {"field_a": field_a, "op": op, "field_b": field_b}
            ops = {
                ">": lambda a, b: a > b, "<": lambda a, b: a < b,
                ">=": lambda a, b: a >= b, "<=": lambda a, b: a <= b,
                "==": lambda a, b: a == b, "!=": lambda a, b: a != b,
            }
            fn = ops[op]
            self._predicate = lambda r: bool(fn(r.get(field_a), r.get(field_b)))
        else:
            raise ValueError(
                "FilterRows requires a callable predicate or "
                "keyword arguments (field+op+value or field_a+op+field_b)"
            )

    def apply(self, record: dict) -> dict | None:
        return record if self._predicate(record) else None

    def __call__(self, stream):
        return (r for r in map(self.apply, stream) if r is not None)

    def _plan_kwargs(self) -> dict | None:
        return {"filter": self._filter_spec} if self._filter_spec else None
```

/// tip

If your format is text-only and has no numeric fields, the `str` cast is a
no-op — `_plan_kwargs` returns `None` and the engine skips fusing it. No
harm done, but you can skip the `CastTypes` stage entirely.

///

### RenameFields and DropFields { #rename-and-drop }

```python
class RenameFields:
    __slots__ = ("_mapping",)

    def __init__(self, mapping: dict[str, str]):
        self._mapping = mapping

    def apply(self, record: dict) -> dict:
        return {self._mapping.get(k, k): v for k, v in record.items()}

    def __call__(self, stream):
        return map(self.apply, stream)

    def _plan_kwargs(self) -> dict | None:
        return {"field_mapping": self._mapping}


class DropFields:
    __slots__ = ("_fields_set",)

    def __init__(self, fields: list[str]):
        if isinstance(fields, str):
            raise TypeError(
                f"DropFields expects a list, got a string; use DropFields([{fields!r}])"
            )
        self._fields_set = frozenset(fields)

    def apply(self, record: dict) -> dict:
        return {k: v for k, v in record.items() if k not in self._fields_set}

    def __call__(self, stream):
        return map(self.apply, stream)

    def _plan_kwargs(self) -> dict | None:
        return {"drop_fields": sorted(self._fields_set)}
```

/// warning

`DropFields` expects a `list[str]`, not a bare string. Passing a string
raises a `TypeError` with a helpful message — but this is a common mistake
when migrating from other libraries.

///

## Streaming { #streaming }

For bounded-memory streaming, override `iter_record_batches` on your Source:

```python
class MySource(Source):
    def _read_arrow(self, plan_overrides=None):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return _rypipe_myfmt.read(str(self._path), **plan)

    def iter_record_batches(self, memory="64MiB", batch_size=None, **kwargs):
        plan = self._build_plan_kwargs()
        return _rypipe_myfmt.iter_batches(
            str(self._path), memory=memory, batch_size=batch_size, **plan
        )
```

Users can then process large files:

```python
from my_adapter import MySource

src = MySource("huge_file.myfmt")
for batch in src.iter_record_batches(memory="256MiB"):
    process(batch)
```

## Recap { #recap }

* **Source** — pipeline-capable, implements `_read_arrow()` with plan
  forwarding.
* **Adapter** — thin wrapper, `read()` delegates to `Source(...).to_arrow()`.
* **Stages** — repacked copies of `CastTypes`, `FilterRows`, etc.
* **Registration** — adapter registered at import time via side-effect import.
* Users import everything from the adapter package, never from **rypipe**.
