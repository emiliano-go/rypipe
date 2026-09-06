# Python Adapter Wiring { #python-wiring }

This page explains how to wire your Rust adapter to Python: the Source
subclass, adapter class, registration, and repacked stages.

## The crxml formula { #the-crxml-formula }

The reference adapter ([**crxml**](../crxml-adapter.md)) defines the standard pattern. Every adapter
follows this structure:

```python
# Users import everything from the adapter package { #users-import-everything-from-the-adapter-package }
from rypipe_log import LogSource, CastTypes, FilterRows
```

### Directory layout { #directory-layout }

```
rypipe_log/
├── __init__.py            # re-exports, lazy loading
├── rypipe_adapter.py      # LogAdapter + registration
├── source.py              # LogSource(Source)
├── sinks.py               # collect, to_pandas, to_csv (repacked)
└── stages/
    ├── __init__.py        # lazy re-exports
    ├── cast.py            # CastTypes
    ├── filter.py          # FilterRows
    ├── rename.py          # RenameFields
    └── drop.py            # DropFields
```

!!! important

    **Users only import from your adapter.** They write
    `from rypipe_log import CastTypes, FilterRows`: never
    `from rypipe import CastTypes`. This is the **crxml formula**:
    adapters repack the full pipeline API so end users never depend on
    **rypipe** directly.


## Source subclass { #source-subclass }

The Source subclass is the pipeline-capable entry point. It implements
`_read_arrow()` and forwards plan kwargs from fused stages:

```python
# rypipe_log/source.py { #rypipe_logsourcepy }
from __future__ import annotations
from typing import Any

import _rypipe_log
from rypipe import Source


class LogSource(Source):
    """Pipeline-capable source for newline-delimited key=value logs."""

    def _read_arrow(self, plan_overrides: dict[str, Any] | None = None) -> Any:
        # Start with construction-time kwargs (field_mapping, drop_fields, etc.)
        plan = self._build_plan_kwargs()
        # Fused pipeline stages override construction-time kwargs
        if plan_overrides:
            plan.update(plan_overrides)
        # Pass the merged plan to the Rust reader
        return _rypipe_log.read(str(self._path), **plan)
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
to Python execution: 10–50× slower.

!!! warning

    Never ignore `plan_overrides`. Fused stages silently fall back to Python
    execution over a full table when plan kwargs are not forwarded, turning a
    microsecond Rust path into a millisecond Python loop.


## Adapter class { #adapter-class }

The adapter is a thin, stateless wrapper. It delegates to the Source for
actual parsing:

```python
# rypipe_log/rypipe_adapter.py { #rypipe_logrypipe_adapterpy }
from __future__ import annotations
from typing import Any

from .source import LogSource


class LogAdapter:
    """rypipe-compatible adapter for newline-delimited key=value logs."""

    def read(self, path: str, **kwargs: Any) -> Any:
        """Parse ``path`` and return a ``pyarrow.Table``."""
        return LogSource(path, **kwargs).to_arrow()

    def iter_record_batches(
        self, path: str, memory: str | int = "64MiB",
        batch_size: int | None = None, **kwargs: Any,
    ):
        """Yield ``pyarrow.RecordBatch`` objects with constant memory."""
        yield from LogSource(path, **kwargs).iter_record_batches(
            memory=memory, batch_size=batch_size
        )
```

!!! note

    The adapter's `read()` method returns a `pyarrow.Table`, not a Source.
    This is by design: `rypipe.read()` calls `adapter.read()` and expects a
    table. Users who want pipelines use the Source directly.


## Registration { #registration }

Register the adapter at import time. Users get the adapter by importing
your package:

```python
# rypipe_log/rypipe_adapter.py (continued) { #rypipe_logrypipe_adapterpy }

def _register() -> None:
    try:
        import rypipe
    except Exception:  # pragma: no cover: rypipe is optional
        return
    rypipe.register_adapter("log", LogAdapter(), extensions=[".log"])


_register()  # runs on import
```

### __init__.py { #init-py }

The `__init__.py` triggers registration and lazily loads public names:

```python
# rypipe_log/__init__.py { #rypipe_log__init__py }
import importlib

# Side-effect import: registers the adapter with rypipe on import { #side-effect-import-registers-the-adapter-with-rypipe-on-import }
from . import rypipe_adapter  # noqa: F401

__all__ = [
    "LogSource",
    "LogAdapter",
    "CastTypes",
    "FilterRows",
    "RenameFields",
    "DropFields",
]

_modules = {
    "LogSource": ".source",
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

* `rypipe.read("data.log")` auto-detects the `.log` extension.
* `rypipe.read("data.log", format="log")` works explicitly.
* `rypipe.read("data.txt", format="log")` works with explicit format.

## Re-exporting stages { #re-exporting-stages }

Adapters re-export the pipeline stage classes from **rypipe**. Users
import everything from the adapter, never from **rypipe** directly:

```python
from rypipe_log import CastTypes, FilterRows, RenameFields, DropFields
```

The re-export pattern:

```python
# rypipe_log/stages/__init__.py
from rypipe.stages import (
    CastTypes,
    FilterRows,
    FilterRowsAny,
    FilterRowsAll,
    FilterRowsNot,
    RenameFields,
    DropFields,
)

__all__ = [
    "CastTypes",
    "FilterRows",
    "FilterRowsAny",
    "FilterRowsAll",
    "FilterRowsNot",
    "RenameFields",
    "DropFields",
]
```

See [Stages](../tutorial/stages.md) for what each stage does and
[Stage Protocol](../advanced/stage-protocol.md) for why re-exporting works
and when to re-implement.


## Streaming { #streaming }

For bounded-memory streaming, override `iter_record_batches` on your Source.
This enables users to pass `memory=` to any sink:

```python
class LogSource(Source):
    def _read_arrow(self, plan_overrides=None):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return _rypipe_log.read(str(self._path), **plan)

    def iter_record_batches(self, memory="64MiB", batch_size=None, **kwargs):
        plan = self._build_plan_kwargs()
        return _rypipe_log.iter_batches(
            str(self._path), memory=memory, batch_size=batch_size, **plan
        )
```

Users can then process large files with bounded memory:

```python
from rypipe_log import LogSource

src = LogSource("huge_report.log")

# Streaming DataFrame (most common)
df = src.to_pandas(memory="256MiB")

# Streaming Parquet
src.to_parquet("output.parquet", memory="256MiB")

# Parallel streaming (higher throughput)
df = src.to_pandas(memory="256MiB", threads=16)

# Advanced: batch-level control
for batch in src.iter_record_batches(memory="256MiB"):
    process(batch)
```

## Recap { #recap }

* **Source**: pipeline-capable, implements `_read_arrow()` with plan
  forwarding.
* **Adapter**: thin wrapper, `read()` delegates to `Source(...).to_arrow()`.
* **Stages**: re-exported from `rypipe.stages`; users import from your adapter.
* **Registration**: adapter registered at import time via side-effect import.
* Users import everything from the adapter package, never from **rypipe**.
