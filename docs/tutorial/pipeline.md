# Pipeline { #pipeline }

The pipeline operator (`|`) lets you chain transformation stages on a Source.
Each stage transforms the data as it flows through, like a Unix pipe.

## Basic usage { #basic-usage }

```python
from crxml import CrystalXMLSource, RenameFields, CastTypes, FilterRows

src = CrystalXMLSource("report.xml", row_tag="Details")

result = (
    src
    | RenameFields({"Name": "name", "Amount": "amount"})
    | CastTypes({"amount": float})
    | FilterRows(field="Status", op="==", value="Active")
)

# Materialize to a table
table = result.to_arrow()
```

Each `|` returns a new `Pipeline` — the original Source is not modified.

### What **rypipe** does automatically { #what-rypipe-does}

When you call `.to_arrow()` on a pipeline, **rypipe**:

1. Splits the stages into **fusable** and **non-fusable** groups.
2. Pushes fusable stages (RenameFields, DropFields, CastTypes, constant
   FilterRows) into the Rust parse loop via the plan.
3. Runs remaining stages (lambda predicates, complex combinators) over Arrow
   batches in Python.

This means fusable stages run at Rust speed during parsing: they never touch
Python.

## Building the Python wrapper { #building-the-python-wrapper }

To support the pipeline `|` operator, your adapter needs a Source subclass
that forwards plan kwargs to the Rust reader.

### `rypipe_log/source.py` { #source-py }

```python
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
        return _rypipe_log.read_log(str(self._path), **plan)
```

When a user writes `src | RenameFields(...) | FilterRows(...)`, the pipeline
collects stages into a plan. When `.to_arrow()` is called, the pipeline calls
`_read_arrow(plan_overrides=...)` on your source.

`plan_overrides` contains the fused stage kwargs:

```python
{
    "field_mapping": {"Name": "name"},
    "drop_fields": ["InternalId"],
    "filter": {"field": "Status", "op": "==", value": "Active"},
    "field_types": {"Amount": "float64"},
}
```

You must merge these with your construction kwargs and pass them to your
Rust reader. If you ignore `plan_overrides`, fused stages silently fall back
to Python execution (10-50x slower).

### `rypipe_log/rypipe_adapter.py` { #adapter-py}

The adapter is a thin, stateless wrapper that delegates to the Source:

```python
from __future__ import annotations
from typing import Any

from .source import LogSource


class LogAdapter:
    """rypipe-compatible adapter for newline-delimited key=value logs."""

    def read(self, path: str, **kwargs: Any) -> Any:
        """Parse ``path`` and return a ``pyarrow.Table``."""
        return LogSource(path, **kwargs).to_arrow()


def _register() -> None:
    try:
        import rypipe
    except Exception:  # pragma: no cover — rypipe is optional
        return
    rypipe.register_adapter("log", LogAdapter(), extensions=[".log"])


_register()
```

!!! note

    The adapter's `read()` method returns a `pyarrow.Table`, not a Source.
    This is by design — `rypipe.read()` calls `adapter.read()` and expects a
    table. Users who want pipelines use the Source directly.

## Using the Pipeline { #using-the-pipeline}

With the Source and stages in place, users can write:

```python
from rypipe_log import LogSource, CastTypes, FilterRows

src = LogSource("test.log")
result = (
    src
    | CastTypes({"age": int})
    | FilterRows(field="active", op="==", value="true")
)

table = result.to_arrow()
```

## Chaining multiple stages { #chaining-multiple-stages}

Stages are applied in order. Each `|` returns a new `Pipeline`:

```python
from rypipe_log import LogSource
from rypipe_log import RenameFields, DropFields, CastTypes, FilterRows

src = LogSource("test.log")

# All stages are fusable: runs entirely in Rust
table = (
    src
    | RenameFields({"Name": "name"})
    | DropFields(["InternalId"])
    | CastTypes({"age": int})
    | FilterRows(field="active", op="==", value="true")
).to_arrow()
```

## Iterating rows { #iterating-rows}

You can iterate over pipeline results as Python dicts:

```python
from rypipe_log import LogSource, FilterRows

src = LogSource("test.log")
pipeline = src | FilterRows(field="active", op="==", value="true")

for row in pipeline:
    print(row["name"], row["age"])
```

## Collecting to a list { #collecting-to-a-list}

```python
from rypipe import collect

rows = collect(src | FilterRows(field="active", op="==", value="true"))
```

## Recap { #recap }

* Use `|` to chain stages on a Source.
* **rypipe** pushes fusable stages into the Rust parse loop automatically.
* The Source's `_read_arrow()` method must forward `plan_overrides` to the
  Rust reader.
* Import stages from the adapter package, not from **rypipe**.
* Call `.to_arrow()`, `.to_pandas()`, or `.to_polars()` to materialize.

**Next:** [Stages](stages.md#stages), implement `CastTypes`, `FilterRows`,
etc.
