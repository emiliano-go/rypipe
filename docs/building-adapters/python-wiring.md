# Python Adapter Wiring { #python-wiring }

This page explains how to wire your Rust adapter to Python: the Source
subclass, adapter class, registration, and repacked stages.

## The crxml formula { #the-crxml-formula }

The reference adapter ([**crxml**](../crxml-adapter.md)) defines the standard pattern. Every adapter
should follow this structure:

### Users import everything from the adapter package { #users-import-everything-from-the-adapter-package }

```python
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
from typing import Any

from rypipe import Source
from rypipe_log import _rypipe_log


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
to Python execution: 10-50× slower.

!!! warning

    Never ignore `plan_overrides`. Fused stages silently fall back to Python
    execution over a full table when plan kwargs are not forwarded, turning a
    microsecond Rust path into a millisecond Python loop.


## Adapter class { #adapter-class }

The adapter is a thin, stateless wrapper. `read()` calls the Rust reader
directly; `iter_record_batches()` delegates to the Source for streaming:

```python
# rypipe_log/rypipe_adapter.py { #rypipe_logrypipe_adapterpy }
from typing import Any

from .source import LogSource


class LogAdapter:
    """rypipe-compatible adapter for newline-delimited key=value logs."""

    def read(self, path: str, **kwargs: Any) -> Any:
        """Parse ``path`` and return a ``pyarrow.Table``."""
        from rypipe_log import _rypipe_log

        return _rypipe_log.read_log(path, **kwargs)

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

    `LogAdapter` deliberately does **not** inherit from `rypipe.Adapter`.
    `register_adapter()` accepts any plain object with a
    `read(path, **kwargs)` method that returns a `pyarrow.Table`;
    `rypipe.read()` calls that method directly. `rypipe.Adapter` is a
    different thing: a `Source` subclass instantiated per file
    (`LogSource` plays that role here), giving users pipelines, caching,
    and streaming. See
    [Adapter design patterns](../advanced/source-pattern.md) for details.


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
    "LogAdapter": ".rypipe_adapter",
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
[Stage Protocol](../advanced/stage-protocol.md) for why re-exporting works.

!!! tip

    Re-exporting is zero-cost. The stage classes are the same objects; the
    engine fuses them identically whether they come from your package or
    from **rypipe**. Copying the implementations creates maintenance burden
    with no benefit.

### When to re-implement a stage { #when-to-re-implement }

Re-export the standard stages. Only re-implement when you need
format-specific behavior they cannot express. For example, suppose your
format has a `status` field that is always uppercase, but downstream
consumers expect lowercase. Subclass `FilterRows` to normalize before
filtering and log warnings for unexpected values:

```python
from rypipe.stages import FilterRows

class ValidatingFilterRows(FilterRows):
    """FilterRows that normalizes status values and logs warnings."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._warnings = []

    def apply(self, record: dict) -> dict | None:
        if "status" in record:
            record["status"] = record["status"].lower()
            if record["status"] not in ("active", "inactive", "pending"):
                self._warnings.append(f"unexpected status: {record['status']}")
        return super().apply(record)
```

It fuses identically to the standard stage because `_plan_kwargs()` is
inherited. If you override `_plan_kwargs()` and return `None`, the stage
falls back to Python execution: only do this when fusion is impossible
(for example, the stage depends on external state). See
[Pushdown Fusion](../advanced/fusion.md) for how the engine compiles stages
into an execution plan.

## Re-exporting sinks { #re-exporting-sinks }

Adapters also repack the standalone sink functions, so users can
materialize pipelines without importing **rypipe**:

```python
# rypipe_log/sinks.py
from rypipe.sinks import to_pandas as _rypipe_to_pandas
from rypipe.sinks import to_csv as _rypipe_to_csv
from rypipe.sinks import collect as _rypipe_collect
from rypipe.sinks import to_arrow as _rypipe_to_arrow
from rypipe.sinks import to_polars as _rypipe_to_polars
from rypipe.sinks import to_parquet as _rypipe_to_parquet

# Re-export from rypipe with the adapter's namespace
collect = _rypipe_collect
to_pandas = _rypipe_to_pandas
to_arrow = _rypipe_to_arrow
to_polars = _rypipe_to_polars
to_parquet = _rypipe_to_parquet
to_csv = _rypipe_to_csv
```

Or reimplement them from scratch for full control. Users then write
`from rypipe_log import collect, to_pandas` and never touch **rypipe**.

## Adapter kwargs { #adapter-kwargs }

You decide which kwargs your Source accepts and how to forward them to your
Rust backend.

### Common kwargs (recommended) { #common-kwargs }

These are defined by **rypipe** and give users a consistent experience
across adapters. The base `Source` class stores them and
`_build_plan_kwargs()` forwards them:

| Kwarg | Rust type | Purpose |
|-------|-----------|---------|
| `field_mapping` | `HashMap<String, String>` | Rename columns during parsing |
| `drop_fields` | `Vec<String>` | Skip columns entirely |
| `filter` | `HashMap<String, Value>` | Pushdown filter predicate |
| `field_types` | `HashMap<String, String>` | Type hints for columns |
| `dictionary_columns` | `Vec<String>` | Dictionary-encode columns |
| `schema` | `Vec<String>` | Expected column names and order |
| `auto_dict` | `bool` | Auto-dictionary low-cardinality strings |

### Custom kwargs { #custom-kwargs }

Add adapter-specific kwargs to your Source constructor, store them as
instance attributes, and always call `super().__init__()` so the common
kwargs keep working:

```python
class LogSource(Source):
    def __init__(self, path, *, row_tag="Row", threads=0, **kwargs):
        self._row_tag = row_tag
        self._threads = threads
        super().__init__(path, **kwargs)  # handles the common kwargs
```

## Engine selection { #engine-selection }

rypipe-core provides four execution modes: `columnar`, `parallel`,
`stream`, and `parallel_streaming`. Your adapter wires two entry points:

| Entry point | Modes | Purpose |
|-------------|-------|---------|
| `_read_arrow()` | columnar, parallel, stream | Full-table reads (`to_arrow`, `to_pandas`) |
| `iter_record_batches()` | stream, parallel_streaming | Bounded-memory reads |

`rypipe.resolve_engine` picks the optimal mode from file size, memory
budget, and threads. Use it to dispatch in `_read_arrow`:

```python
from rypipe import Source, resolve_engine
from rypipe_log import _rypipe_log

class LogSource(Source):
    def __init__(self, path, *, engine="auto", **kwargs):
        self._engine = engine
        super().__init__(path, **kwargs)

    def _resolve_engine(self) -> str:
        if self._engine != "auto":
            return self._engine
        return resolve_engine(
            file_size=self._path.stat().st_size,
            memory=self._memory,
            threads=self._threads,
            schema=self._schema or None,
            has_parallel=_HAS_PARALLEL,
            has_columnar=_HAS_COLUMNAR,
        )

    def _read_arrow(self, plan_overrides=None):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)

        engine = self._resolve_engine()
        if engine == "parallel":
            return _rypipe_log.read_par(str(self._path), **plan)
        elif engine == "stream":
            return _rypipe_log.read_stream(str(self._path), **plan)
        else:  # columnar (default)
            return _rypipe_log.read_log(str(self._path), **plan)
```

`iter_record_batches` always streams regardless of the engine mode, so it
needs no dispatch. A simple adapter can skip `resolve_engine` and call one
mode directly, as shown in [Source subclass](#source-subclass) above; add
engine selection when you expose more than one Rust entry point.

## Streaming { #streaming }

Most adapters get streaming almost for free: the engine provides Rust
streaming iterators that handle bounded memory, chunking, and parallelism.
Your side of the deal is overriding `iter_record_batches` on your Source
and forwarding the plan kwargs, the same way `_read_arrow` does:

```python
from rypipe_log import _rypipe_log

class LogSource(Source):
    def _read_arrow(self, plan_overrides=None):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return _rypipe_log.read_log(str(self._path), **plan)

    def iter_record_batches(self, memory="64MiB", batch_size=None, **kwargs):
        plan = self._build_plan_kwargs()
        return _rypipe_log.iter_batches(
            str(self._path), memory=memory, batch_size=batch_size, **plan
        )
```

The adapter class delegates to the Source (see
[Adapter class](#adapter-class) above for the full definition):

```python
class LogAdapter:
    def iter_record_batches(self, path, memory="64MiB", batch_size=None, **kwargs):
        yield from LogSource(path, **kwargs).iter_record_batches(
            memory=memory, batch_size=batch_size
        )
```

Users can then process large files with bounded memory:

```python
from rypipe_log import LogSource

src = LogSource("huge_report.log")

# Materialize (bounded by the engine's default budget)
df = src.to_pandas()
src.to_parquet("output.parquet")

# Batch-level control with an explicit memory budget
for batch in src.iter_record_batches(memory="256MiB"):
    process(batch)
```

With this wiring:

* `to_pandas()` and `to_parquet(path)` go through the engine's bounded
  read path.
* `iter_record_batches(memory="256MiB")` streams batches with peak memory
  bounded by the `memory` parameter.
* Fusable stages run in the parse loop (no Python overhead).

!!! note

    If your format has special chunking requirements (for example, row
    boundaries span chunks), implement `iter_batches` in Rust and expose it
    via PyO3. See [Rust Creation](rust-creation.md) for the traits and
    [Chunk planning](chunk-planning.md) for how the engine splits input.

## Build and test { #build-and-test }

Rebuild the extension and smoke-test the wiring: registration, the Source,
and the sinks should all work from the adapter package alone:

```console
$ uv run --with maturin maturin develop --release
📦 Built wheel for abi3 Python ≥ 3.10 to /tmp/.../rypipe_log-0.1.0-cp310-abi3-linux_x86_64.whl
🛠 Installed rypipe-log-0.1.0
$ python wiring_smoke.py
2 rows via rypipe.read
2 rows via LogSource
```

```python
# wiring_smoke.py
import rypipe
import rypipe_log  # side-effect: register_adapter("log", ...)

print(rypipe.read("test.log").num_rows, "rows via rypipe.read")

from rypipe_log import LogSource

print(LogSource("test.log").to_arrow().num_rows, "rows via LogSource")
```

If the first line raises `ValueError: no adapter registered`, the
side-effect import in `rypipe_log/__init__.py` is missing. If the second
raises `TypeError` about unexpected kwargs, `_read_arrow` is not
forwarding the plan (see [Adapter kwargs](#adapter-kwargs)).

## Recap { #recap }

* **Source**: pipeline-capable, implements `_read_arrow()` with plan
  forwarding.
* **Adapter**: thin wrapper, `read()` delegates to `Source(...).to_arrow()`.
* **Stages**: re-exported from `rypipe.stages`; subclass only for
  format-specific behavior.
* **Sinks**: repacked from `rypipe.sinks` so users never import **rypipe**.
* **Kwargs**: accept the common kwargs via `super().__init__()`, add custom
  ones for your format, use `resolve_engine` for `engine="auto"`.
* **Streaming**: override `iter_record_batches()` and forward plan kwargs.
* **Registration**: adapter registered at import time via side-effect import.
* Users import everything from the adapter package, never from **rypipe**.
