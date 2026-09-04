# Quick Start

This guide gets you from zero to a working rypipe adapter in 15 minutes. By
the end, you will have a complete adapter that parses a simple format,
registers with rypipe, and supports the full pipeline API.

## What you will build

A minimal adapter for a newline-delimited `key=value` log format:

```
name=Alice,age=30,active=true
name=Bob,age=25,active=false
```

Each line is a row. Fields are comma-separated `key=value` pairs.

## Prerequisites

- Rust toolchain (1.78+)
- Python 3.10+
- `rypipe` installed (`pip install rypipe`)
- Basic familiarity with Rust traits

## Step 1: Create the adapter package

```bash
mkdir rypipe-log && cd rypipe-log
mkdir src
```

### `Cargo.toml`

```toml
[package]
name = "rypipe-log"
version = "0.1.0"
edition = "2021"

[lib]
name = "_rypipe_log"
crate-type = ["cdylib"]

[dependencies]
rypipe-core = "2"
pyo3 = { version = "0.29", features = ["extension-module", "abi3-py310"] }
memchr = "2"
simdutf8 = "0.1"
```

## Step 2: Implement the Splitter

The Splitter finds safe chunk boundaries for parallel parsing. For our
newline-delimited format, we split on `\n`.

### `src/lib.rs`

```rust
use std::borrow::Cow;
use rypipe_core::{Splitter, RecordParser, ColumnarSink, Value, Result};

#[derive(Clone, Default)]
pub struct LogSplitter;

impl Splitter for LogSplitter {
    /// Find the next record start after `from`.
    /// Returns the byte offset of the first byte of the next record,
    /// or None if we have reached the end of the input.
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        memchr::memchr(b'\n', &bytes[from..]).map(|r| from + r + 1)
    }

    /// Estimate bytes per row from a sample.
    /// Used by the engine to size chunks and memory budgets.
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
        (sample.len() / n).max(1)
    }
}
```

### How it works

`next_record_start` is called repeatedly to find where each chunk should
start. The engine calls it like this:

```
pos = 0
while let Some(next) = splitter.next_record_start(bytes, pos) {
    // bytes[pos..next] is one chunk
    pos = next
}
```

For our format, we find the next newline and return the byte after it. The
`estimate_bytes_per_row` method helps the engine decide how many rows to
put in each chunk. We count newlines in a sample and divide.

## Step 3: Implement the RecordParser

The RecordParser turns raw bytes into field/value events that the engine
accumulates into Arrow columns.

```rust
#[derive(Clone, Default)]
pub struct LogParser;

impl RecordParser for LogParser {
    /// Validate that the bytes are valid UTF-8.
    /// Called once per chunk before parsing.
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Utf8(e))?;
        Ok(())
    }

    /// Parse a chunk of bytes into field/value events.
    /// This is the hot path. Make it fast.
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

        for line in text.lines() {
            if line.is_empty() {
                continue;
            }

            // Begin a new row
            sink.begin_row();

            // Parse comma-separated key=value pairs
            for part in line.split(',') {
                if let Some((key, value)) = part.split_once('=') {
                    // Only emit fields the engine wants (projection pushdown)
                    if sink.wants(key) {
                        sink.put_field(key, Value::Str(Cow::Borrowed(value)));
                    }
                }
            }

            // End the row
            sink.end_row();
        }

        Ok(())
    }
}
```

### How it works

`parse_chunk` is called once per chunk. The engine passes you a slice of
bytes (the chunk) and a `sink` (the `ColumnarSink`). Your job is to:

1. Iterate over rows in the chunk
2. For each row, call `sink.begin_row()`
3. For each field, call `sink.put_field(name, value)`
4. Call `sink.end_row()` when the row is done

The `sink.wants(key)` check is important. It returns `true` if the engine
needs this field (based on `schema_order`, `drop_fields`, etc.). If it
returns `false`, skip the field entirely (no scanning, no decoding).

### Value types

The `Value` enum represents a parsed field value:

```rust
pub enum Value<'a> {
    Str(Cow<'a, str>),      // string value (borrowed or owned)
    Int64(i64),              // 64-bit integer
    Float64(f64),            // 64-bit float
    Bool(bool),              // boolean
    Date32(i32),             // days since epoch
    Timestamp(i64),          // timestamp as raw integer (unit declared via field_types)
    Null,                    // explicit null/missing value
}
```

For a simple string format, always use `Value::Str(Cow::Borrowed(v))`.
The `Cow::Borrowed` variant avoids allocation by borrowing directly from
the input bytes.

When you have typed data, emit the correct variant directly. The engine
builds Arrow arrays from these values without string-to-number conversion.

## Step 4: Expose to Python

Add PyO3 bindings to expose your parser to Python.

```rust
use pyo3::prelude::*;
use rypipe_python::record_batches_to_pyarrow_table;

/// Read a log file and return a pyarrow.Table.
#[pyfunction]
fn read_log(path: String) -> PyResult<PyObject> {
    let plan = ExecutionPlan::new();
    let pipeline = Pipeline::new(LogSplitter, LogParser)
        .with_plan(plan);

    let batches = pipeline.read_path(&path, false, false)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    Python::with_gil(|py| {
        record_batches_to_pyarrow_table(py, &[batches])
            .map(|obj| obj.into())
    })
}

/// Python module definition
#[pymodule]
fn _rypipe_log(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_log, m)?)?;
    Ok(())
}
```

## Step 5: Create the Python wrapper

Follow the crxml adapter formula: a `Source` subclass for pipeline support,
a thin adapter that delegates to it, and repacked stage classes so users
never import from rypipe directly.

### Directory layout

```
rypipe_log/
├── __init__.py        # LogSource, LogAdapter, registration
└── stages/
    ├── __init__.py    # lazy re-exports
    ├── cast.py        # CastTypes
    ├── filter.py      # FilterRows
    ├── rename.py      # RenameFields
    └── drop.py        # DropFields
```

### `rypipe_log/__init__.py`

```python
import importlib

from . import rypipe_adapter  # noqa: F401  — registers with rypipe on import

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

### `rypipe_log/source.py`

```python
from __future__ import annotations

from typing import Any

import _rypipe_log
from rypipe import Source


class LogSource(Source):
    """Pipeline-capable source for newline-delimited key=value logs."""

    def _read_arrow(self, plan_overrides: dict[str, Any] | None = None) -> Any:
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return _rypipe_log.read_log(str(self._path), **plan)
```

### `rypipe_log/rypipe_adapter.py`

```python
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


def _register() -> None:
    try:
        import rypipe
    except Exception:  # pragma: no cover — rypipe is optional
        return
    rypipe.register_adapter("log", LogAdapter(), extensions=[".log"])


_register()
```

### `rypipe_log/stages/__init__.py`

```python
import importlib

__all__ = ["CastTypes", "FilterRows", "RenameFields", "DropFields"]

_modules = {
    "CastTypes": ".cast",
    "FilterRows": ".filter",
    "RenameFields": ".rename",
    "DropFields": ".drop",
}


def __getattr__(name):
    if name in _modules:
        mod = importlib.import_module(_modules[name], __package__)
        return getattr(mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return __all__
```

### `rypipe_log/stages/cast.py`

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
        mapping = self._mapping
        if not mapping:
            return record
        for field, cast_fn in mapping.items():
            try:
                record[field] = cast_fn(record[field])
            except KeyError:
                pass
            except (ValueError, TypeError) as e:
                val = record[field]
                raise ValueError(
                    f"CastTypes: cannot cast field '{field}' "
                    f"value {val!r}: {e}"
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
                    continue
                return None
            ft[field] = rust_type
        if not ft:
            return None
        return {"field_types": ft}
```

### `rypipe_log/stages/filter.py`

```python
class _ConstantPredicate:
    __slots__ = ("_field", "_op", "_value")

    _VALID_OPS = frozenset({"==", "eq", "!=", "ne"})

    def __init__(self, field: str, op: str, value: str):
        if op not in self._VALID_OPS:
            raise ValueError(
                f"FilterRows: unsupported operator {op!r} for constant filter; "
                f"use '==' or '!='"
            )
        self._field = field
        self._op = op
        self._value = value

    def __call__(self, record: dict) -> bool:
        actual = record.get(self._field)
        if self._op in ("==", "eq"):
            return actual == self._value
        return actual != self._value


class _ComparePredicate:
    __slots__ = ("_field_a", "_op", "_field_b")

    _OPS = {
        ">": lambda a, b: a > b,
        "<": lambda a, b: a < b,
        ">=": lambda a, b: a >= b,
        "<=": lambda a, b: a <= b,
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
        "eq": lambda a, b: a == b,
        "ne": lambda a, b: a != b,
        "gt": lambda a, b: a > b,
        "lt": lambda a, b: a < b,
        "ge": lambda a, b: a >= b,
        "le": lambda a, b: a <= b,
    }

    def __init__(self, field_a: str, op: str, field_b: str):
        if op not in self._OPS:
            valid = ", ".join(sorted(self._OPS))
            raise ValueError(
                f"FilterRows: unsupported operator {op!r} for column comparison; "
                f"valid operators: {valid}"
            )
        self._field_a = field_a
        self._op = op
        self._field_b = field_b

    def __call__(self, record: dict) -> bool:
        fn = self._OPS.get(self._op)
        return bool(fn(record.get(self._field_a), record.get(self._field_b)))


class FilterRows:
    __slots__ = ("_predicate", "_filter_spec")

    def __init__(
        self,
        predicate=None,
        *,
        field=None,
        op=None,
        value=None,
        field_a=None,
        field_b=None,
    ):
        if predicate is not None:
            self._predicate = predicate
            self._filter_spec = None
        elif field is not None and op is not None and value is not None:
            self._filter_spec = {"field": field, "op": op, "value": value}
            self._predicate = _ConstantPredicate(field, op, value)
        elif field_a is not None and op is not None and field_b is not None:
            self._filter_spec = {"field_a": field_a, "op": op, "field_b": field_b}
            self._predicate = _ComparePredicate(field_a, op, field_b)
        else:
            raise ValueError(
                "FilterRows requires either a callable predicate or "
                "keyword arguments (field+op+value for constant filter, "
                "or field_a+op+field_b for column comparison). "
                f"Got predicate={predicate!r}, field={field!r}, op={op!r}, "
                f"value={value!r}, field_a={field_a!r}, field_b={field_b!r}"
            )

    def apply(self, record: dict) -> dict | None:
        return record if self._predicate(record) else None

    def __call__(self, stream):
        return (r for r in map(self.apply, stream) if r is not None)

    def _plan_kwargs(self) -> dict | None:
        if self._filter_spec is not None:
            return {"filter": self._filter_spec}
        return None
```

### `rypipe_log/stages/rename.py`

```python
class RenameFields:
    __slots__ = ("_mapping",)

    def __init__(self, mapping: dict[str, str]):
        self._mapping = mapping

    def apply(self, record: dict) -> dict:
        mapping = self._mapping
        return {mapping.get(k, k): v for k, v in record.items()}

    def __call__(self, stream):
        return map(self.apply, stream)

    def _plan_kwargs(self) -> dict | None:
        return {"field_mapping": self._mapping}
```

### `rypipe_log/stages/drop.py`

```python
class DropFields:
    __slots__ = ("_fields_set",)

    def __init__(self, fields: list[str]):
        if isinstance(fields, str):
            raise TypeError(
                "DropFields expects a list of field names, got a bare "
                f"string; use DropFields([{fields!r}])"
            )
        self._fields_set = frozenset(fields)

    def apply(self, record: dict) -> dict:
        fields_set = self._fields_set
        if not fields_set:
            return record
        return {k: v for k, v in record.items() if k not in fields_set}

    def __call__(self, stream):
        return map(self.apply, stream)

    def _plan_kwargs(self) -> dict | None:
        return {"drop_fields": sorted(self._fields_set)}
```

## Step 6: Build and test

### Build the Rust extension

```bash
pip install maturin
maturin develop --release
```

### Test it

```python
import rypipe
import rypipe_log  # registers the adapter

# Create a test file
with open("test.log", "w") as f:
    f.write("name=Alice,age=30,active=true\n")
    f.write("name=Bob,age=25,active=false\n")

# Pattern 1: one-liner via rypipe (extension auto-detected)
table = rypipe.read("test.log")
print(table)
# pyarrow.Table<name: string, age: string, active: string>
# ----
# name: ["Alice", "Bob"]
# age: ["30", "25"]
# active: ["true", "false"]

# Pattern 2: pipeline via LogSource + repacked stages
from rypipe_log import LogSource, CastTypes, FilterRows

src = LogSource("test.log")
result = (
    src
    | CastTypes({"age": int})
    | FilterRows(field="active", op="==", value="true")
).to_arrow()
print(result)
# pyarrow.Table<name: string, age: int64, active: string>
# ----
# name: ["Alice"]
# age: [30]
# active: ["true"]
```

## What just happened

1. **Splitter** found newline boundaries in the file
2. **RecordParser** parsed each chunk, calling `sink.put_field` for each
   field in each row
3. **Engine** accumulated values into Arrow columns
4. **Export** produced a `pyarrow.Table` with zero-copy

The engine handled parallel execution, memory management, schema discovery,
and Arrow export. You only wrote the format-specific parsing logic.

## Next steps

- [Python wiring](./python-wiring.md): Subclass `Source`/`Adapter` for
  pipeline support and fusion
- [Rust adapter creation](./rust-creation.md): Deep dive into `Splitter`,
  `RecordParser`, and `ColumnarSink`
- [Techniques](./techniques.md): Performance optimizations for production
  adapters
- [Schema](./schema.md): Declare column names and types for maximum
  throughput
- [Examples](./examples.md): Worked CSV, JSONL, and TSV adapters

## Complete code listing

### `src/lib.rs`

```rust
use std::borrow::Cow;
use rypipe_core::{Splitter, RecordParser, ColumnarSink, Value, Result};
use rypipe_core::{ExecutionPlan, Pipeline};
use pyo3::prelude::*;
use rypipe_python::record_batches_to_pyarrow_table;

#[derive(Clone, Default)]
pub struct LogSplitter;

impl Splitter for LogSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        memchr::memchr(b'\n', &bytes[from..]).map(|r| from + r + 1)
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
        (sample.len() / n).max(1)
    }
}

#[derive(Clone, Default)]
pub struct LogParser;

impl RecordParser for LogParser {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Utf8(e))?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
        for line in text.lines() {
            if line.is_empty() { continue; }
            sink.begin_row();
            for part in line.split(',') {
                if let Some((k, v)) = part.split_once('=') {
                    if sink.wants(k) {
                        sink.put_field(k, Value::Str(Cow::Borrowed(v)));
                    }
                }
            }
            sink.end_row();
        }
        Ok(())
    }
}

#[pyfunction]
fn read_log(path: String) -> PyResult<PyObject> {
    let plan = ExecutionPlan::new();
    let batches = Pipeline::new(LogSplitter, LogParser)
        .with_plan(plan)
        .read_path(&path, false, false)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    Python::with_gil(|py| {
        record_batches_to_pyarrow_table(py, &[batches])
            .map(|obj| obj.into())
    })
}

#[pymodule]
fn _rypipe_log(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_log, m)?)?;
    Ok(())
}
```

### `rypipe_log/__init__.py`

```python
import importlib

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

### `Cargo.toml`

```toml
[package]
name = "rypipe-log"
version = "0.1.0"
edition = "2021"

[lib]
name = "_rypipe_log"
crate-type = ["cdylib"]

[dependencies]
rypipe-core = "2"
pyo3 = { version = "0.29", features = ["extension-module", "abi3-py310"] }
memchr = "2"
simdutf8 = "0.1"
```

## Extending the adapter

### Schema support

Users can pass schema kwargs through the adapter:

```python
table = rypipe.read(
    "app.log",
    schema=["name", "age", "active"],
    field_types={"age": "int64"},
)
```

Or directly on the Source:

```python
from rypipe_log import LogSource

src = LogSource("app.log", schema=["name", "age"], field_types={"age": "int64"})
table = src.to_arrow()
```

### Pipeline support

`LogSource` already supports the pipeline `|` operator. Users import
stages from the adapter package:

```python
from rypipe_log import LogSource, CastTypes, FilterRows, RenameFields, DropFields

src = LogSource("app.log")
table = (
    src
    | RenameFields({"name": "user_name"})
    | CastTypes({"age": int})
    | FilterRows(field="active", op="==", value="true")
).to_arrow()
```

### Streaming support

`LogSource` inherits `iter_record_batches` from `rypipe.Source`. For
true bounded-memory streaming, override it in `rypipe_log/source.py`:

```python
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

Users can then process large files:

```python
from rypipe_log import LogSource

src = LogSource("huge.log")
for batch in src.iter_record_batches(memory="256MiB"):
    process(batch)
```

## Troubleshooting

### "no adapter registered for 'log'"

Make sure you import your adapter before calling `rypipe.read`:

```python
import rypipe_log  # registers the adapter
table = rypipe.read("app.log")
```

### "cannot infer adapter from extension '.log'"

The adapter was not registered with the `.log` extension. Check your
`register_adapter` call in `rypipe_adapter.py`:

```python
rypipe.register_adapter("log", LogAdapter(), extensions=[".log"])
```

### "module '_rypipe_log' has no attribute 'read_log'"

The Rust extension was not built. Run:

```bash
maturin develop --release
```

### Pipeline stages not fusing

Make sure `LogSource` forwards `plan_overrides` in `_read_arrow`:

```python
def _read_arrow(self, plan_overrides=None):
    plan = self._build_plan_kwargs()
    if plan_overrides:
        plan.update(plan_overrides)
    return _rypipe_log.read_log(str(self._path), **plan)
```

## Next steps

- [Python wiring](./python-wiring.md): Deep dive into Source/Adapter patterns
- [Rust creation](./rust-creation.md): Implement Splitter and RecordParser
- [Techniques](./techniques.md): Performance optimizations
- [Schema](./schema.md): Declare columns for maximum throughput
- [Examples](./examples.md): Worked CSV, JSONL, and TSV adapters

## Understanding the code

### Why `Cow::Borrowed`?

`Cow::Borrowed` means "borrow this string from the input bytes, don't copy
it." This avoids heap allocation. The engine copies the bytes into the
Arrow array later (zero-copy when possible).

```rust
// Good: zero allocation, borrows from input
sink.put_field("name", Value::Str(Cow::Borrowed(value)));

// Bad: allocates a String on the heap
sink.put_field("name", Value::Str(Cow::Owned(value.to_string())));
```

### Why check `sink.wants()`?

The engine uses `wants()` for projection pushdown. If the user drops a
column, `wants()` returns `false` and you skip all work for that field:

```rust
if sink.wants(name) {
    // Only scan and emit if the engine needs this field
    let value = self.extract_value(bytes);
    sink.put_field(name, Value::Str(Cow::Borrowed(value)));
}
```

This saves significant CPU when columns are dropped.

### Why `estimate_bytes_per_row`?

The engine uses this to size chunks. A good estimate means chunks are
roughly equal size, which improves parallel efficiency:

```rust
fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
    let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
    (sample.len() / n).max(1)
}
```

Count newlines in a sample and divide. The engine uses this to decide
how many rows to put in each chunk.

### Why `validate`?

The engine calls `validate` once per chunk before parsing. It catches
invalid UTF-8 early with a useful error message, instead of panicking
later in `std::str::from_utf8`.

```rust
fn validate(&self, bytes: &[u8]) -> Result<()> {
    simdutf8::basic::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Utf8(e))?;
    Ok(())
}
```

## How parallel parsing works

The engine splits the file into chunks and parses them concurrently:

```
File: [row1][row2][row3][row4][row5][row6][row7][row8]
       |              |              |              |
       v              v              v              v
    Chunk 1        Chunk 2        Chunk 3        Chunk 4
    (thread 1)    (thread 2)    (thread 3)    (thread 4)
       |              |              |              |
       v              v              v              v
    TableBuilder  TableBuilder  TableBuilder  TableBuilder
       |              |              |              |
       v              v              v              v
    RecordBatch   RecordBatch   RecordBatch   RecordBatch
       |              |              |              |
       v              v              v              v
    pyarrow.Table (merged or parallel export)
```

Each chunk gets its own `TableBuilder`. The `Splitter` finds safe
boundaries so chunks don't overlap rows. The `RecordParser` parses
each chunk independently.

## How bounded-memory streaming works

In streaming mode, the engine processes one chunk at a time:

```
Chunk 1: [parse] -> [export] -> [free]
Chunk 2:         [parse] -> [export] -> [free]
Chunk 3:                 [parse] -> [export] -> [free]
Chunk 4:                         [parse] -> [export] -> [free]
```

Memory stays bounded regardless of file size. The `memory` parameter
controls how much memory each chunk can use:

```python
for batch in src.iter_record_batches(memory="256MiB"):
    process(batch)
```

Each batch is a `pyarrow.RecordBatch` with at most `memory` bytes of
data. Processing 1 GB of data with `memory="256MiB"` uses at most
256 MB of parsing memory at any time.
