# Building an Adapter { #building-an-adapter }

This page shows how to build a **rypipe** adapter for a newline-delimited
`key=value` log format. By the end, you will have a complete adapter that
works with the pipeline API.

## The format { #the-format }

Your format is newline-delimited key=value pairs:

```
name=Alice,age=30,active=true
name=Bob,age=25,active=false
```

Each line is a row. Fields are comma-separated `key=value` pairs.

## Prerequisites { #prerequisites }

* Rust toolchain (1.78+)
* Python 3.10+
* `rypipe` installed (`pip install rypipe`)

## Step 1: Create the package { #step-1-create-the-package }

```bash
mkdir rypipe-log && cd rypipe-log
mkdir src
```

### `Cargo.toml` { #cargo-toml }

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

## Step 2: Implement the Splitter { #step-2-implement-the-splitter}

The Splitter finds row boundaries. For newline-delimited formats, split on
`\n`:

### `src/lib.rs` (Splitter) { #splitter }

```rust
use rypipe_core::{Splitter, RecordParser, ColumnarSink, Value, Result};

// The Splitter tells the engine where each row starts.
// For newline-delimited formats, the next row starts after the next '\n'.
#[derive(Clone, Default)]
pub struct LogSplitter;

impl Splitter for LogSplitter {
    // Find the byte position of the next record start after `from`.
    // Return None when we reach the end of the input.
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        memchr::memchr(b'\n', &bytes[from..]).map(|r| from + r + 1)
    }

    // Estimate bytes per row from a sample. The engine uses this to size
    // chunks and memory budgets. Count newlines and divide.
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
        (sample.len() / n).max(1)
    }
}
```

The Splitter has two methods:

* `next_record_start` — called repeatedly to find chunk boundaries. The engine
  splits the file at these positions for parallel parsing.
* `estimate_bytes_per_row` — tells the engine how many rows to expect per
  chunk, so it can size memory budgets.

## Step 3: Implement the RecordParser { #step-3-implement-the-recordparser}

The RecordParser extracts field values from each row:

### `src/lib.rs` (RecordParser) { #recordparser }

```rust
// The RecordParser turns raw bytes into field/value events.
// parse_chunk is called once per chunk: this is the hot path.
#[derive(Clone, Default)]
pub struct LogParser;

impl RecordParser for LogParser {
    // Validate UTF-8 before parsing. Called once per chunk.
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Utf8(e))?;
        Ok(())
    }

    // Parse a chunk of bytes into field/value events.
    // For each row: begin_row -> put_field x N -> end_row.
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

        for line in text.lines() {
            if line.is_empty() { continue; }

            // Signal the start of a new row
            sink.begin_row();

            // Parse comma-separated key=value pairs
            for part in line.split(',') {
                if let Some((key, value)) = part.split_once('=') {
                    // sink.wants() returns false if the engine doesn't need
                    // this field (projection pushdown). Skip it entirely.
                    if sink.wants(key) {
                        // Borrow the string from the input bytes (zero allocation)
                        sink.put_field(key, Value::Str(std::borrow::Cow::Borrowed(value)));
                    }
                }
            }

            // Signal the end of the row
            sink.end_row();
        }

        Ok(())
    }
}
```

The RecordParser has two methods:

* `validate` — called once per chunk to check that the bytes are valid UTF-8.
* `parse_chunk` — the hot path. For each row, call `sink.begin_row()`,
  then `sink.put_field()` for each field, then `sink.end_row()`.

!!! tip

    Always check `sink.wants(key)` before parsing a field's value. When the user
    drops a column, `wants()` returns `false` and you skip all work for that
    field: no scanning, no decoding.

## Step 4: Expose to Python { #step-4-expose-to-python}

Add PyO3 bindings:

### `src/lib.rs` (Python bindings) { #python-bindings }

```rust
use pyo3::prelude::*;
use rypipe_core::{ExecutionPlan, Pipeline};
use rypipe_python::record_batches_to_pyarrow_table;

// Read a log file and return a pyarrow.Table.
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

// Python module definition
#[pymodule]
fn _rypipe_log(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_log, m)?)?;
    Ok(())
}
```

This creates a Python module `_rypipe_log` with a `read_log` function that
the Python wrapper will call.

## Step 5: Create the Python wrapper { #step-5-create-the-python-wrapper}

Follow the crxml formula: a Source subclass, a thin adapter, and repacked
stages.

### `rypipe_log/__init__.py` { #init-py }

```python
import importlib

# Side-effect import: registers the adapter with rypipe on import
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

### `rypipe_log/source.py` { #source-py }

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

!!! warning

    The `_read_arrow` method **must** forward `plan_overrides` to the
    Rust reader. If you ignore them, fused pipeline stages silently fall back
    to Python execution: 10-50x slower than the Rust path.

### `rypipe_log/rypipe_adapter.py` { #adapter-py }

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

For the full stage implementations (`CastTypes`, `FilterRows`, etc.), see
[Stages](stages.md#stages).

## Step 6: Build and test { #step-6-build-and-test}

### Build { #build }

```bash
pip install maturin
maturin develop --release
```

### Test it { #test }

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
# name: ["Alice"]
# age: [30]
# active: ["true"]
```

## What just happened { #what-just-happened}

1. **Splitter** found newline boundaries in the file.
2. **RecordParser** parsed each chunk, calling `sink.put_field` for each
   field in each row.
3. **Engine** accumulated values into Arrow columns.
4. **Export** produced a `pyarrow.Table` with zero-copy.

## Next steps { #next-steps}

* [Pipeline](pipeline.md#pipeline) — how the `|` operator and plan forwarding
  work
* [Stages](stages.md#stages) — implement `CastTypes`, `FilterRows`, etc.
* [Sinks](sinks.md#sinks) — implement `collect`, `to_pandas`, `to_csv`
* [Rust Creation](../writing-adapters/rust-creation.md) — deep dive into
  Splitter, RecordParser, and ColumnarSink
* [Schema](../writing-adapters/schema.md) — declare columns for maximum
  performance

## Recap { #recap }

* An adapter is a Rust crate (Splitter + RecordParser) and a Python package
  (Source + stages + sinks).
* The engine handles parallel execution, memory management, and Arrow export.
* Users import everything from the adapter package, never from **rypipe**
  directly.
* `rypipe.read("file.log")` works via the registered adapter.
* `LogSource("file.log") | CastTypes(...) | FilterRows(...)` works via the
  Source pipeline.
