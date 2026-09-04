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

### `rypipe_log/__init__.py`

```python
import rypipe
import _rypipe_log


class LogAdapter:
    """rypipe adapter for newline-delimited key=value logs."""

    def read(self, path, **kwargs):
        return _rypipe_log.read_log(path, **kwargs)


# Register with rypipe so rypipe.read("file.log") works
rypipe.register_adapter("log", LogAdapter(), extensions=[".log"])
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

# Read with rypipe
table = rypipe.read("test.log")
print(table)
# pyarrow.Table<name: string, age: string, active: string>
# ----
# name: ["Alice", "Bob"]
# age: ["30", "25"]
# active: ["true", "false"]

# Read with pipeline
from rypipe import CastTypes, FilterRows

src = rypipe_log.LogAdapter()
result = (
    src.read("test.log")
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
import rypipe
import _rypipe_log


class LogAdapter:
    def read(self, path, **kwargs):
        return _rypipe_log.read_log(path, **kwargs)


rypipe.register_adapter("log", LogAdapter(), extensions=[".log"])
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

### Adding schema support

Once your basic adapter works, add schema support for better performance:

```python
class LogAdapter:
    def read(self, path, **kwargs):
        return _rypipe_log.read_log(path, **kwargs)
```

Users can now pass schema kwargs:

```python
table = rypipe.read(
    "app.log",
    schema=["name", "age", "active"],
    field_types={"age": "int64"},
)
```

### Adding pipeline support

To support the pipeline `|` operator, upgrade to a Source subclass:

```python
from rypipe import Source

class LogSource(Source):
    def _read_arrow(self, *, plan_overrides=None, **kwargs):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return _rypipe_log.read_log(str(self._path), **plan)

rypipe.register_adapter("log", LogSource, extensions=[".log"])
```

Now users can write:

```python
from rypipe_log import LogSource
from rypipe import CastTypes, FilterRows

src = LogSource("app.log")
table = (
    src
    | CastTypes({"age": int})
    | FilterRows(field="active", op="==", value="true")
).to_arrow()
```

### Adding streaming support

To support bounded-memory streaming:

```python
class LogSource(Source):
    def _read_arrow(self, *, plan_overrides=None, **kwargs):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return _rypipe_log.read_log(str(self._path), **plan)

    def iter_record_batches(self, *, memory="64MiB", batch_size=None, **kwargs):
        plan = self._build_plan_kwargs()
        return _rypipe_log.iter_batches(
            str(self._path),
            memory=memory,
            batch_size=batch_size,
            **plan,
        )
```

Users can now process large files:

```python
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
`register_adapter` call:

```python
rypipe.register_adapter("log", LogAdapter(), extensions=[".log"])
```

### "module '_rypipe_log' has no attribute 'read_log'"

The Rust extension was not built. Run:

```bash
maturin develop --release
```

### Pipeline stages not fusing

Make sure your Source forwards `plan_overrides`:

```python
def _read_arrow(self, *, plan_overrides=None, **kwargs):
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
