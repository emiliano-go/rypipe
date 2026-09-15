# Building an Adapter { #building-an-adapter }

The [track overview](index.md) showed what an adapter is and how the engine
works. In this walkthrough you will build one: a complete **rypipe** adapter
for a newline-delimited `key=value` log format. By the end, it works with
`rypipe.read()` and with the full pipeline API.

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

An adapter is a separate package that depends on `rypipe-core`.

### Scaffold with cargo-generate (optional) { #scaffold-with-cargo-generate }

To skip the manual setup, generate the package from the built-in template:

```bash
cargo generate emiliano-go/rypipe template
```

This creates `src/lib.rs`, `Cargo.toml`, `pyproject.toml`, and a README with
the correct dependencies already wired. Skip to [Step 2](#step-2-implement-the-splitter) if you
used the template; otherwise continue with the manual setup below.

### Manual setup { #manual-setup }

Create the package structure and `Cargo.toml`:

```bash
mkdir rypipe-log && cd rypipe-log
mkdir src
```

The `Cargo.toml` defines the Rust crate that will be compiled into a Python
extension module. We depend on `rypipe-core` for the `Splitter` and
`RecordParser` traits, and `pyo3` for Python bindings:

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
rypipe-core = "0.3"
rypipe_python = { package = "rypipe-python", version = "0.3.2" }
arrow = { version = "=59.3.0", default-features = false, features = ["pyarrow", "ffi"] }
pyo3 = { version = "0.29", features = ["extension-module", "abi3-py310"] }
memchr = "2"
simdutf8 = "0.1"
```

The `cdylib` crate type produces a shared library that Python can import.
The `abi3` feature enables stable ABI, so one wheel works across Python
versions.

!!! warning "Arrow and pyo3 version pinning"

    `arrow` must be pinned with `=` (e.g. `=59.3.0`) to match the version
    `rypipe-core` was built against. A mismatch produces inscrutable linker
    errors like `undefined symbol` or `missing field`. If you see these,
    check that your `arrow` version matches `rypipe-core`'s exactly. The
    `rypipe_python` dependency alias avoids its `_rypipe` extension name
    colliding with your adapter module.

!!! note "Developing inside the rypipe workspace"

    If your adapter lives inside the rypipe repo (or any Cargo workspace),
    add `[workspace]` to your own `Cargo.toml` to avoid resolver conflicts:

    ```toml
    [workspace]
    ```

    Without this, Cargo may try to unify your adapter's dependencies with
    the workspace root, causing version resolution errors.

You also need a `pyproject.toml` so maturin can build the package. The
`module-name` places the compiled extension inside the Python package,
and `python-source` tells maturin where the pure-Python files live:

### `pyproject.toml` { #pyproject-toml }

```toml
[build-system]
requires = ["maturin>=1.5"]
build-backend = "maturin"

[project]
name = "rypipe-log"
version = "0.1.0"
dependencies = ["rypipe", "pyarrow>=15", "pandas>=1.5"]

[tool.maturin]
module-name = "rypipe_log._rypipe_log"
python-source = "."
features = ["pyo3/extension-module"]
```

## Step 2: Implement the Splitter { #step-2-implement-the-splitter}

The Splitter tells the engine where each row starts. The engine calls
`next_record_start` repeatedly to split the file into chunks for parallel
parsing. For newline-delimited formats, the next row starts after the next
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

* `next_record_start`: called repeatedly to find chunk boundaries. The engine
  splits the file at these positions for parallel parsing. We use `memchr`
  for fast newline scanning.
* `estimate_bytes_per_row`: tells the engine how many rows to expect per
  chunk, so it can size memory budgets. We count newlines in a sample and
  divide.

## Step 3: Implement the RecordParser { #step-3-implement-the-recordparser}

The RecordParser extracts field values from each row. This is the hot path:
it is called once per chunk, so it must be fast. For each row, we call
`sink.begin_row()`, then `sink.put_field()` for each field, then
`sink.end_row()`:

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

* `validate`: called once per chunk to check that the bytes are valid UTF-8.
  We use `simdutf8` for fast validation.
* `parse_chunk`: the hot path. For each row, call `sink.begin_row()`, then
  `sink.put_field()` for each field, then `sink.end_row()`. We use
  `Cow::Borrowed` to borrow the string from the input bytes without
  allocation.

!!! tip

    Always check `sink.wants(key)` before parsing a field's value. When the user
    drops a column, `wants()` returns `false` and you skip all work for that
    field: no scanning, no decoding.

!!! tip "Empty rows from blank lines and comment-only blocks"

    Blank lines and comment-only blocks produce silent null rows if not
    handled. Two patterns:

    **Skip blank lines** (one-line-per-record formats):

    ```rust
    for line in text.lines() {
        if line.is_empty() { continue; }
        // ... parse and emit row ...
    }
    ```

    **Accumulate across lines** (multi-line record formats). Call
    `begin_row` on the first non-blank line and `end_row` on blank-line
    boundaries:

    ```rust
    let mut in_record = false;
    for line in text.lines() {
        if line.is_empty() {
            if in_record { sink.end_row(); in_record = false; }
            continue;
        }
        if !in_record { sink.begin_row(); in_record = true; }
        // ... put_field for each line ...
    }
    if in_record { sink.end_row(); }
    ```

    See [Multi-line Record Adapter](examples.md#multi-line-adapter) for a
    complete worked example.

!!! tip "Continuations and comments"

    If your format uses `\` continuations or `#`/`!` comments, use the
    declarative Splitter methods instead of scanning bytes manually:

    ```rust
    use rypipe_core::{RecordBoundary, find_next_record_boundary};

    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        find_next_record_boundary(
            bytes, from,
            self.continuation_char(),    // Some(b'\\')
            self.comment_prefixes(),     // &[b"#", b"!"]
            self.record_boundary() == RecordBoundary::BlankLine,
        )
    }
    ```

    See [Splitter: Stateful formats](splitter.md#stateful-formats) and
    [Properties Adapter](examples.md#properties-adapter) for complete
    examples.

## Step 4: Expose to Python { #step-4-expose-to-python}

Add PyO3 bindings to expose your parser to Python. The `read_log` function
is an internal function that `LogSource._read_arrow()` calls. Users never
call it directly, they use `LogSource` instead:

### `src/lib.rs` (Python bindings) { #python-bindings }

The shared helper validates the plan and builds every supported predicate.
Detach Python while parsing, then export the result as a table:

```rust
use std::collections::HashMap;
use pyo3::prelude::*;
use rypipe_core::Pipeline;
use rypipe_python::{
    execution_plan_from_kwargs, py_err_from_rypipe, record_batches_to_pyarrow_table,
};

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (path, field_mapping=None, drop_fields=None, filter=None,
    field_types=None, dictionary_columns=None, schema=None, auto_dict=false,
    auto_dict_threshold=None, auto_dict_max_size=None, strict_types=false,
    max_split_chunks=None, observer=None, use_mmap=false, prefault=false))]
fn read_log(
    py: Python<'_>,
    path: String,
    field_mapping: Option<HashMap<String, String>>,
    drop_fields: Option<Vec<String>>,
    filter: Option<Bound<'_, PyAny>>,
    field_types: Option<HashMap<String, String>>,
    dictionary_columns: Option<Vec<String>>,
    schema: Option<Vec<String>>,
    auto_dict: bool,
    auto_dict_threshold: Option<f64>,
    auto_dict_max_size: Option<usize>,
    strict_types: bool,
    max_split_chunks: Option<usize>,
    observer: Option<Bound<'_, PyAny>>,
    use_mmap: bool,
    prefault: bool,
) -> PyResult<Py<PyAny>> {
    let plan = execution_plan_from_kwargs(
        field_mapping, drop_fields, filter.as_ref(), field_types,
        dictionary_columns, schema, auto_dict, auto_dict_threshold,
        auto_dict_max_size, strict_types, max_split_chunks, observer.as_ref(),
    )?;
    let batch = py.detach(|| {
        Pipeline::new(LogSplitter, LogParser)
            .with_plan(plan)
            .read_path(&path, use_mmap, prefault)
    }).map_err(py_err_from_rypipe)?;
    record_batches_to_pyarrow_table(py, &[batch]).map(|table| table.unbind())
}

#[pymodule]
fn _rypipe_log(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_log, m)?)?;
    Ok(())
}
```

This creates `rypipe_log._rypipe_log.read_log`. The Python source calls it
with construction options and fused pipeline options. The helper accepts 12
plan arguments; `path`, `use_mmap`, and `prefault` belong to the reader.

!!! warning "Forward every plan option"

    An unsupported keyword raises `TypeError`. Accepting `**kwargs` and
    discarding options can silently change results. Use the shared helper
    and test a pipeline with renaming, casting, dropping, and compound filters.
    Stages outside the fusable prefix run after parsing in their original order.

!!! note "Local development before publication"

    Use `rypipe_python = { package = "rypipe-python", version = "0.3.2" }`.
    The dependency alias gives the helper a Rust name independent of its
    Python extension name. Errors use the same Python classes across adapters.

    Before matching crates are published, add local overrides:

    ```toml
    [patch.crates-io]
    rypipe-core = { path = "/path/to/rypipe/crates/rypipe-core" }
    rypipe-python = { path = "/path/to/rypipe/crates/rypipe-python" }
    ```

## Step 5: Create the Python wrapper { #step-5-create-the-python-wrapper}

Follow the [crxml](../crxml-adapter.md) formula: a Source subclass, a thin adapter, and repacked
stages. The Source subclass gives users the pipeline `|` operator and
caching. The thin adapter enables `rypipe.read()`. The repacked stages
make the adapter self-contained.

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
    "collect",
    "to_arrow",
    "to_pandas",
    "to_polars",
    "to_parquet",
    "to_csv",
]

_modules = {
    "LogSource": ".source",
    "LogAdapter": ".rypipe_adapter",
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
```

The `__init__.py` uses lazy loading: modules are only imported when accessed.
This avoids loading the Rust extension until it is actually needed.

### `rypipe_log/stages.py` { #stages-py }

The `_modules` map above points the stage names at a `.stages` module, so
create it. It just re-exports the standard stages from **rypipe**:

```python
from rypipe.stages import CastTypes, DropFields, FilterRows, RenameFields  # noqa: F401
```

### `rypipe_log/sinks.py` { #sinks-py }

The `_modules` map also points sink names at a `.sinks` module. Re-export
the standalone sink functions so users never import **rypipe** directly:

```python
# rypipe_log/sinks.py
from rypipe.sinks import collect as _rypipe_collect
from rypipe.sinks import to_arrow as _rypipe_to_arrow
from rypipe.sinks import to_pandas as _rypipe_to_pandas
from rypipe.sinks import to_polars as _rypipe_to_polars
from rypipe.sinks import to_parquet as _rypipe_to_parquet
from rypipe.sinks import to_csv as _rypipe_to_csv

collect = _rypipe_collect
to_arrow = _rypipe_to_arrow
to_pandas = _rypipe_to_pandas
to_polars = _rypipe_to_polars
to_parquet = _rypipe_to_parquet
to_csv = _rypipe_to_csv
```

Users then write `from rypipe_log import to_pandas` and never touch
**rypipe**. This is part of the **crxml formula**: adapters repack the
full pipeline API.

### `rypipe_log/source.py` { #source-py }

The Source subclass is the pipeline-capable entry point. It implements
`_read_arrow()` and forwards plan kwargs from fused stages:

```python
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

!!! warning

    The `_read_arrow` method **must** forward `plan_overrides` to the
    Rust reader. If you ignore them, fused stage transformations are lost from
    the plan. A reader that rejects the keywords raises `TypeError`.

### `rypipe_log/rypipe_adapter.py` { #adapter-py}

The adapter registered with **rypipe** is a plain object with a
`read(path, **kwargs)` method. Registration makes `rypipe.read()` work;
users still interact with `LogSource`:

```python
from typing import Any


class LogAdapter:
    """rypipe-compatible adapter for newline-delimited key=value logs."""

    def read(self, path: str, **kwargs: Any) -> Any:
        """Parse ``path`` and return a ``pyarrow.Table``."""
        from rypipe_log import _rypipe_log

        return _rypipe_log.read_log(path, **kwargs)


def _register() -> None:
    try:
        import rypipe
    except Exception:  # pragma: no cover, rypipe is optional
        return
    rypipe.register_adapter("log", LogAdapter(), extensions=[".log"])


_register()
```

!!! note

    `rypipe.Adapter` is a different thing: a `Source` subclass you
    instantiate per file (`CsvAdapter("data.csv")`), useful as a base for
    sources like `LogSource`. The object passed to `register_adapter` is a
    plain callable adapter like `LogAdapter` above. Complex adapters (like
    [`crxml`](../crxml-adapter.md)) also override `_read_arrow()` instead
    of `read()` to control engine selection and streaming. See
    [Adapter design patterns](../advanced/source-pattern.md) for details.

For the stage and sink re-export pattern (`CastTypes`, `FilterRows`,
`collect`, `to_pandas`, ...), see
[Python Adapter Wiring](python-wiring.md#re-exporting-stages).

## Step 6: Build and test { #step-6-build-and-test}

Build the Rust extension with maturin (release mode, so timings are
meaningful), then test your adapter:

### Build { #build }

```console
$ uv run --with maturin maturin develop --release
📦 Built wheel for abi3 Python ≥ 3.10 to /tmp/.../rypipe_log-0.1.0-cp310-abi3-linux_x86_64.whl
✏️ Setting installed package as editable
🛠 Installed rypipe-log-0.1.0
```

### Test it { #test }

Save this as `try_log.py` and run it:

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
```

```console
$ python try_log.py
pyarrow.Table
name: string
age: string
active: string
----
name: [["Alice","Bob"]]
age: [["30","25"]]
active: [["true","false"]]
```

Pattern 2 uses the Source directly with the pipeline `|` operator; fused
stages are pushed into the Rust parse:

```python
from rypipe_log import to_pandas, LogSource, CastTypes, FilterRows

src = LogSource("test.log")
df = to_pandas(
    src
    | CastTypes({"age": int})
    | FilterRows(field="active", op="==", value="true")
)
print(df)
```

```console
    name  age active
0  Alice   30   true
```

Pattern 2 is the recommended approach. It gives you the pipeline `|`
operator, caching, and streaming. Pattern 1 (`rypipe.read()`) is a
convenience for one-liner reads but does not support pipelines. Note that
`age` came back as a real integer and only Alice survived the filter:
both stages ran inside the Rust parse, not in Python afterward.

## What the adapter did { #what-just-happened}

1. **Splitter** found newline boundaries in the file.
2. **RecordParser** parsed each chunk, calling `sink.put_field` for each
   field in each row.
3. **Engine** accumulated values into Arrow columns.
4. **Export** produced a `pyarrow.Table`; string and dictionary buffers move
   without copying, while primitive arrays are copied into Arrow buffers.

!!! tip

    Pass `schema=["name", "age", "active"]` and
    `field_types={"age": "int64"}` when constructing `LogSource` to skip
    column discovery and emit typed Arrow arrays directly. This alone can
    boost throughput by +80% on projection workloads. See
    [Schema](schema.md).

## Next steps { #next-steps}

* [Python Adapter Wiring](python-wiring.md), stage and sink re-exports,
  adapter kwargs, engine selection, and streaming
* [Rust Creation](rust-creation.md), details on
  Splitter, RecordParser, and ColumnarSink
* [Schema](schema.md), declare columns for maximum
  performance
* [Pipeline](../tutorial/pipeline.md#plans), how plan fusion works (user
  perspective)

## Recap { #recap }

* An adapter is a Rust crate (Splitter + RecordParser) and a Python package
  (Source + stages + sinks).
* The engine handles parallel execution, memory management, and Arrow export.
* Users import everything from the adapter package, never from **rypipe**
  directly.
* `rypipe.read("file.log")` works via the registered adapter.
* `LogSource("file.log") | CastTypes(...) | FilterRows(...)` works via the
  Source pipeline.
