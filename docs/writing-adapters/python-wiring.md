# Python Adapter Wiring

This page explains how to wire your Rust adapter to Python using PyO3, and
how to expose it through `rypipe.Adapter` or `rypipe.Source` for full
pipeline support.

## Overview

A rypipe adapter has two layers:

1. **Rust layer**: `Splitter` + `RecordParser` (the parsing logic)
2. **Python layer**: Registration with `rypipe` so users can call
   `rypipe.read()` or use pipeline syntax

The Python layer is thin. It wraps the Rust extension and registers the
adapter. Users never see the Rust code.

## Two approaches

### Approach 1: Minimal adapter (no pipeline support)

The simplest adapter exposes a `read(path, **kwargs)` method that returns
a `pyarrow.Table`. Users call `rypipe.read("file.ext")`.

```python
import rypipe
import _rypipe_log


class LogAdapter:
    def read(self, path, **kwargs):
        return _rypipe_log.read_log(path, **kwargs)


rypipe.register_adapter("log", LogAdapter(), extensions=[".log"])
```

Users:

```python
import rypipe
import rypipe_log  # registers the adapter

table = rypipe.read("app.log")
```

### Approach 2: Source subclass (pipeline support)

Subclass `rypipe.Source` to get the pipeline `|` operator, fusion, and
all sinks (`.to_arrow()`, `.to_pandas()`, `.to_parquet()`, etc.).

```python
import rypipe
from rypipe import Source
import _rypipe_log


class LogSource(Source):
    def _read_arrow(self, *, plan_overrides=None, **kwargs):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return _rypipe_log.read_log(str(self._path), **plan)


rypipe.register_adapter("log", LogSource, extensions=[".log"])
```

Users:

```python
from rypipe_log import LogSource
from rypipe import RenameFields, DropFields, CastTypes, FilterRows

src = LogSource("app.log")

# Pipeline syntax
table = (
    src
    | RenameFields({"name": "user_name"})
    | CastTypes({"age": int})
    | FilterRows(field="active", op="==", value="true")
).to_arrow()

# Or direct methods
df = src.to_pandas()
src.to_parquet("out.parquet")
```

## `rypipe.Source` vs `rypipe.Adapter`

| Feature | `Source` | `Adapter` |
|---------|----------|-----------|
| Pipeline `\|` operator | Yes | Yes |
| Fusion (stages in Rust) | Yes (with `_read_arrow`) | No (stages fall back to Python) |
| `.to_arrow()`, `.to_pandas()` | Yes | Yes |
| `.iter_arrow_batches()` | Yes | Yes |
| Complexity | More (must handle `plan_overrides`) | Less (just `read`) |

**Choose `Adapter` if:**

- You want the simplest possible code
- Your format does not benefit from fusion (e.g., pure Python parsing)
- You are prototyping

**Choose `Source` if:**

- You want maximum performance (fusion pushes stages into Rust)
- You need streaming batches
- You want full pipeline support

## How `_read_arrow` works

When a user writes `src | RenameFields(...) | FilterRows(...)`, the pipeline
stages are collected into a plan. When `to_arrow()` is called, the pipeline
calls `_read_arrow(plan_overrides=...)` on your source.

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
Rust reader:

```python
def _read_arrow(self, *, plan_overrides=None, **kwargs):
    plan = self._build_plan_kwargs()  # kwargs from __init__
    if plan_overrides:
        plan.update(plan_overrides)   # fused stages override
    return my_rust_read(str(self._path), **plan)
```

**If you ignore `plan_overrides`**, fused stages silently fall back to
Python execution over a full table. This is a common performance bug.

## Registration

`register_adapter` tells rypipe about your adapter:

```python
rypipe.register_adapter(
    "log",           # name: used for format="log" lookups
    LogAdapter(),    # adapter: object with read() method
    extensions=[".log"],  # extensions: for auto-detection
)
```

After registration:

- `rypipe.read("file.log")` auto-detects the `.log` extension
- `rypipe.read("file.log", format="log")` works explicitly
- `rypipe.read("file.txt", format="log")` works with explicit format

### When to register

Register at module load time. Users should get the adapter by importing
your package:

```python
import rypipe_log  # triggers registration
table = rypipe.read("app.log")
```

Or the user can pass your adapter directly:

```python
from rypipe_log import LogAdapter
table = rypipe.read("app.log", adapter=LogAdapter())
```

## Python wrapper patterns

### Pattern 1: Delegating to Rust

```python
class LogAdapter:
    def read(self, path, **kwargs):
        return _rypipe_log.read_log(path, **kwargs)
```

The Rust function handles everything. The Python wrapper is one line.

### Pattern 2: Adding Python-side logic

```python
class LogAdapter:
    def read(self, path, **kwargs):
        # Add Python-side validation or preprocessing
        if not path.endswith(".log"):
            raise ValueError("Expected .log file")

        # Delegate to Rust
        return _rypipe_log.read_log(path, **kwargs)
```

### Pattern 3: Multiple entry points

```python
class LogSource(Source):
    def _read_arrow(self, *, plan_overrides=None, **kwargs):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return _rypipe_log.read_log(str(self._path), **plan)

    # Add custom methods specific to your format
    def preview(self, n=5):
        """Return first n rows as a list of dicts."""
        table = self.to_arrow()
        return [dict(zip(table.column_names, row)) for row in zip(*[col.to_pylist() for col in table.columns])][:n]
```

## PyO3 integration

### Passing kwargs from Python to Rust

In your Rust code, accept kwargs as a `HashMap` or individual parameters:

```rust
use pyo3::prelude::*;
use std::collections::HashMap;

#[pyfunction]
fn read_log(path: String, kwargs: Option<HashMap<String, PyObject>>) -> PyResult<PyArrowTable> {
    let mut plan = ExecutionPlan::new();

    // Process kwargs
    if let Some(kw) = kwargs {
        if let Some(Some(types)) = kw.get("field_types") {
            // Parse field_types from Python dict
        }
        if let Some(Some(schema)) = kw.get("schema") {
            // Parse schema from Python list
        }
    }

    let table = Pipeline::new(LogSplitter::new(), LogParser::new())
        .with_plan(plan)
        .read_path(&path, false, false)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    Ok(table.into())
}
```

### Using `execution_plan_from_kwargs`

For complex adapters, use the `rypipe_python` helper:

```rust
use rypipe_python::execution_plan_from_kwargs;

#[pyfunction]
fn read_log(path: String, kwargs: HashMap<String, PyObject>) -> PyResult<PyArrowTable> {
    let plan = execution_plan_from_kwargs(&kwargs)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    let table = Pipeline::new(LogSplitter::new(), LogParser::new())
        .with_plan(plan)
        .read_path(&path, false, false)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    Ok(table.into())
}
```

This handles all standard plan kwargs (`field_types`, `schema`, `filter`,
`drop_fields`, `field_mapping`, `dictionary_columns`, etc.) automatically.

## Error handling

### Rust side

```rust
use rypipe_core::Error;

fn parse_line(line: &str) -> Result<(&str, &str)> {
    line.split_once('=')
        .ok_or_else(|| Error::Plan(format!("invalid line: {line}")))
}
```

### Python side

```python
import rypipe

try:
    table = rypipe.read("bad_file.log")
except rypipe.ParseError as e:
    print(f"Parse error: {e}")
except rypipe.PlanError as e:
    print(f"Invalid plan: {e}")
except rypipe.RypipeError as e:
    print(f"API error: {e}")
```

## Testing

### Unit test the Rust parser

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_row() {
        let parser = LogParser;
        let mut sink = MockSink::new();
        let bytes = b"name=Alice,age=30";
        parser.parse_chunk(bytes, &mut sink).unwrap();
        assert_eq!(sink.rows.len(), 1);
        assert_eq!(sink.rows[0]["name"], "Alice");
    }
}
```

### Integration test the Python adapter

```python
import rypipe
import rypipe_log

def test_read_returns_table(tmp_path):
    p = tmp_path / "test.log"
    p.write_text("name=Alice,age=30\nname=Bob,age=25\n")
    table = rypipe.read(str(p))
    assert table.num_rows == 2
    assert "name" in table.column_names

def test_pipeline_stages(tmp_path):
    from rypipe import CastTypes, FilterRows
    p = tmp_path / "test.log"
    p.write_text("name=Alice,age=30\nname=Bob,age=25\n")
    src = rypipe_log.LogSource(p)
    result = (
        src
        | CastTypes({"age": int})
        | FilterRows(field="age", op=">", value="25")
    ).to_arrow()
    assert result.num_rows == 1
```

## Common mistakes

1. **Forgetting to register**: Users get "no adapter registered" error
2. **Ignoring `plan_overrides`**: Fused stages fall back to Python (slow)
3. **Not returning `pyarrow.Table`**: `read()` must return a table, not dicts
4. **Missing `extensions`**: Auto-detection from file extension does not work

## Package structure

### Minimal package

```
my-adapter/
├── Cargo.toml
├── pyproject.toml
├── src/
│   └── lib.rs
└── my_adapter/
    └── __init__.py
```

### `pyproject.toml`

```toml
[build-system]
requires = ["maturin>=1.0"]
build-backend = "maturin"

[project]
name = "my-adapter"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = ["rypipe"]

[tool.maturin]
features = ["pyo3/extension-module"]
python-source = "my_adapter"
module-name = "my_adapter._core"
```

### Building for distribution

```bash
# Build wheel
maturin build --release

# Build for specific Python version
maturin build --release --interpreter python3.12

# Publish to PyPI
maturin publish
```

## Schema integration

### Accepting schema kwargs

Your adapter should accept `schema` and `field_types` kwargs and pass them
to the Rust reader:

```python
class LogSource(Source):
    def _read_arrow(self, *, plan_overrides=None, **kwargs):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        # plan now contains schema, field_types, etc.
        return _rypipe_log.read_log(str(self._path), **plan)
```

### Passing schema to Rust

In your Rust code, accept these kwargs:

```rust
use std::collections::HashMap;

#[pyfunction]
fn read_log(
    path: String,
    schema: Option<Vec<String>>,
    field_types: Option<HashMap<String, String>>,
    kwargs: Option<HashMap<String, PyObject>>,
) -> PyResult<PyArrowTable> {
    let mut plan = ExecutionPlan::new();

    // Apply schema_order
    if let Some(names) = schema {
        plan.schema_order = names;
    }

    // Apply field_types
    if let Some(types) = field_types {
        for (name, type_str) in &types {
            if let Some(ft) = FieldType::from_str(type_str) {
                plan.field_types.insert(name.clone(), ft);
            }
        }
    }

    // ... rest of implementation
}
```

### Using execution_plan_from_kwargs

For simpler code, use the `rypipe_python` helper:

```rust
use rypipe_python::execution_plan_from_kwargs;

#[pyfunction]
fn read_log(path: String, kwargs: HashMap<String, PyObject>) -> PyResult<PyArrowTable> {
    let plan = execution_plan_from_kwargs(&kwargs)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    // ... use plan ...
}
```

This handles all standard kwargs automatically.

## Dictionary encoding

### Accepting dictionary kwargs

Your adapter should accept `dictionary_columns` and `auto_dict` kwargs:

```python
class LogSource(Source):
    def _read_arrow(self, *, plan_overrides=None, **kwargs):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        # kwargs may contain dictionary_columns, auto_dict, etc.
        return _rypipe_log.read_log(str(self._path), **plan)
```

### Passing dictionary options to Rust

```rust
#[pyfunction]
fn read_log(
    path: String,
    dictionary_columns: Option<Vec<String>>,
    auto_dict: Option<bool>,
    kwargs: Option<HashMap<String, PyObject>>,
) -> PyResult<PyArrowTable> {
    let mut plan = ExecutionPlan::new();

    // Apply dictionary_columns
    if let Some(cols) = dictionary_columns {
        for col in &cols {
            plan.dictionary_columns.insert(col.clone());
        }
    }

    // Apply auto_dict
    if let Some(auto) = auto_dict {
        plan.auto_dict = auto;
    }

    // ... rest of implementation
}
```

## Streaming

### Implementing batch iteration

To support `iter_record_batches`, implement it in your adapter:

```python
class LogSource(Source):
    def _read_arrow(self, *, plan_overrides=None, **kwargs):
        # ... same as before ...

    def iter_record_batches(self, *, memory="64MiB", batch_size=None, **kwargs):
        plan = self._build_plan_kwargs()
        return _rypipe_log.iter_batches(
            str(self._path),
            memory=memory,
            batch_size=batch_size,
            **plan,
        )
```

### Rust-side batch iteration

```rust
use pyo3::iter::IterNextOutput;

#[pyclass]
struct BatchIterator {
    inner: rypipe_core::StreamingBatchIterator,
}

#[pymethods]
impl BatchIterator {
    fn __next__(&mut self) -> IterNextOutput<PyArrowRecordBatch, PyObject> {
        match self.inner.next() {
            Some(Ok(batch)) => IterNextOutput::Yield(batch.into()),
            Some(Err(e)) => IterNextOutput::Return(
                Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())).into()
            ),
            None => IterNextOutput::Return(Err(pyo3::exceptions::PyStopIteration::new_err(()).into())),
        }
    }
}
```

## Fusion in detail

### How fusion works

When a user writes:

```python
src = LogSource("data.log")
result = src | RenameFields({"name": "user_name"}) | FilterRows(field="age", op=">", value="25")
```

The pipeline stages are collected into a plan. When `to_arrow()` is called,
the pipeline calls `_read_arrow(plan_overrides=...)` on your source.

`plan_overrides` contains the fused stage kwargs:

```python
{
    "field_mapping": {"name": "user_name"},
    "filter": {"field": "age", "op": ">", "value": "25"},
}
```

### Forwarding plan_overrides

Always forward `plan_overrides` to your Rust reader:

```python
def _read_arrow(self, *, plan_overrides=None, **kwargs):
    plan = self._build_plan_kwargs()
    if plan_overrides:
        plan.update(plan_overrides)
    return _rypipe_log.read_log(str(self._path), **plan)
```

### What happens if you ignore plan_overrides

If you ignore `plan_overrides`, the stages fall back to Python execution:

```python
# Bad: ignores plan_overrides
def _read_arrow(self, **kwargs):
    return _rypipe_log.read_log(str(self._path))

# Result: RenameFields and FilterRows run in Python over a full table
# This is 10-50x slower than fused execution
```

### What happens if you forward plan_overrides

If you forward `plan_overrides`, the stages are pushed into Rust:

```python
# Good: forwards plan_overrides
def _read_arrow(self, *, plan_overrides=None, **kwargs):
    plan = self._build_plan_kwargs()
    if plan_overrides:
        plan.update(plan_overrides)
    return _rypipe_log.read_log(str(self._path), **plan)

# Result: RenameFields and FilterRows run in the Rust parse loop
# This is 10-50x faster than Python execution
```

## Advanced: Source with custom methods

You can add custom methods to your Source:

```python
class LogSource(Source):
    def _read_arrow(self, *, plan_overrides=None, **kwargs):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return _rypipe_log.read_log(str(self._path), **plan)

    def preview(self, n=5):
        """Return first n rows as a list of dicts."""
        table = self.to_arrow()
        return [
            dict(zip(table.column_names, row))
            for row in zip(*[col.to_pylist() for col in table.columns])
        ][:n]

    def field_names(self):
        """Return the list of field names in the file."""
        return _rypipe_log.list_fields(str(self._path))

    def sample(self, n=100):
        """Return a sample of n rows for inspection."""
        return _rypipe_log.sample_rows(str(self._path), n)
```

## See also

- [Rust adapter creation](./rust-creation.md): Deep dive into the Rust traits
- [Schema](./schema.md): Declare column names for maximum performance
- [Techniques](./techniques.md): Performance optimizations
- [Examples](./examples.md): Worked CSV, JSONL, and TSV adapters
