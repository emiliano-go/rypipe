# rypipe examples

This page shows common patterns in Python and Rust. All Python examples assume a registered adapter is installed (for example `pip install crxml` for XML). `rypipe` itself does not ship format parsers.

Legend: `(ADAPTER BOUND)` is code you write in your adapter crate (format specific). `(CORE)` is code in `rypipe` crates (reused). The split is the same as in [Architecture](./architecture/).

## Python API examples

### Read a file through a registered adapter

```python
import rypipe

table = rypipe.read("data.xml", format="crxml", row_tag="Row")
print(table.num_rows, table.num_columns)
```

### Chain a pipeline

```python
import rypipe
from rypipe import RenameFields, DropFields, FilterRows, CastTypes

df = (
    rypipe.read("data.xml", format="crxml", row_tag="Row")
    | RenameFields({"old_name": "new_name"})
    | DropFields(["internal_id"])
    | FilterRows(field="status", op="==", value="active")
    | CastTypes({"amount": "float64"})
).to_dataframe()
```

### Build an adapter subclass

```python
import rypipe
import pyarrow as pa

class DummyAdapter(rypipe.Adapter):
    def read(self, path, **kwargs):
        # A real adapter would call a Rust parser here.
        return pa.table({
            "id": [1, 2, 3],
            "name": ["alice", "bob", "carol"],
        })

# Register once per session
rypipe.register_adapter("dummy", DummyAdapter, extensions=[".dummy"])

table = rypipe.read("example.dummy")
```

### Work with a Source object directly

```python
from crxml import CrystalXMLSource

source = CrystalXMLSource("report.xml", row_tag="Row")

# Iterate rows without loading the whole table
for row in source:
    print(row["id"])

# Or export to Arrow and reuse it
arrow = source.to_arrow()
df = source.to_dataframe()
source.to_parquet("report.parquet")
```

### Memory-bounded stream

```python
import rypipe

# Adapter decides how to honor the memory budget.
stream = rypipe.read_stream("huge.xml", format="crxml", memory="256MiB")
for batch in stream.to_batches(max_chunksize=4096):
    process(batch.to_pylist())
```

### Parallel parse

```python
import rypipe

table = rypipe.read_par("large.xml", format="crxml", chunks=16)
```

### Sink to Parquet

```python
import rypipe
from rypipe import to_parquet

pipeline = (
    rypipe.read("data.xml", format="crxml", row_tag="Row")
    | FilterRows(field="status", op="==", value="active")
)
to_parquet(pipeline, "active.parquet")
```

## Rust API examples

All Rust examples use the `rypipe-core` crate. `(ADAPTER BOUND)` parts are the `Splitter` and `RecordParser` you implement; `(CORE)` parts are the engine.

### Define a custom Splitter and RecordParser  (ADAPTER BOUND)

```rust
use rypipe_core::{Splitter, RecordParser, ColumnarSink, Value, Result};

#[derive(Clone, Default)]
pub struct LogSplitter; // (ADAPTER BOUND)

impl Splitter for LogSplitter { // (ADAPTER BOUND)
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize> {
        if max_chunks <= 1 || bytes.is_empty() { return vec![0, bytes.len()]; }
        let mut points = vec![0];
        for (i, &b) in bytes.iter().enumerate().skip(1) {
            if b == b'\n' && points.len() < max_chunks { points.push(i + 1); }
        }
        if *points.last().unwrap() != bytes.len() { points.push(bytes.len()); }
        points
    }
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        (sample.len() / sample.iter().filter(|&&b| b == b'\n').count().max(1)).max(1)
    }
}

#[derive(Clone, Default)]
pub struct LogParser; // (ADAPTER BOUND)

impl RecordParser for LogParser { // (ADAPTER BOUND)
    fn validate(&self, bytes: &[u8]) -> Result<()> { // (ADAPTER BOUND) check UTF-8
        simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
        Ok(())
    }
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> { // (ADAPTER BOUND) emits (CORE) Value events
        let text = std::str::from_utf8(bytes).unwrap();
        for line in text.lines() {
            if line.is_empty() { continue; }
            sink.begin_row(); // (CORE) TableBuilder
            for part in line.split(',') {
                if let Some((k, v)) = part.split_once('=') {
                    if let Some(r) = sink.resolve(k) { // (CORE) one hash
                        sink.put_field_resolved(r, Value::Str(v)); // (CORE) single index lookup plus dirty
                    }
                }
            }
            sink.end_row(); // (CORE) null fill plus filter
        }
        Ok(())
    }
}
```

### Run single-threaded, parallel, and bounded  (CORE) drivers

```rust
use rypipe_core::{ExecutionPlan, FieldType, Pipeline, MemoryBudget}; // (CORE)
use std::path::Path; // (CORE) InputBuffer uses Path

// (CORE) Pipeline wires (ADAPTER BOUND) splitter/parser to (CORE) engine
let pipeline = Pipeline::new(LogSplitter, LogParser).with_plan( // (CORE) plus (ADAPTER BOUND) types
    ExecutionPlan::new().type_as("amount", FieldType::Float64).drop("internal_id")
);

// (CORE) Single thread, whole file
let batch = pipeline.read_path(Path::new("data.log"), false, false)?; // (CORE) via InputBuffer

// (CORE) Parallel (rayon), chunked, fast path when !auto_dict and schemas consistent
let batches = pipeline.read_path_par(Path::new("data.log"), 8, false, false)?; // (CORE)

// (CORE) Bounded memory (streaming), from file or from bytes
let batches = pipeline.read_path_stream(Path::new("huge.log"), MemoryBudget::new(128 * 1024 * 1024), false)?; // (CORE)
let batches = pipeline.read_bytes_stream(data_bytes, MemoryBudget::new(64 * 1024 * 1024))?; // (CORE) no file IO, slicing
```

### Low level direct TableBuilder  (CORE)

```rust
use rypipe_core::{ExecutionPlan, TableBuilder, ColumnarSink, Value}; // (CORE)
use std::path::Path; // (CORE)

let plan = ExecutionPlan::new().drop("internal_id"); // (CORE)
let input = rypipe_core::InputBuffer::open(Path::new("data.log"), false, false)?; // (CORE) handles mmap plus gzip/zstd/lz4
let mut builder = TableBuilder::with_plan(1024, plan); // (CORE)
LogParser.validate(input.as_slice())?; // (ADAPTER BOUND)
LogParser.parse_chunk(input.as_slice(), &mut builder)?; // (ADAPTER BOUND) -> (CORE) sink
let batch = builder.finish()?; // (CORE) normalize, auto_dict, sort, to_arrow
```

### Export a RecordBatch to PyArrow  (CORE) helper plus (ADAPTER BOUND) registration

```rust
use rypipe_python::{execution_plan_from_kwargs, record_batches_to_pyarrow_table}; // (CORE) helper
use pyo3::prelude::*; // (CORE) plus (ADAPTER BOUND) glue

#[pyfunction]
fn read_log(py: Python, path: &str, field_mapping: Option<std::collections::HashMap<String,String>>) -> PyResult<pyo3::Bound<pyo3::PyAny>> {
    let plan = execution_plan_from_kwargs(field_mapping, None, None, None, None, None, false, None, None)?; // (CORE)
    let batches = py.allow_threads(|| { // (CORE) GIL release
        use rypipe_core::{Pipeline, MemoryBudget};
        Pipeline::new(LogSplitter, LogParser).with_plan(plan).read_path_par(path, 4, false, false) // (CORE) plus (ADAPTER BOUND)
    })?;
    record_batches_to_pyarrow_table(py, &batches) // (CORE) C Data Interface
}
```

## Adapter package layout

A minimal adapter package looks like this:

```
rypipe-foo/
├── Cargo.toml          # depends on rypipe-core + rypipe-python
├── pyproject.toml      # maturin build
├── src/
│   └── lib.rs          # Rust parser + PyO3 module
└── rypipe_foo/
    └── __init__.py     # Python adapter + rypipe.register_adapter
```

The Python side:

```python
import rypipe

class FooAdapter(rypipe.Adapter):
    def read(self, path, **kwargs):
        return _rypipe_foo.read_foo(path, **kwargs)

rypipe.register_adapter("foo", FooAdapter, extensions=[".foo"])
```

See [Writing adapters](writing-adapters.md) for the full trait reference.
