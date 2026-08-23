# rypipe examples

This page shows common patterns in Python and Rust. All Python examples assume a registered adapter is installed (for example `pip install crxml` for XML). `rypipe` itself does not ship format parsers.

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

All Rust examples use the `rypipe-core` crate.

### Define a custom record parser

```rust
use rypipe_core::{RecordParser, Value, Error};

#[derive(Clone, Default)]
struct LogParser;

impl RecordParser for LogParser {
    fn parse_record(&self, bytes: &[u8]) -> Result<Vec<Value>, Error> {
        // Split a line like "key=value,key2=value2" into fields.
        let mut values = Vec::new();
        for part in bytes.split(|b| *b == b',') {
            let kv: Vec<_> = part.splitn(2, |b| *b == b'=').collect();
            if kv.len() == 2 {
                values.push(Value::String(std::str::from_utf8(kv[1])?.into()));
            }
        }
        Ok(values)
    }
}
```

### Run a single-threaded columnar parse

```rust
use rypipe_core::{ExecutionPlan, TableBuilder};
use std::path::Path;

fn parse_file(path: &Path) -> arrow::record_batch::RecordBatch {
    let plan = ExecutionPlan::default()
        .with_field_types([("amount".into(), "float64".into())])
        .with_drop_fields(["internal_id".into()]);

    let bytes = std::fs::read(path).unwrap();
    let parser = LogParser;
    let records = split_records(&bytes); // adapter-specific splitting
    let builder = TableBuilder::new(plan);
    for record in records {
        builder.push(&parser.parse_record(record).unwrap()).unwrap();
    }
    builder.finish().unwrap()
}
```

### Parallel chunked parse

```rust
use rypipe_core::{Pipeline, ParallelConfig};
use std::path::Path;

fn parse_parallel(path: &Path, chunks: usize) -> arrow::record_batch::RecordBatch {
    let config = ParallelConfig::default()
        .chunks(chunks)
        .memory_budget(512 * 1024 * 1024);

    Pipeline::new(path, LogParser, config)
        .run()
        .unwrap()
}
```

### Export a RecordBatch to PyArrow

```rust
use rypipe_python::{record_batch_to_pyarrow, py_err_from_rypipe};
use pyo3::prelude::*;

#[pyfunction]
fn read_log(py: Python, path: &str) -> PyResult<PyObject> {
    let batch = parse_file(path.into());
    record_batch_to_pyarrow(py, &batch).map_err(py_err_from_rypipe)
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
