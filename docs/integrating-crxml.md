# Integrating with crxml

`crxml` was the first consumer of `rypipe`. Its Rust crate now contains only:

- The streaming `CrxmlReader` / `RowParser` engine (Crystal-specific).
- Thin FFI wrappers that call into `rypipe-core` and `rypipe-xml` for the
  columnar paths.

This guide explains how the integration works so you can do the same in your
own project.

## Cargo setup

In `src/crxml_core/Cargo.toml`:

```toml
[dependencies]
rypipe-core = { path = "../../../rypipe/crates/rypipe-core" }
rypipe-xml = { path = "../../../rypipe/crates/rypipe-xml" }

pyo3 = { version = "0.24", features = ["extension-module"] }
quick-xml = "0.36"        # still needed for the stream engine
arrow = { version = "=55.2.0", default-features = false, features = ["pyarrow"] }
rustc-hash = "2"
mimalloc = { version = "0.1", default-features = false }
```

## Keeping the public Python API unchanged

crxml kept the same four `#[pyfunction]` signatures and the same exception
names (`XmlError`, `PlanError`, `MergeError`). The bodies changed from inline
engine code to rypipe calls.

Example: `read_to_columnar`

```rust
#[pyfunction]
fn read_to_columnar(
    py: Python<'_>,
    path: String,
    row_tag: Option<String>,
    // ... same kwargs as before
) -> PyResult<PyObject> {
    let plan = build_plan_from_kwargs(...)?;
    let p = Path::new(&path);
    let row_tag = row_tag.unwrap_or_else(|| "Row".to_string()).into_bytes();

    let batch = py.allow_threads(|| -> PyResult<RecordBatch> {
        let input = rypipe_core::InputBuffer::open(p, use_mmap, prefault)
            .map_err(|e| PyIOError::new_err(...))?;
        let mut engine = rypipe_core::TableBuilder::with_plan(
            (input.as_slice().len() / 512).max(64),
            plan.clone(),
        );
        let decoder = rypipe_xml::CrystalXmlDecoder::with_row_tag(&row_tag);
        decoder.validate(input.as_slice()).map_err(...)?;
        decoder.parse_chunk(input.as_slice(), &mut engine).map_err(...)?;
        let mut batch = engine.finish().map_err(...)?;
        if let Some(ref filter) = plan.filter {
            batch = rypipe_core::apply_compare_filter(batch, filter).map_err(...)?;
        }
        Ok(batch)
    })?;

    export_record_batch_to_pyarrow(py, batch)
}
```

`build_plan_from_kwargs` can be reimplemented locally for exact error-message
compatibility, or you can reuse `rypipe_python::plan_kwargs::execution_plan_from_kwargs`.

## Export helper

crxml already depends on `arrow` with the `pyarrow` feature, so exporting a
`RecordBatch` is a one-liner:

```rust
use arrow::pyarrow::ToPyArrow;

fn export_record_batch_to_pyarrow(py: Python<'_>, batch: RecordBatch) -> PyResult<PyObject> {
    batch.to_pyarrow(py)
}
```

For multiple batches, use `pyarrow.concat_tables` via Python or reuse
`rypipe_python::export::record_batches_to_pyarrow_table`.

## Parallel and bounded wrappers

For `read_to_columnar_par`, call `rypipe_core::ParallelExecutor::parse`:

```rust
let batches = rypipe_core::parallel::ParallelExecutor::parse(
    bytes,
    &rypipe_xml::CrystalXmlSplitter::with_row_tag(&row_tag),
    rypipe_xml::CrystalXmlDecoder::with_row_tag(&row_tag),
    plan,
    num_chunks,
)?;
```

For `read_to_columnar_bounded`, call `rypipe_core::bounded::BoundedExecutor`:

```rust
let batches = rypipe_core::bounded::BoundedExecutor::new(
    rypipe_core::bounded::MemoryBudget::new(memory_bytes),
)
.run(path, &splitter, decoder, plan, prefault)?;
```

## Error mapping

Map `rypipe_core::Error` variants to your Python exceptions:

```rust
fn map_err(e: rypipe_core::Error) -> PyErr {
    match e {
        rypipe_core::Error::Utf8(_) | rypipe_core::Error::Plan(s) if s.contains("XML") =>
            XmlError::new_err(s),
        rypipe_core::Error::Plan(s) => PlanError::new_err(s),
        rypipe_core::Error::Merge(s) => MergeError::new_err(s),
        rypipe_core::Error::Io(e) => PyIOError::new_err(e.to_string()),
        rypipe_core::Error::Arrow(e) => pyo3::exceptions::PyException::new_err(e.to_string()),
    }
}
```

## Why keep the stream engine separate?

The streaming `CrxmlReader` returns `list[dict[str, str]]` and releases the GIL
in batches. It is tightly coupled to Crystal Reports XML row semantics and is
not part of the columnar engine, so it stayed in crxml.

## Verification

After the integration, crxml's full pytest suite should pass:

```bash
export PYO3_PYTHON=/path/to/python3.12
cd /path/to/crxml
maturin develop --release
python -m pytest tests/
```

The differential tests (`test_differential.py`) compare the columnar/multi/par/
bounded paths against an independent ElementTree oracle, confirming that the
rypipe-backed engine produces the same results as the original crxml engine.

## See also

- [Architecture](./architecture.md): how rypipe is structured.
- [Rust API](./rust-api.md): using `Pipeline` or low-level executors.
- [Python API](./python-api.md): the public Python bindings.
