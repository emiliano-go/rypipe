# Rust Adapter Creation { #rust-adapter-creation }

This page covers the Rust side of writing a rypipe adapter: implementing
the `Splitter`, `RecordParser`, and understanding how the engine calls them.

It is a deep dive into the two traits you already used in the
[walkthrough](walkthrough.md), and it keeps the same running example: the
`rypipe_log` adapter for lines of comma-separated `key=value` pairs. Every
code block below is taken from that crate, trimmed only for teaching.

## Overview { #overview }

A rypipe adapter implements two traits:

1. **`Splitter`**: Finds safe chunk boundaries for parallel parsing
2. **`RecordParser`**: Parses bytes into field/value events

The engine provides `TableBuilder` as the production `ColumnarSink`. You
rarely implement `ColumnarSink` yourself.

```
Input bytes
  -> Splitter::next_record_start    (find chunk boundaries)
  -> RecordParser::parse_chunk      (per-chunk, feeds ColumnarSink)
  -> ColumnarSink (TableBuilder)    (accumulates typed columns)
  -> Arrow RecordBatch              (zero-copy export)
```

## Splitter trait { #splitter-trait }

```rust
pub trait Splitter: Send + Sync {
    /// Find the start of the next record after byte offset `from`.
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize>;

    /// Estimate bytes per row from a sample of the file.
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize;
}
```

### `next_record_start` { #next-record-start }

Find the byte offset of the next record boundary after `from`. The engine
iterates this to carve the input into chunks. The log format is
newline-delimited, so `LogSplitter` scans for newline characters:

```rust
fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
    memchr::memchr(b'\n', &bytes[from..])          // SIMD-accelerated scan
        .map(|r| from + r + 1)                      // offset past the newline
}
```

### `estimate_bytes_per_row` { #estimate-bytes-per-row }

Return a rough estimate of how many bytes one row consumes. The engine uses
this to size chunks and memory budgets.

```rust
fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
    let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
    (sample.len() / n).max(1)   // guard against division by zero
}
```

Both methods come straight from `LogSplitter` in the walkthrough crate.

## RecordParser trait { #recordparser-trait }

```rust
pub trait RecordParser: Send + Sync {
    /// Validate that the bytes are valid for this format.
    fn validate(&self, bytes: &[u8]) -> Result<()>;

    /// Parse a chunk of bytes into field/value events.
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()>;
}
```

### `validate` { #validate }

Called once per chunk before parsing. Reject invalid input early, as
`LogParser` does:

```rust
fn validate(&self, bytes: &[u8]) -> Result<()> {
    simdutf8::basic::from_utf8(bytes)              // fast SIMD UTF-8 check
        .map_err(rypipe_core::Error::Utf8)?;
    Ok(())
}
```

### `parse_chunk` { #parse-chunk }

This is the hot path. Iterate rows, call `sink.begin_row()` / `put_field()`
/ `sink.end_row()` for each record. Here is the real `LogParser` body:

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

    for line in text.lines() {
        if line.is_empty() { continue; }
        sink.begin_row();
        for part in line.split(',') {
            if let Some((key, value)) = part.split_once('=') {
                if sink.wants(key) {                // skip dropped fields
                    sink.put_field(key, Value::Str(Cow::Borrowed(value)));
                }
            }
        }
        sink.end_row();
    }
    Ok(())
}
```

Always check `sink.wants(name)` before scanning a field's value. Skipping
dropped fields saves significant CPU.

!!! tip

    For maximum performance, implement `parse_chunk_generic` which takes a
    monomorphized sink instead of a trait object. The compiler can then inline
    every `begin_row`/`put_field`/`end_row` call, eliminating vtable dispatch
    (5-10% improvement on the hot path). This is what the engine calls on
    every path (columnar, parallel, and streaming), and it is what
    [**crxml**](../crxml-adapter.md) implements. See
    [Parser: parse_chunk_generic](parser.md#parse_chunk_generic) for the full
    treatment and [Adapter design](../advanced/adapter-design.md#parse_chunk_generic)
    for how the engine drives it.


## Value types { #value-types }

`Value` variants: `Str(Cow<str>)` (default for text), `Int64(i64)`,
`Float64(f64)`, `Bool(bool)`, `Date32(i32)` (days since epoch),
`Timestamp(i64)`, `Null` (explicit missing).

There is deliberately no `Decimal128` variant. Decimal columns are fed with
`Str` (or `Int64`) values plus a declared `decimal128(N)` field type, and
the column builder parses and scales them into `i128` internally
(`parse_decimal128`: signs and fractional parts handled, extra digits
truncated at scale `N`). The same pattern applies to dates and timestamps
when you declare the type instead of emitting `Date32`/`Timestamp`
directly.

Always prefer `Cow::Borrowed` when the value is a slice of the input.
Only use `Cow::Owned` when you must modify the value (e.g., unescape HTML
entities or normalize encoding).

!!! note

    `Cow::Borrowed` is safe because the engine copies bytes into Arrow arrays
    before your parse function returns. The borrowed reference never outlives
    the chunk's byte slice: no lifetime issues.


## The ColumnarSink interface { #columnsink-interface }

The engine provides `TableBuilder` as the production `ColumnarSink`. Key
methods: `begin_row()`, `put_field(name, value)`, `end_row()`, `wants(name)`,
`resolve(name)`, `put_field_resolved(resolved, value)`, `finish()`.

Fields can be pushed in any order. The engine handles column reordering.

### `wants()` vs `resolve()` + `put_field_resolved()` { #wants-vs-resolve }

```rust
// Simpler: two hash probes per field
if sink.wants(name) { sink.put_field(name, value); }

// Faster: single hash probe: resolve returns the resolved column name
if let Some(resolved) = sink.resolve(name) {
    sink.put_field_resolved(resolved, value);
}
```

Use `resolve` in performance-critical parsers.

## Complete example: the log parser { #log-parser }

This is the full Rust adapter from the walkthrough crate, minus the Python
bindings (covered below):

```rust
use std::borrow::Cow;
use rypipe_core::{Splitter, RecordParser, ColumnarSink, Value, Result};

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
            .map_err(rypipe_core::Error::Utf8)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

        for line in text.lines() {
            if line.is_empty() { continue; }
            sink.begin_row();
            for part in line.split(',') {
                if let Some((key, value)) = part.split_once('=') {
                    if sink.wants(key) {
                        sink.put_field(key, Value::Str(Cow::Borrowed(value)));
                    }
                }
            }
            sink.end_row();
        }
        Ok(())
    }
}
```

The splitter finds newline boundaries; the parser iterates lines, splits
each into `key=value` pairs, and feeds values into the sink. Field names
come from the data itself, so no header handling is needed. That is the
main contrast with CSV, where a parser must also consume a header row and
map positions to names (and where quoted fields containing newlines call
for [skip regions](skip-regions.md)).

## Common patterns { #common-patterns }

### XML with namespace handling { #xml-namespace-pattern }

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
    let cleaned = text.replace("tns:", "").replace("xs:", "");  // strip ns prefixes

    for line in cleaned.lines() {
        if line.contains(&self.row_tag) {
            sink.begin_row();
            self.parse_xml_row(line, sink)?;
            sink.end_row();
        }
    }
    Ok(())
}
```

### Skip regions (comments, CDATA) { #skip-regions-pattern }

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let regions = self.find_skip_regions(bytes);  // pre-compute byte ranges to skip

    for line in bytes.split(|&b| b == b'\n') {
        if self.in_skip_region(line.as_ptr() as usize, &regions) { continue; }
        sink.begin_row();
        self.parse_line(line, sink)?;
        sink.end_row();
    }
    Ok(())
}
```

### Typed values { #typed-values-pattern }

The log adapter emits everything as `Str`. If you know a field's type up
front, emit a typed value instead:

```rust
fn parse_field(&self, name: &str, value: &str, sink: &mut dyn ColumnarSink) {
    match name {
        "age" => {
            if let Ok(n) = value.parse::<i64>() { sink.put_field(name, Value::Int64(n)); }
        }
        "score" => {
            if let Ok(f) = value.parse::<f64>() { sink.put_field(name, Value::Float64(f)); }
        }
        "active" => {
            sink.put_field(name, Value::Bool(matches!(value, "true" | "1" | "yes")));
        }
        _ => { sink.put_field(name, Value::Str(Cow::Borrowed(value))); }
    }
}
```

Emitting typed values lets the engine produce Arrow columns with the correct
data type rather than converting everything to strings.

## Testing { #testing }

### Unit test the parser { #unit-testing }

These are two of the real tests from the walkthrough crate, using
`TableBuilder` as the sink:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use rypipe_core::{ExecutionPlan, TableBuilder};
    use std::sync::Arc;

    const SAMPLE: &[u8] = b"name=Alice,age=30,active=true\nname=Bob,age=25,active=false\n";

    #[test]
    fn splitter_finds_row_starts() {
        let s = LogSplitter;
        assert_eq!(s.next_record_start(SAMPLE, 0), Some(30));
        assert_eq!(s.next_record_start(SAMPLE, 30), Some(SAMPLE.len()));
        assert_eq!(s.next_record_start(SAMPLE, SAMPLE.len()), None);
    }

    #[test]
    fn parser_emits_all_rows() {
        let plan: Arc<ExecutionPlan> = Arc::new(ExecutionPlan::new());
        let mut builder = TableBuilder::with_plan(1024, plan);
        LogParser.validate(SAMPLE).unwrap();
        LogParser.parse_chunk(SAMPLE, &mut builder).unwrap();
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
    }
}
```

`finish()` returns a single `RecordBatch`; assert on its shape and schema.

### Integration test with Python { #python-integration }

```python
def test_log_adapter(tmp_path):
    import rypipe, rypipe_log
    p = tmp_path / "test.log"
    p.write_text("name=Alice,age=30,active=true\nname=Bob,age=25,active=false\n")
    table = rypipe.read(str(p))
    assert table.num_rows == 2
    assert table.column("name").to_pylist() == ["Alice", "Bob"]
```

!!! warning

    Do not allocate `String` objects in the hot path. Every `Cow::Owned`
    allocation costs ~100 ns. For 10 million fields, that's 1 second of pure
    allocation overhead: easily avoidable with `Cow::Borrowed`.


## Error handling { #error-handling }

Use `rypipe_core::Error` variants with line numbers for clear diagnostics.
This is `LogParser::parse_chunk` with per-line error context added:

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Plan(format!("invalid UTF-8: {e}")))?;

    for (i, line) in text.lines().enumerate() {
        if line.is_empty() { continue; }
        sink.begin_row();
        for part in line.split(',') {
            let (key, val) = part.split_once('=').ok_or_else(|| {
                rypipe_core::Error::Plan(format!("line {i}: missing '=' in field: {part}"))
            })?;
            if sink.wants(key) {
                sink.put_field(key, Value::Str(Cow::Borrowed(val)));
            }
        }
        sink.end_row();
    }
    Ok(())
}
```

For formats with mixed record quality, skip bad rows with `continue`.

## Performance considerations { #performance }

Every `Cow::Owned` allocation costs ~100 ns. For 10 million fields, that's
1 second. Use `Cow::Borrowed` whenever the value is a slice of the input.

The `memchr` crate uses AVX2 on x86_64 and NEON on ARM. Always use it for
byte searching instead of scalar loops:

```rust
let pos = memchr::memchr(b'<', bytes);   // Good: SIMD-accelerated
let pos = bytes.iter().position(|&b| b == b'<');  // Bad: 5-10x slower
```

## PyO3 bindings { #pyo3-bindings }

The walkthrough crate exposes a single `read_log` function instead of
Python classes. It receives the merged pushdown plan from the Python side,
builds an `ExecutionPlan`, runs the `Pipeline`, and exports the batch as a
`pyarrow.Table`. Trimmed to the essentials (the real crate also handles
`filter` and `schema` kwargs the same way):

```rust
use arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use rypipe_core::{ExecutionPlan, FieldType, Pipeline};
use std::collections::HashMap;

#[pyfunction]
#[pyo3(signature = (path, field_mapping=None, drop_fields=None, field_types=None,
                    auto_dict=false, use_mmap=false, prefault=false))]
fn read_log(
    path: String,
    field_mapping: Option<HashMap<String, String>>,
    drop_fields: Option<Vec<String>>,
    field_types: Option<HashMap<String, String>>,
    auto_dict: bool,
    use_mmap: bool,
    prefault: bool,
) -> PyResult<Py<PyAny>> {
    let mut plan = ExecutionPlan::new();
    if let Some(map) = field_mapping {
        plan.field_map = map.into_iter().collect();
    }
    if let Some(drop) = drop_fields {
        plan.drop_fields = drop.into_iter().collect();
    }
    plan.auto_dict = auto_dict;
    if let Some(ft) = field_types {
        for (name, type_str) in ft {
            let ft = FieldType::from_str(&type_str).ok_or_else(|| {
                PyValueError::new_err(format!("unknown field type '{type_str}' for '{name}'"))
            })?;
            plan.field_types.insert(name, ft);
        }
    }

    let batch = Pipeline::new(LogSplitter, LogParser)
        .with_plan(plan)
        .read_path(&path, use_mmap, prefault)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    Python::with_gil(|py| {
        let pa = PyModule::import(py, "pyarrow")?;
        let rb = batch.to_pyarrow(py)?;
        let table = pa
            .getattr("Table")?
            .call_method1("from_batches", (vec![rb],))?;
        Ok(table.into())
    })
}

#[pymodule]
fn _rypipe_log(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_log, m)?)?;
    Ok(())
}
```

Build with `maturin develop`. Python calls `read_log` with plain kwargs;
the hot path stays entirely in Rust. Note the export goes through
`arrow::pyarrow::ToPyArrow` with the `arrow` version `rypipe-core` pins,
not through `rypipe-python`: see the [walkthrough](walkthrough.md) for the
full `Cargo.toml`.

## Build and test { #build-and-test }

Run the crate's unit tests with `cargo test`, then rebuild the extension
and run the Python integration test. This is the output from the log
adapter built in the [walkthrough](walkthrough.md):

```console
$ cargo test
   Compiling rypipe-log v0.1.0 (/tmp/rydoc/rypipe-log)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 2.07s
     Running unittests src/lib.rs (target/debug/deps/_rypipe_log-02abd61066597acf)

running 10 tests
test tests::parser_rejects_invalid_utf8 ... ok
test tests::splitter_estimates_bytes_per_row ... ok
test tests::chunk_planning_respects_floor ... ok
test tests::scan_helpers_find_bytes ... ok
test tests::end_to_end_through_pipeline ... ok
test tests::parser_emits_all_rows ... ok
test tests::sink_wants_skips_dropped_fields ... ok
test tests::splitter_finds_row_starts ... ok
test tests::typed_schema_casts_during_parse ... ok
test skip_region_tests::skip_regions_reject_splits_inside_quotes ... ok

test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

A failing `validate()` or `parse_chunk()` shows up here in milliseconds,
long before you involve Python. For the end-to-end check through
`rypipe.read()`, see the [walkthrough](walkthrough.md#step-6-build-and-test).

## See also { #see-also }

- [Splitter](./splitter.md): `Splitter` trait reference
- [Parser](./parser.md): `RecordParser` trait reference
- [Sink](./sink.md): `ColumnarSink` method reference
- [Scan primitives](./scan.md): Byte-searching utilities
- [Skip regions](./skip-regions.md): Comment/CDATA handling
- [Techniques](./techniques.md): Performance optimizations
