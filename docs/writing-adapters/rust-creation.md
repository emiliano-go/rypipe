# Rust Adapter Creation { #rust-adapter-creation }

This page covers the Rust side of writing a rypipe adapter: implementing
the `Splitter`, `RecordParser`, and understanding how the engine calls them.

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
iterates this to carve the input into chunks. For line-based formats,
scan for newline characters:

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

Called once per chunk before parsing. Reject invalid input early:

```rust
fn validate(&self, bytes: &[u8]) -> Result<()> {
    simdutf8::basic::from_utf8(bytes)              // fast SIMD UTF-8 check
        .map_err(|e| rypipe_core::Error::Utf8(e))?;
    Ok(())
}
```

### `parse_chunk` { #parse-chunk }

This is the hot path. Iterate rows, call `sink.begin_row()` / `put_field()`
/ `sink.end_row()` for each record:

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

    for line in text.lines() {
        if line.is_empty() { continue; }
        sink.begin_row();
        for field in self.parse_fields(line) {
            if sink.wants(field.name) {             // skip dropped fields
                sink.put_field(field.name, Value::Str(Cow::Borrowed(field.value)));
            }
        }
        sink.end_row();
    }
    Ok(())
}
```

Always check `sink.wants(name)` before scanning a field's value. Skipping
dropped fields saves significant CPU. For maximum performance, implement
`parse_chunk_generic` which takes a monomorphized sink instead of a trait
object (5-10% improvement by eliminating vtable dispatch).

!!! tip

    For maximum performance, implement `parse_chunk_generic` which takes a
    monomorphized sink instead of a trait object. The compiler can then inline
    every `begin_row`/`put_field`/`end_row` call, eliminating vtable dispatch
    (5-10% improvement on the hot path).


## Value types { #value-types }

`Value` variants: `Str(Cow<str>)` (default for text), `Int64(i64)`,
`Float64(f64)`, `Bool(bool)`, `Date32(i32)` (days since epoch),
`Timestamp(i64)`, `Null` (explicit missing).

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

## Complete example: CSV parser { #csv-parser }

```rust
use std::borrow::Cow;
use rypipe_core::{Splitter, RecordParser, ColumnarSink, Value, Result};

#[derive(Clone, Default)]
pub struct CsvSplitter { separator: u8 }

impl CsvSplitter {
    pub fn new(separator: u8) -> Self { Self { separator } }
}

impl Splitter for CsvSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        memchr::memchr(b'\n', &bytes[from..]).map(|r| from + r + 1)
    }
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
        (sample.len() / n).max(1)
    }
}

#[derive(Clone, Default)]
pub struct CsvParser { separator: u8, has_header: bool }

impl CsvParser {
    pub fn new(separator: u8, has_header: bool) -> Self {
        Self { separator, has_header }
    }
}

impl RecordParser for CsvParser {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Utf8(e))?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
        let mut lines = text.lines();
        let mut headers: Vec<String> = Vec::new();

        if self.has_header {
            if let Some(h) = lines.next() {
                headers = h.split(self.separator as char)
                    .map(|s| s.trim().to_string()).collect();
            }
        }

        for line in lines {
            if line.is_empty() { continue; }
            sink.begin_row();
            let fields: Vec<&str> = line.split(self.separator as char).collect();

            if headers.is_empty() {
                for (i, v) in fields.iter().enumerate() {
                    let name = format!("col_{i}");
                    if sink.wants(&name) {
                        sink.put_field(&name, Value::Str(Cow::Borrowed(v)));
                    }
                }
            } else {
                for (i, v) in fields.iter().enumerate() {
                    if let Some(name) = headers.get(i) {
                        if sink.wants(name) {
                            sink.put_field(name, Value::Str(Cow::Borrowed(v)));
                        }
                    }
                }
            }
            sink.end_row();
        }
        Ok(())
    }
}
```

The splitter finds newline boundaries; the parser iterates lines, maps
columns by header name, and feeds values into the sink.

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

```rust
fn parse_field(&self, name: &str, value: &str, sink: &mut dyn ColumnarSink) {
    match name {
        "id" => {
            if let Ok(n) = value.parse::<i64>() { sink.put_field(name, Value::Int64(n)); }
        }
        "amount" => {
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

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use rypipe_core::engine::TableBuilder;
    use std::sync::Arc;

    #[test]
    fn test_parse_csv() {
        let parser = CsvParser::new(b',', true);
        let mut builder = TableBuilder::with_plan(10, Arc::new(ExecutionPlan::new()));
        parser.parse_chunk(b"name,age\nAlice,30\nBob,25\n", &mut builder).unwrap();
        let batches = builder.finish().unwrap();
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn test_splitter() {
        let splitter = CsvSplitter::new(b',');
        assert_eq!(splitter.next_record_start(b"line1\nline2\n", 0), Some(6));
    }
}
```

### Integration test with Python { #python-integration }

```python
def test_csv_adapter(tmp_path):
    import rypipe, rypipe_csv
    p = tmp_path / "test.csv"
    p.write_text("name,age\nAlice,30\nBob,25\n")
    table = rypipe.read(str(p))
    assert table.num_rows == 2
    assert table.column("name").to_pylist() == ["Alice", "Bob"]
```

!!! warning

    Do not allocate `String` objects in the hot path. Every `Cow::Owned`
    allocation costs ~100 ns. For 10 million fields, that's 1 second of pure
    allocation overhead: easily avoidable with `Cow::Borrowed`.


## Error handling { #error-handling }

Use `rypipe_core::Error` variants with line numbers for clear diagnostics:

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

Wrap your adapter in a Python module using PyO3 and build with maturin:

```rust
use pyo3::prelude::*;
use rypipe_core::{Splitter, RecordParser};

#[pymodule]
fn rypipe_csv(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCsvAdapter>()?;
    Ok(())
}

#[pyclass]
struct PyCsvAdapter { separator: u8, has_header: bool }

#[pymethods]
impl PyCsvAdapter {
    #[new]
    fn new(separator: u8, has_header: bool) -> Self {
        Self { separator, has_header }
    }
    fn splitter(&self) -> PyResult<CsvSplitter> { Ok(CsvSplitter::new(self.separator)) }
    fn parser(&self) -> PyResult<CsvParser> { Ok(CsvParser::new(self.separator, self.has_header)) }
}
```

Build with `maturin develop`. The methods return Rust trait objects the engine
uses internally. Python never touches the hot path.

## See also { #see-also }

- [Splitter](./splitter.md): `Splitter` trait reference
- [Parser](./parser.md): `RecordParser` trait reference
- [Sink](./sink.md): `ColumnarSink` method reference
- [Scan primitives](./scan.md): Byte-searching utilities
- [Skip regions](./skip-regions.md): Comment/CDATA handling
- [Techniques](./techniques.md): Performance optimizations
