# Rust Adapter Creation

This page covers the Rust side of writing a rypipe adapter: implementing
the `Splitter`, `RecordParser`, and understanding how the engine calls them.

## Overview

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

## The Splitter trait

```rust
pub trait Splitter: Send + Sync {
    /// Find the start of the next record after byte offset `from`.
    ///
    /// Returns the byte offset of the first byte of the next record,
    /// or `None` if we have reached the end of the input.
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize>;

    /// Estimate bytes per row from a sample of the file.
    ///
    /// The engine uses this to size chunks and memory budgets.
    /// A good estimate means chunks are well-balanced.
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize;
}
```

### `next_record_start`

This is the most important method. The engine calls it repeatedly to find
where each chunk should start:

```
pos = 0
while let Some(next) = splitter.next_record_start(bytes, pos) {
    // bytes[pos..next] is one chunk
    pos = next
}
```

For line-based formats, find the next newline:

```rust
fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
    memchr::memchr(b'\n', &bytes[from..]).map(|r| from + r + 1)
}
```

For XML-like formats, find the next row tag:

```rust
fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
    memchr::memmem::find(&bytes[from..], &self.row_tag)
        .map(|r| from + r + self.row_tag.len())
}
```

### `estimate_bytes_per_row`

The engine uses this to decide how many rows to put in each chunk. A good
estimate means chunks are roughly equal size.

For line-based formats:

```rust
fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
    let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
    (sample.len() / n).max(1)
}
```

For XML formats, count row tags in a sample:

```rust
fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
    let sample_end = sample.len().min(65536);
    let tag_count = memchr::memmem::find_iter(&sample[..sample_end], &self.row_tag)
        .count()
        .max(1);
    (sample_end / tag_count).max(1)
}
```

## The RecordParser trait

```rust
pub trait RecordParser: Send + Sync {
    /// Validate that the bytes are valid for this format.
    ///
    /// Called once per chunk before parsing. Common implementation:
    /// check UTF-8 validity.
    fn validate(&self, bytes: &[u8]) -> Result<()>;

    /// Parse a chunk of bytes into field/value events.
    ///
    /// This is the hot path. Called once per chunk. Must call
    /// `sink.begin_row()`, `sink.put_field()`, and `sink.end_row()`
    /// for each row and field in the chunk.
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()>;
}
```

### `validate`

Called once per chunk before parsing. Use it to reject invalid input early:

```rust
fn validate(&self, bytes: &[u8]) -> Result<()> {
    simdutf8::basic::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Utf8(e))?;
    Ok(())
}
```

### `parse_chunk`

This is the hot path. Called once per chunk. Your implementation must:

1. Iterate over rows in the chunk
2. For each row, call `sink.begin_row()`
3. For each field, call `sink.put_field(name, value)`
4. Call `sink.end_row()`

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

    for line in text.lines() {
        if line.is_empty() {
            continue;
        }

        sink.begin_row();

        for field in self.parse_fields(line) {
            if sink.wants(field.name) {
                sink.put_field(field.name, Value::Str(Cow::Borrowed(field.value)));
            }
        }

        sink.end_row();
    }

    Ok(())
}
```

### `parse_chunk_generic`

For maximum performance, implement the devirtualized `parse_chunk_generic`.
This method takes a monomorphized sink (not a trait object), allowing the
compiler to inline `begin_row`, `put_field`, and `end_row` calls:

```rust
fn parse_chunk_generic<S: ColumnarSink>(&self, bytes: &[u8], sink: &mut S) -> Result<()>
where
    Self: Sized,
{
    // Same logic as parse_chunk, but with monomorphized sink
    // The compiler can inline all sink methods
}
```

This provides 5-10% performance improvement on the hot path by eliminating
vtable dispatch overhead.

### The `sink.wants()` check

Always check `sink.wants(name)` before scanning a field's value. This
returns `false` if the field is dropped or not in the schema. Skipping
dropped fields saves significant CPU:

```rust
// Good: check wants() first
if sink.wants(name) {
    let value = self.extract_value(bytes);  // expensive scan
    sink.put_field(name, Value::Str(Cow::Borrowed(value)));
}

// Bad: always scan, even for dropped fields
let value = self.extract_value(bytes);  // wasted work
sink.put_field(name, Value::Str(Cow::Borrowed(value)));
```

## Value types

The `Value` enum represents a parsed field value:

```rust
pub enum Value<'a> {
    Str(Cow<'a, str>),
    Int64(i64),
    Float64(f64),
    Bool(bool),
    Date32(i32),
    Timestamp(i64),
    Null,
}
```

### When to use each type

| Type | When to use | Example |
|------|-------------|---------|
| `Str(Cow::Borrowed)` | Default for text data | `"hello"`, `"2024-01-01"` |
| `Int64` | Integer columns | `"123"` -> `123` |
| `Float64` | Float columns | `"123.45"` -> `123.45` |
| `Bool` | Boolean columns | `"true"` / `"1"` -> `true` |
| `Date32` | Date columns | Days since Unix epoch |
| `Timestamp` | Timestamp columns | Raw integer; unit declared via `field_types` |
| `Null` | Explicit missing value | Use when field is absent or invalid |

### Borrowed vs owned strings

Always prefer `Cow::Borrowed` when the value is a slice of the input:

```rust
// Good: zero allocation
sink.put_field("name", Value::Str(Cow::Borrowed(name)));

// Bad: allocates a String
sink.put_field("name", Value::Str(Cow::Owned(name.to_string())));
```

If you need to modify the value (e.g., unescape HTML entities), use
`Cow::Owned`:

```rust
let unescaped = self.unescape(value);
sink.put_field("name", Value::Str(Cow::Owned(unescaped)));
```

## The ColumnarSink interface

You rarely implement `ColumnarSink` directly. The engine provides
`TableBuilder` as the production implementation. But understanding the
interface helps you write correct parsers.

### Core methods

```rust
pub trait ColumnarSink {
    /// Begin a new row.
    fn begin_row(&mut self);

    /// Push a value into a column.
    fn put_field(&mut self, name: &str, value: Value<'_>);

    /// End the current row.
    fn end_row(&mut self);

    /// Check if the engine needs a field (projection pushdown).
    fn wants(&self, name: &str) -> bool;

    /// Resolve a raw field name to the output column name.
    fn resolve<'a>(&'a self, name: &'a str) -> Option<&'a str>;

    /// Push a value using a pre-resolved name.
    fn put_field_resolved(&mut self, resolved_name: &str, value: Value<'_>);

    /// Finalize into an Arrow RecordBatch.
    fn finish(&mut self) -> Result<RecordBatch>;
}
```

### Row lifecycle

```
begin_row()
put_field("name", Value::Str("Alice"))
put_field("age", Value::Int64(30))
end_row()

begin_row()
put_field("name", Value::Str("Bob"))
put_field("age", Value::Int64(25))
end_row()
```

Fields can be pushed in any order within a row. The engine handles column
reordering at export time.

### `wants()` vs `resolve()` + `put_field_resolved()`

**`wants()` + `put_field()`**: Two hash probes (one for wants, one for put).
Simpler code, slightly slower.

```rust
if sink.wants(name) {
    sink.put_field(name, value);
}
```

**`resolve()` + `put_field_resolved()`**: Single hash probe (resolve returns
the resolved name, put_field_resolved uses it directly). Faster for hot paths.

```rust
if let Some(resolved) = sink.resolve(name) {
    sink.put_field_resolved(resolved, value);
}
```

Use `resolve` + `put_field_resolved` in your hot path if performance matters.

## Complete example: CSV parser

```rust
use std::borrow::Cow;
use rypipe_core::{Splitter, RecordParser, ColumnarSink, Value, Result};

#[derive(Clone, Default)]
pub struct CsvSplitter {
    separator: u8,
}

impl CsvSplitter {
    pub fn new(separator: u8) -> Self {
        Self { separator }
    }
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
pub struct CsvParser {
    separator: u8,
    has_header: bool,
}

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

        // Parse header row if present
        if self.has_header {
            if let Some(header_line) = lines.next() {
                headers = header_line.split(self.separator as char)
                    .map(|s| s.trim().to_string())
                    .collect();
            }
        }

        for line in lines {
            if line.is_empty() {
                continue;
            }

            sink.begin_row();

            let fields: Vec<&str> = line.split(self.separator as char).collect();

            if headers.is_empty() {
                // No header: use column indices as names
                for (i, value) in fields.iter().enumerate() {
                    let name = format!("col_{i}");
                    if sink.wants(&name) {
                        sink.put_field(&name, Value::Str(Cow::Borrowed(value)));
                    }
                }
            } else {
                // Use header names
                for (i, value) in fields.iter().enumerate() {
                    if let Some(name) = headers.get(i) {
                        if sink.wants(name) {
                            sink.put_field(name, Value::Str(Cow::Borrowed(value)));
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

## Common patterns

### Pattern: XML with nested elements

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let mut pos = 0;
    while pos < bytes.len() {
        // Find next row tag
        if let Some(row_start) = memchr::memmem::find(&bytes[pos..], &self.row_tag) {
            pos += row_start + self.row_tag.len();

            // Find row end
            if let Some(row_end) = memchr::memmem::find(&bytes[pos..], &self.row_end_tag) {
                let row_bytes = &bytes[pos..pos + row_end];
                pos += row_end + self.row_end_tag.len();

                // Parse fields within the row
                sink.begin_row();
                self.parse_row_fields(row_bytes, sink)?;
                sink.end_row();
            }
        } else {
            break;
        }
    }
    Ok(())
}
```

### Pattern: Skip regions (comments, CDATA)

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let regions = self.find_skip_regions(bytes);

    for line in bytes.split(|&b| b == b'\n') {
        // Skip lines that overlap with skip regions
        if self.in_skip_region(line.as_ptr() as usize, &regions) {
            continue;
        }

        // Parse the line
        sink.begin_row();
        self.parse_line(line, sink)?;
        sink.end_row();
    }
    Ok(())
}
```

### Pattern: Typed values

```rust
fn parse_field(&self, name: &str, value: &str, sink: &mut dyn ColumnarSink) {
    match name {
        "id" => {
            if let Ok(n) = value.parse::<i64>() {
                sink.put_field(name, Value::Int64(n));
            }
        }
        "amount" => {
            if let Ok(f) = value.parse::<f64>() {
                sink.put_field(name, Value::Float64(f));
            }
        }
        "active" => {
            let b = matches!(value, "true" | "1" | "yes");
            sink.put_field(name, Value::Bool(b));
        }
        _ => {
            sink.put_field(name, Value::Str(Cow::Borrowed(value)));
        }
    }
}
```

## Testing

### Unit test the parser

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use rypipe_core::engine::TableBuilder;
    use std::sync::Arc;

    #[test]
    fn test_parse_csv() {
        let parser = CsvParser::new(b',', true);
        let plan = ExecutionPlan::new();
        let mut builder = TableBuilder::with_plan(10, Arc::new(plan));

        let bytes = b"name,age\nAlice,30\nBob,25\n";
        parser.parse_chunk(bytes, &mut builder).unwrap();

        let batches = builder.finish().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn test_splitter() {
        let splitter = CsvSplitter::new(b',');
        let bytes = b"line1\nline2\nline3\n";

        let pos1 = splitter.next_record_start(bytes, 0);
        assert_eq!(pos1, Some(6)); // after "line1\n"

        let pos2 = splitter.next_record_start(bytes, pos1.unwrap());
        assert_eq!(pos2, Some(12)); // after "line2\n"
    }
}
```

### Integration test with Python

```python
def test_csv_adapter(tmp_path):
    import rypipe
    import rypipe_csv

    p = tmp_path / "test.csv"
    p.write_text("name,age\nAlice,30\nBob,25\n")

    table = rypipe.read(str(p))
    assert table.num_rows == 2
    assert table.column_names == ["name", "age"]
    assert table.column("name").to_pylist() == ["Alice", "Bob"]
```

## Error handling patterns

### Returning errors from parse_chunk

Use `rypipe_core::Error` variants:

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Plan(format!("invalid UTF-8: {e}")))?;

    for (i, line) in text.lines().enumerate() {
        if line.is_empty() { continue; }

        sink.begin_row();

        for part in line.split(',') {
            let (key, value) = part.split_once('=').ok_or_else(|| {
                rypipe_core::Error::Plan(format!("line {i}: missing '=' in field: {part}"))
            })?;

            if sink.wants(key) {
                sink.put_field(key, Value::Str(Cow::Borrowed(value)));
            }
        }

        sink.end_row();
    }

    Ok(())
}
```

### Graceful degradation

For some formats, you may want to skip bad rows instead of failing:

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

    for (i, line) in text.lines().enumerate() {
        if line.is_empty() { continue; }

        // Skip malformed lines instead of failing
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() < 2 {
            continue; // skip this row
        }

        sink.begin_row();
        for part in &parts {
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
```

## Advanced patterns

### Pattern: XML with namespace handling

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

    // Strip namespace prefixes for easier matching
    let cleaned = text.replace("tns:", "").replace("xs:", "");

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

### Pattern: Nested JSON-like formats

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

    for line in text.lines() {
        if line.is_empty() { continue; }

        sink.begin_row();

        // Parse key.subkey=value pairs
        for part in line.split(',') {
            if let Some((k, v)) = part.split_once('=') {
                // Flatten nested keys: "user.name" -> "user_name"
                let flat_key = k.replace('.', "_");
                if sink.wants(&flat_key) {
                    sink.put_field(&flat_key, Value::Str(Cow::Borrowed(v)));
                }
            }
        }

        sink.end_row();
    }

    Ok(())
}
```

### Pattern: Binary formats with fixed offsets

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    const RECORD_SIZE: usize = 32; // fixed 32-byte records
    const ID_OFFSET: usize = 0;
    const ID_SIZE: usize = 4;
    const NAME_OFFSET: usize = 4;
    const NAME_SIZE: usize = 20;

    for chunk in bytes.chunks_exact(RECORD_SIZE) {
        sink.begin_row();

        // Extract ID (big-endian i32)
        let id = i32::from_be_bytes([
            chunk[ID_OFFSET],
            chunk[ID_OFFSET + 1],
            chunk[ID_OFFSET + 2],
            chunk[ID_OFFSET + 3],
        ]);
        if sink.wants("id") {
            sink.put_field("id", Value::Int64(id as i64));
        }

        // Extract name (null-terminated ASCII)
        let name_end = chunk[NAME_OFFSET..NAME_OFFSET + NAME_SIZE]
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(NAME_SIZE);
        let name = std::str::from_utf8(&chunk[NAME_OFFSET..NAME_OFFSET + name_end])
            .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
        if sink.wants("name") {
            sink.put_field("name", Value::Str(Cow::Borrowed(name)));
        }

        sink.end_row();
    }

    Ok(())
}
```

### Pattern: Multiple row types in one file

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

    for line in text.lines() {
        if line.is_empty() { continue; }

        sink.begin_row();

        if line.starts_with("HEADER:") {
            // Parse header row
            let fields = &line[7..]; // skip "HEADER:"
            for (i, value) in fields.split(',').enumerate() {
                let name = format!("header_{i}");
                if sink.wants(&name) {
                    sink.put_field(&name, Value::Str(Cow::Borrowed(value)));
                }
            }
        } else if line.starts_with("DATA:") {
            // Parse data row
            let fields = &line[5..]; // skip "DATA:"
            for part in fields.split(',') {
                if let Some((k, v)) = part.split_once('=') {
                    if sink.wants(k) {
                        sink.put_field(k, Value::Str(Cow::Borrowed(v)));
                    }
                }
            }
        }

        sink.end_row();
    }

    Ok(())
}
```

## Performance considerations

### Minimize allocations

Every `Cow::Owned` allocation costs ~100 ns. For 10 million fields, that's
1 second. Use `Cow::Borrowed` whenever possible.

### Use SIMD for scanning

The `memchr` crate uses AVX2 on x86_64 and NEON on ARM. Use it for
byte searching:

```rust
// Good: SIMD-accelerated
let pos = memchr::memchr(b'<', bytes);

// Bad: scalar loop
let pos = bytes.iter().position(|&b| b == b'<');
```

### Batch field extraction

If your format has predictable field positions, extract multiple fields
in one pass:

```rust
fn parse_row(&self, line: &str, sink: &mut dyn ColumnarSink) {
    let mut iter = line.split(',');

    // First field (always present)
    if let Some(name) = iter.next() {
        if sink.wants("name") {
            sink.put_field("name", Value::Str(Cow::Borrowed(name)));
        }
    }

    // Second field (always present)
    if let Some(age_str) = iter.next() {
        if sink.wants("age") {
            if let Ok(age) = age_str.parse::<i64>() {
                sink.put_field("age", Value::Int64(age));
            }
        }
    }
}
```

## See also

- [Splitter](./splitter.md): `Splitter` trait reference
- [Parser](./parser.md): `RecordParser` trait reference
- [Sink](./sink.md): `ColumnarSink` method reference
- [Scan primitives](./scan.md): Byte-searching utilities
- [Skip regions](./skip-regions.md): Comment/CDATA handling
- [Techniques](./techniques.md): Performance optimizations

