# Worked Examples { #worked-examples }

## CSV Adapter { #csv-adapter }

### Splitter { #splitter }

CSV splitting must respect quoted fields. A newline inside `"..."` is not a
record boundary.

```rust
use rypipe_core::Splitter;
use rypipe_core::decoder::SkipRegionFinder;

#[derive(Clone)]
struct CsvSplitter;

impl Splitter for CsvSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        // Skip past any leading non-newline bytes
        let start = memchr::memchr(b'\n', &bytes[from..])
            .map(|r| from + r + 1)?;
        // Scan forward, skipping quoted regions
        let mut pos = start;
        let mut in_quotes = false;
        while pos < bytes.len() {
            match bytes[pos] {
                b'"' => in_quotes = !in_quotes,
                b'\n' if !in_quotes => return Some(pos + 1),
                _ => {}
            }
            pos += 1;
        }
        None
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
        (sample.len() / n).max(1)
    }

    fn skip_regions(&self) -> Option<&dyn SkipRegionFinder> {
        Some(&CsvSkipRegions)
    }
}

struct CsvSkipRegions;
impl SkipRegionFinder for CsvSkipRegions {
    fn openers(&self) -> &[&'static [u8]] { &[b"\""] }
    fn closer_for(&self, _: &[u8]) -> &'static [u8] { b"\"" }
}
```

### Parser { #parser }

```rust
use std::borrow::Cow;
use rypipe_core::{RecordParser, ColumnarSink, Value, Result};

#[derive(Clone)]
struct CsvParser { header: Vec<String> }

impl RecordParser for CsvParser {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
        for line in text.lines() {
            if line.is_empty() { continue; }
            sink.begin_row();
            for (col, val) in self.header.iter().zip(line.split(',')) {
                if sink.wants(col) {
                    sink.put_field(col, Value::Str(Cow::Borrowed(val)));
                }
            }
            sink.end_row();
        }
        Ok(())
    }
}
```

### Usage { #usage }

```rust
let pipeline = Pipeline::new(CsvSplitter, CsvParser {
    header: vec!["id".into(), "name".into(), "amount".into()],
});
let batch = pipeline.read_path("data.csv", false, false)?;
```

---

!!! warning

    CSV splitting must handle quoted fields. A newline inside `"..."` is not a
    record boundary: without `skip_regions`, the splitter will break rows
    mid-quote, producing corrupt chunks. Always implement `SkipRegionFinder`
    for CSV.


## JSONL Adapter { #jsonl-adapter }

### Splitter { #splitter }

JSONL is newline-delimited JSON. Each line is one record.

```rust
#[derive(Clone)]
struct JsonlSplitter;

impl Splitter for JsonlSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        memchr::memchr(b'\n', &bytes[from..]).map(|r| from + r + 1)
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
        (sample.len() / n).max(1)
    }
}
```

No skip regions needed (JSON strings don't contain bare newlines in JSONL).

### Parser { #parser }

```rust
#[derive(Clone)]
struct JsonlParser;

impl RecordParser for JsonlParser {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
        for line in text.lines() {
            if line.is_empty() { continue; }
            // Parse JSON object, extract key-value pairs
            let obj: serde_json::Value = serde_json::from_str(line)
                .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
            if let Some(map) = obj.as_object() {
                sink.begin_row();
                for (k, v) in map {
                    if sink.wants(k) {
                        let val = match v {
                            serde_json::Value::Number(n) => {
                                if let Some(i) = n.as_i64() {
                                    Value::Int64(i)
                                } else {
                                    Value::Float64(n.as_f64().unwrap_or(0.0))
                                }
                            }
                            serde_json::Value::String(s) => Value::Str(Cow::Owned(s.clone())),
                            serde_json::Value::Bool(b) => Value::Bool(*b),
                            _ => Value::Str(Cow::Borrowed("")),
                        };
                        sink.put_field(k, val);
                    }
                }
                sink.end_row();
            }
        }
        Ok(())
    }
}
```

---

!!! tip

    JSON values are inherently typed: `serde_json::Number` maps cleanly to
    `Value::Int64` or `Value::Float64`. Avoid converting everything to strings
    when your format already has a typed representation. Emit typed values
    directly to skip post-parse casting.


## TSV Adapter { #tsv-adapter }

### Splitter { #splitter }

TSV is tab-delimited. Simple newline splitting.

```rust
#[derive(Clone)]
struct TsvSplitter;

impl Splitter for TsvSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        memchr::memchr(b'\n', &bytes[from..]).map(|r| from + r + 1)
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
        (sample.len() / n).max(1)
    }
}
```

### Parser { #parser }

```rust
#[derive(Clone)]
struct TsvParser { header: Vec<String> }

impl RecordParser for TsvParser {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
        for line in text.lines() {
            if line.is_empty() { continue; }
            sink.begin_row();
            for (col, val) in self.header.iter().zip(line.split('\t')) {
                if sink.wants(col) {
                    sink.put_field(col, Value::Str(Cow::Borrowed(val)));
                }
            }
            sink.end_row();
        }
        Ok(())
    }
}
```

## Multi-line Record Adapter { #multi-line-adapter }

Formats like LDIF, INI sections, or LDAP entries put one field per line with
blank-line-separated records. The key difference from the one-line examples
above: the Splitter splits on `\n\n` (double newline), and the Parser
accumulates fields across lines before calling `end_row()`.

### Input format { #multi-line-input }

```
name: Alice
department: Sales
amount: 15000.50

name: Bob
department: Engineering
amount: 8500.00

name: Carol
department: Marketing
amount: 12000.75
```

### Splitter { #multi-line-splitter }

Split on double newlines — each record starts after a blank line:

```rust
use rypipe_core::Splitter;

#[derive(Clone)]
struct MultilineSplitter;

impl Splitter for MultilineSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        // Find the next \n\n (blank line) — the record starts after it.
        let rest = &bytes[from..];
        for i in 0..rest.len().saturating_sub(1) {
            if rest[i] == b'\n' && rest[i + 1] == b'\n' {
                return Some(from + i + 2);
            }
        }
        None
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        // Count blank lines in the sample
        let n = sample
            .windows(2)
            .filter(|w| w[0] == b'\n' && w[1] == b'\n')
            .count()
            .max(1);
        (sample.len() / n).max(1)
    }
}
```

### Parser { #multi-line-parser }

Accumulate fields across lines. Call `begin_row()` on the first non-blank
line, `put_field()` for each `key: value` line, and `end_row()` only when
you hit a blank line or end of input:

```rust
use std::borrow::Cow;
use rypipe_core::{RecordParser, ColumnarSink, Value, Result};

#[derive(Clone)]
struct MultilineParser;

impl RecordParser for MultilineParser {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

        let mut in_record = false;

        for line in text.lines() {
            if line.is_empty() {
                // Blank line = record boundary. Finalize the current record.
                if in_record {
                    sink.end_row();
                    in_record = false;
                }
                continue;
            }

            // Start a new record if we aren't in one
            if !in_record {
                sink.begin_row();
                in_record = true;
            }

            // Parse "key: value" lines
            if let Some((key, value)) = line.split_once(':') {
                let key = key.trim();
                let value = value.trim();
                if sink.wants(key) {
                    sink.put_field(key, Value::Str(Cow::Borrowed(value)));
                }
            }
        }

        // Finalize the last record if the file doesn't end with a blank line
        if in_record {
            sink.end_row();
        }

        Ok(())
    }
}
```

!!! warning "Blank lines produce silent null rows"

    If you forget to guard blank lines (either in the Splitter or Parser),
    they produce rows with all null fields. No error, no warning — just
    wrong data. Always either:
    - Split on `\n\n` so blank lines are boundaries, not rows, **or**
    - Skip blank lines in `parse_chunk` with `if line.is_empty() { continue; }`

---

## Properties Adapter (continuation-aware) { #properties-adapter }

Java `.properties` files have `\` line continuations, `#`/`!` comments,
and `=`/`:`/whitespace separators. The declarative Splitter handles
continuations and comments automatically:

### Input format { #properties-input }

```
# Database config
driver = com.mysql.cj.jdbc.Driver
url = jdbc:mysql://localhost:3306/\
    mydb?useSSL=false
username = admin
password = secret

# Pool settings
pool_size = 10
timeout = 30000
```

### Splitter { #properties-splitter }

```rust
use rypipe_core::{RecordBoundary, Splitter, find_next_record_boundary};

#[derive(Clone)]
struct PropertiesSplitter;

impl Splitter for PropertiesSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        find_next_record_boundary(
            bytes, from,
            self.continuation_char(),
            self.comment_prefixes(),
            self.record_boundary() == RecordBoundary::BlankLine,
        )
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
        (sample.len() / n).max(1)
    }

    fn record_boundary(&self) -> RecordBoundary { RecordBoundary::Line }
    fn continuation_char(&self) -> Option<u8> { Some(b'\\') }
    fn comment_prefixes(&self) -> &[&[u8]] { &[b"#", b"!"] }
}
```

No manual byte scanning — the declarations tell the engine to skip
`\n` preceded by `\` and skip lines starting with `#` or `!`.

### Parser { #properties-parser }

The parser holds state within a chunk (tracking pending continuations)
but the Splitter ensures continuations never span chunks:

```rust
use std::borrow::Cow;
use rypipe_core::{RecordParser, ColumnarSink, Value, Result};

#[derive(Clone)]
struct PropertiesParser;

impl RecordParser for PropertiesParser {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

        let mut pending_key: Option<String> = None;
        let mut pending_value = String::new();

        for line in text.lines() {
            if line.is_empty() {
                if let Some(key) = pending_key.take() {
                    sink.begin_row();
                    if sink.wants("key") {
                        sink.put_field("key", Value::Str(Cow::Owned(key)));
                    }
                    if sink.wants("value") {
                        let v = std::mem::take(&mut pending_value);
                        sink.put_field("value", Value::Str(Cow::Owned(v)));
                    }
                    sink.end_row();
                }
                continue;
            }

            let trimmed = line.trim_end();
            if trimmed.ends_with('\\') {
                // Continuation: accumulate value, don't emit yet
                let content = &trimmed[..trimmed.len() - 1];
                if pending_key.is_none() {
                    // Start of a new key=value
                    if let Some((k, v)) = content.split_once(|c: char| c == '=' || c == ':') {
                        pending_key = Some(k.trim().to_string());
                        pending_value = v.trim().to_string();
                    }
                } else {
                    pending_value.push_str(content.trim());
                }
                continue;
            }

            // Regular line (or end of continuation)
            if let Some((k, v)) = trimmed.split_once(|c: char| c == '=' || c == ':') {
                let key = k.trim().to_string();
                let value = if pending_key.is_some() {
                    pending_value.push_str(v.trim());
                    std::mem::take(&mut pending_value)
                } else {
                    v.trim().to_string()
                };
                pending_key = None;

                sink.begin_row();
                if sink.wants("key") {
                    sink.put_field("key", Value::Str(Cow::Owned(key)));
                }
                if sink.wants("value") {
                    sink.put_field("value", Value::Str(Cow::Owned(value)));
                }
                sink.end_row();
            }
        }

        // Flush last record if file doesn't end with blank line
        if let Some(key) = pending_key.take() {
            sink.begin_row();
            if sink.wants("key") {
                sink.put_field("key", Value::Str(Cow::Owned(key)));
            }
            if sink.wants("value") {
                let v = std::mem::take(&mut pending_value);
                sink.put_field("value", Value::Str(Cow::Owned(v)));
            }
            sink.end_row();
        }

        Ok(())
    }
}
```

The `pending_key` / `pending_value` state persists across lines within the
chunk. The Splitter guarantees that `\` continuations don't cross chunk
boundaries, so the parser never loses data.

---

## Build and test { #build-and-test }

Both examples (the JSONL adapter and the TSV adapter above) build and
test exactly like the [walkthrough](./walkthrough.md#step-6-build-and-test):

```console
$ uv run --with maturin maturin develop --release
📦 Built wheel for abi3 Python ≥ 3.10
🛠 Installed rypipe-log-0.1.0

$ cargo test
test result: ok. 10 passed; 0 failed
```

Swap in the example's `Splitter`/`RecordParser` implementations and the
same commands apply unchanged.

## What the end user sees { #what-the-end-user-sees }

However the adapter is built internally, the finished product is a
self-contained package the user drives through its `Source` class; they
never import **rypipe** itself. A full session with the log adapter from
this guide:

```python
from rypipe_log import LogSource, FilterRows

# One-shot read with projection, types, and a pushed-down filter.
table = (
    LogSource(
        "sample.log",
        schema=["id", "name", "amount"],
        field_types={"id": "int64", "amount": "float64"},
    )
    | FilterRows(field="status", op="eq", value="active")
).to_arrow()
print(table.num_rows)

# Or stream the same file with bounded memory.
for batch in LogSource("sample.log").iter_record_batches(
    memory="64MiB", batch_size=10_000
):
    print(batch.num_rows)
```
