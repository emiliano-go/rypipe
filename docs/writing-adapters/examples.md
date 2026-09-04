# Worked Examples { #worked-examples }

## CSV Adapter { #csv-adapter }

### Splitter { #splitter }

CSV splitting must respect quoted fields. A newline inside `"..."` is not a
record boundary.

```rust
use rypipe_core::Splitter;
use rypipe_core::decoder::SkipRegionFinder;

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

/// warning

CSV splitting must handle quoted fields. A newline inside `"..."` is not a
record boundary: without `skip_regions`, the splitter will break rows
mid-quote, producing corrupt chunks. Always implement `SkipRegionFinder`
for CSV.

///

## JSONL Adapter { #jsonl-adapter }

### Splitter { #splitter }

JSONL is newline-delimited JSON. Each line is one record.

```rust
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

/// tip

JSON values are inherently typed: `serde_json::Number` maps cleanly to
`Value::Int64` or `Value::Float64`. Avoid converting everything to strings
when your format already has a typed representation. Emit typed values
directly to skip post-parse casting.

///

## TSV Adapter { #tsv-adapter }

### Splitter { #splitter }

TSV is tab-delimited. Simple newline splitting.

```rust
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
