# Performance Techniques

This page covers performance techniques for adapter authors. These are
the optimizations that separate a working adapter from a fast one.

## The performance budget

For a 533 MB file on a Ryzen 5800X:

| Phase | Budget | Your responsibility |
|-------|--------|-------------------|
| Splitting | ~5% | `next_record_start` must be fast |
| Validation | ~2% | UTF-8 check |
| Parsing | ~80% | `parse_chunk` is the hot path |
| Export | ~5% | Engine handles this |
| Schema | ~5% | Declare upfront if known |

Your parser's job is to make the parsing phase fast. The engine handles
everything else.

## Technique 1: Declare schema upfront

This is the single largest performance lever. When the column set is known,
declare it with `schema_order` and `field_types`:

```python
src = MySource("data.log", schema=["id", "name", "amount"],
               field_types={"id": "int64", "amount": "float64"})
```

Or in Rust:

```rust
let plan = ExecutionPlan::new()
    .schema_order(["id", "name", "amount"])
    .type_as("id", FieldType::Int64)
    .type_as("amount", FieldType::Float64);
```

**Why it helps:**

- **Skips column discovery**: no full I/O pass to find field names
- **Stabilizes column order**: parallel chunks produce identical schemas
  (fast export path)
- **Enables typed arrays**: `field_types` builds Arrow arrays directly
  (no intermediate strings)
- **Activates `row_satisfied` byte-jump**: scanner skips remaining fields
  once all wanted columns arrive

**Performance gain:** +80% with projection, +11% without.

See [Schema](./schema.md) for the full guide.

## Technique 2: Check `wants()` before scanning

Always check `sink.wants(name)` before doing expensive extraction:

```rust
// Good: skip dropped fields entirely
if sink.wants(name) {
    let value = self.extract_value(bytes);  // expensive scan
    sink.put_field(name, Value::Str(Cow::Borrowed(value)));
}

// Bad: always scan, even for dropped fields
let value = self.extract_value(bytes);  // wasted work
sink.put_field(name, Value::Str(Cow::Borrowed(value)));
```

**Why it helps:** When the user drops columns, your parser skips all work
for those columns (no scanning, no decoding, no allocation).

**Performance gain:** +66% on `drop_all` workloads.

## Technique 3: Use `scan::find` instead of raw `memchr`

The `rypipe_core::scan` module provides byte-search primitives with an
O(1) fast path:

```rust
use rypipe_core::scan;

// Good: uses scan::find with O(1) fast path
let pos = scan::find(bytes, 0, b'<');

// Raw memchr: no fast path
let pos = memchr::memchr(b'<', bytes);
```

**Why it helps:** `scan::find` checks for single-byte patterns first
(no SIMD setup cost), then falls back to `memchr` for multi-byte patterns.

See [Scan primitives](./scan.md) for details.

## Technique 4: Borrow strings with `Cow::Borrowed`

Always borrow from the input when possible:

```rust
// Good: zero allocation
sink.put_field("name", Value::Str(Cow::Borrowed(name)));

// Bad: allocates a String
sink.put_field("name", Value::Str(Cow::Owned(name.to_string())));
```

**Why it helps:** `Cow::Borrowed` avoids heap allocation. The engine copies
the bytes into the Arrow array later (zero-copy when possible).

**When to use `Cow::Owned`:** Only when you must modify the value (e.g.,
unescape HTML entities, normalize whitespace).

## Technique 5: Emit typed values

When the format has numeric or boolean data, parse directly into the
correct `Value` variant:

```rust
// Good: engine skips string-to-number conversion
let value: i64 = field.value.parse().unwrap_or(0);
sink.put_field("id", Value::Int64(value));

// Bad: engine must parse the string later
sink.put_field("id", Value::Str(Cow::Borrowed(field.value)));
```

**Why it helps:** The engine builds typed Arrow arrays directly from
`Int64`/`Float64`/`Bool` values. With strings, it must parse later
(double work).

**Performance gain:** 10-20% for numeric-heavy workloads.

## Technique 6: Use `resolve` + `put_field_resolved`

For hot paths, use single-hash-probe resolution:

```rust
// Good: single hash probe
if let Some(resolved) = sink.resolve(name) {
    sink.put_field_resolved(resolved, value);
}

// Slower: two hash probes (wants + put_field)
if sink.wants(name) {
    sink.put_field(name, value);
}
```

**Why it helps:** `resolve` returns the resolved column name with one hash
probe. `put_field_resolved` uses that name directly (no second lookup).

**When to use:** In the inner loop of your parser, for every field.

**When `wants()` is fine:** For fields that appear rarely or are checked
once per row (not per field).

## Technique 7: Implement `skip_regions`

If your format has comments, CDATA sections, or quoted strings that may
contain false-positive delimiters, implement `skip_regions` by implementing
the `SkipRegionFinder` trait:

```rust
use rypipe_core::decoder::SkipRegionFinder;

struct XmlSkipRegions;

impl SkipRegionFinder for XmlSkipRegions {
    fn openers(&self) -> &[&'static [u8]] {
        &[b"<!--", b"<![CDATA["]
    }

    fn closer_for(&self, opener: &[u8]) -> &'static [u8] {
        match opener {
            b"<!--" => b"-->",
            b"<![CDATA[" => b"]]>",
            _ => unreachable!(),
        }
    }
}

// In your Splitter implementation:
fn skip_regions(&self) -> Option<&dyn SkipRegionFinder> {
    Some(&XmlSkipRegions)
}
```

**Why it helps:** The engine uses skip regions to avoid false-positive
split points. Without them, the splitter may chunk inside a comment,
causing parse errors.

See [Skip regions](./skip-regions.md) for details.

## Technique 8: Implement `parse_chunk_generic`

For maximum performance, implement the devirtualized `parse_chunk_generic`:

```rust
fn parse_chunk_generic(&self, bytes: &[u8], sink: &mut impl ColumnarSink) -> Result<()> {
    // Same logic as parse_chunk, but with monomorphized sink
    // The compiler can inline sink methods
}
```

**Why it helps:** The standard `parse_chunk` takes `&mut dyn ColumnarSink`
(trait object). `parse_chunk_generic` takes `&mut impl ColumnarSink`
(monomorphized). The compiler can inline `begin_row`, `put_field`, and
`end_row` calls.

**Performance gain:** 5-10% on the hot path.

## Technique 9: Borrow from input bytes

When parsing, borrow directly from the input byte slice:

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    // Find field value as a byte slice
    let start = /* ... */;
    let end = /* ... */;
    let value = &bytes[start..end];

    // Convert to &str (zero-copy if valid UTF-8)
    let text = std::str::from_utf8(value)
        .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

    // Borrow directly into the value
    sink.put_field("name", Value::Str(Cow::Borrowed(text)));
}
```

**Why it helps:** No allocation for the field value. The bytes are borrowed
from the input, which is already in memory.

## Technique 10: Use SIMD for scanning

For formats with complex delimiters, use SIMD-accelerated scanning:

```rust
// Find multiple patterns simultaneously
fn find_field_end(&self, bytes: &[u8]) -> usize {
    // Scan for </Field>, </Text>, or </Section>
    let finder = memchr::memmem::Finder::new(b"</");
    match finder.find(bytes) {
        Some(pos) => pos,
        None => bytes.len(),
    }
}
```

**Why it helps:** SIMD instructions scan 16-32 bytes per cycle. The
`memchr` crate uses AVX2 on x86_64, NEON on ARM.

## Benchmarking

### Microbenchmark your parser

```rust
use criterion::{criterion_group, criterion_main, Criterion};

fn bench_parse(c: &mut Criterion) {
    let parser = MyParser::new();
    let bytes = generate_test_data(1024 * 1024); // 1 MB

    c.bench_function("parse_chunk", |b| {
        b.iter(|| {
            let mut sink = BlackHoleSink::new();
            parser.parse_chunk(&bytes, &mut sink).unwrap();
        })
    });
}

criterion_group!(benches, bench_parse);
criterion_main!(benches);
```

### Profile with `perf`

```bash
# Record profiling data
perf record -g -- cargo run --release --example bench_parse

# View report
perf report
```

### Measure with the engine

```bash
# Run the engine throughput benchmark
cargo run --release -p rypipe-core --example bench_throughput

# Run the extended benchmark suite
python benchmarks/bench_extended.py --quick
```

## Common performance mistakes

### Mistake 1: Allocating in the hot path

```rust
// Bad: allocates a String for every field
let name = field.name.to_string();
sink.put_field(&name, value);

// Good: borrow the name
sink.put_field(field.name, value);
```

### Mistake 2: Parsing the same field twice

```rust
// Bad: parse for validation, then parse again for the value
let value: i64 = field.value.parse().map_err(|e| ...)?;
sink.put_field("id", Value::Str(Cow::Borrowed(field.value)));

// Good: parse once, emit typed value
let value: i64 = field.value.parse().map_err(|e| ...)?;
sink.put_field("id", Value::Int64(value));
```

### Mistake 3: Checking `wants()` after expensive work

```rust
// Bad: extract value first, then check wants
let value = self.extract_value(bytes);  // expensive
if sink.wants(name) {
    sink.put_field(name, Value::Str(Cow::Borrowed(value)));
}

// Good: check wants first
if sink.wants(name) {
    let value = self.extract_value(bytes);
    sink.put_field(name, Value::Str(Cow::Borrowed(value)));
}
```

### Mistake 4: Using `Cow::Owned` unnecessarily

```rust
// Bad: allocates a String
sink.put_field("name", Value::Str(Cow::Owned(value.to_string())));

// Good: borrow if possible
sink.put_field("name", Value::Str(Cow::Borrowed(value)));
```

### Mistake 5: Not implementing `estimate_bytes_per_row`

```rust
// Bad: returns a fixed value
fn estimate_bytes_per_row(&self, _sample: &[u8]) -> usize {
    100  // wrong for most formats
}

// Good: count delimiters in sample
fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
    let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
    (sample.len() / n).max(1)
}
```

## Performance checklist

- [ ] Schema declared upfront (`schema_order` + `field_types`)
- [ ] `sink.wants()` checked before expensive extraction
- [ ] `Cow::Borrowed` used for all field values
- [ ] Typed `Value` variants emitted for numeric/boolean columns
- [ ] `resolve` + `put_field_resolved` used in hot path
- [ ] No allocations in the hot path
- [ ] `estimate_bytes_per_row` returns accurate estimate
- [ ] `skip_regions` implemented for formats with comments/CDATA
- [ ] `parse_chunk_generic` implemented for devirtualization

## Detailed examples

### Example 1: Optimized CSV parser

```rust
use std::borrow::Cow;
use rypipe_core::{Splitter, RecordParser, ColumnarSink, Value, Result};

#[derive(Clone, Default)]
pub struct CsvSplitter;

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

        // Parse header row
        if self.has_header {
            if let Some(header_line) = lines.next() {
                headers = header_line.split(self.separator as char)
                    .map(|s| s.trim().to_string())
                    .collect();
            }
        }

        // Parse data rows
        for line in lines {
            if line.is_empty() { continue; }

            sink.begin_row();

            let fields: Vec<&str> = line.split(self.separator as char).collect();

            for (i, value) in fields.iter().enumerate() {
                let name = if headers.is_empty() {
                    format!("col_{i}")
                } else {
                    headers.get(i).cloned().unwrap_or_default()
                };

                // Check wants before doing any work
                if sink.wants(&name) {
                    // Try to parse as typed value
                    if let Ok(n) = value.parse::<i64>() {
                        sink.put_field(&name, Value::Int64(n));
                    } else if let Ok(f) = value.parse::<f64>() {
                        sink.put_field(&name, Value::Float64(f));
                    } else {
                        sink.put_field(&name, Value::Str(Cow::Borrowed(value)));
                    }
                }
            }

            sink.end_row();
        }

        Ok(())
    }
}
```

### Example 2: XML parser with skip regions

```rust
use std::borrow::Cow;
use rypipe_core::{Splitter, RecordParser, ColumnarSink, Value, Result};
use rypipe_core::scan::SkipRegion;

#[derive(Clone, Default)]
pub struct XmlSplitter {
    row_tag: Vec<u8>,
}

impl XmlSplitter {
    pub fn new(row_tag: &str) -> Self {
        Self {
            row_tag: row_tag.as_bytes().to_vec(),
        }
    }
}

impl Splitter for XmlSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        memchr::memmem::find(&bytes[from..], &self.row_tag)
            .map(|r| from + r + self.row_tag.len())
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let sample_end = sample.len().min(65536);
        let tag_count = memchr::memmem::find_iter(&sample[..sample_end], &self.row_tag)
            .count()
            .max(1);
        (sample_end / tag_count).max(1)
    }

    fn skip_regions(&self) -> Option<&[SkipRegion]> {
        Some(&[
            SkipRegion::new(b"<!--", b"-->"),
            SkipRegion::new(b"<![CDATA[", b"]]>"),
        ])
    }
}

#[derive(Clone, Default)]
pub struct XmlParser {
    row_tag: String,
    field_tag: String,
    value_tag: String,
}

impl RecordParser for XmlParser {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Utf8(e))?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

        let mut in_row = false;

        for line in text.lines() {
            let trimmed = line.trim();

            // Detect row start
            if trimmed.contains(&self.row_tag) && !trimmed.starts_with('/') {
                in_row = true;
                sink.begin_row();
                continue;
            }

            // Detect row end
            if trimmed.starts_with('/') && trimmed.contains(&self.row_tag) {
                in_row = false;
                sink.end_row();
                continue;
            }

            // Parse fields within row
            if in_row && trimmed.contains(&self.field_tag) {
                if let Some(name) = self.extract_attribute(trimmed, "Name") {
                    if let Some(value) = self.extract_tag_value(trimmed, &self.value_tag) {
                        if sink.wants(&name) {
                            sink.put_field(&name, Value::Str(Cow::Borrowed(value)));
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl XmlParser {
    fn extract_attribute<'a>(&self, line: &'a str, attr: &str) -> Option<&'a str> {
        let pattern = format!("{attr}=\"");
        let start = line.find(&pattern)? + pattern.len();
        let end = line[start..].find('"')?;
        Some(&line[start..start + end])
    }

    fn extract_tag_value<'a>(&self, line: &'a str, tag: &str) -> Option<&'a str> {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        let start = line.find(&open)? + open.len();
        let end = line[start..].find(&close)?;
        Some(&line[start..start + end])
    }
}
```

## Benchmarking workflow

### Step 1: Create a benchmark

```rust
use criterion::{criterion_group, criterion_main, Criterion};
use rypipe_core::{Pipeline, ExecutionPlan};

fn bench_adapter(c: &mut Criterion) {
    let bytes = std::fs::read("bench_data/test_100mb.csv").unwrap();
    let plan = ExecutionPlan::new()
        .schema_order(["id", "name", "amount"])
        .type_as("id", FieldType::Int64)
        .type_as("amount", FieldType::Float64);

    c.bench_function("csv_parse_100mb", |b| {
        b.iter(|| {
            let pipeline = Pipeline::new(CsvSplitter, CsvParser::new(b',', true))
                .with_plan(plan.clone());
            pipeline.read_bytes(&bytes).unwrap();
        })
    });
}

criterion_group!(benches, bench_adapter);
criterion_main!(benches);
```

### Step 2: Profile with perf

```bash
# Record profiling data
perf record -g -- cargo run --release --example bench_adapter

# View hotspots
perf report --stdio | head -50
```

### Step 3: Optimize and re-measure

After making changes, re-run the benchmark to verify improvement.

## Memory considerations

### Peak memory

Peak memory occurs when all chunks are being parsed simultaneously in
parallel mode. For a 533 MB file with 16 threads:

- Each chunk: ~33 MB (533 / 16)
- Peak: ~528 MB (16 chunks x 33 MB)
- With streaming: ~88 MB (one chunk at a time)

### Reducing memory

1. Use streaming mode for large files
2. Implement `wants()` to skip dropped columns
3. Use typed values instead of strings
4. Avoid `Cow::Owned` allocations

## See also

- [Schema](./schema.md): The biggest performance lever
- [Splitter](./splitter.md): `Splitter` trait reference
- [Parser](./parser.md): `RecordParser` trait reference
- [Sink](./sink.md): `ColumnarSink` method reference
- [Scan primitives](./scan.md): Byte-searching utilities
