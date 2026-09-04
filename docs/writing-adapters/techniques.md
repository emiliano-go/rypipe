# Performance Techniques { #performance-techniques }

This page covers the highest-impact optimizations for adapter authors. For
a complete checklist see the [Anti-Patterns](./anti-patterns.md) guide;
this page focuses on what to do, not what to avoid.

## The performance budget { #the-performance-budget }

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

## Technique 1: Declare schema upfront { #technique-1-declare-schema-upfront }

This is the single largest performance lever. When the column set is known,
declare it with `schema_order` and `field_types`:

```python
# Python — tell the engine exactly which columns exist and their types. { #python-tell-the-engine-exactly-which-columns-exist-and-their-types }
# This skips column discovery, stabilizes column order, and enables { #this-skips-column-discovery-stabilizes-column-order-and-enables }
# typed Arrow arrays (no intermediate strings). { #typed-arrow-arrays }
src = MySource("data.log", schema=["id", "name", "amount"],
               field_types={"id": "int64", "amount": "float64"})
```

```rust
// Rust — same declaration on the execution plan.
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

## Technique 2: Check `wants()` before scanning { #technique-2-check-wants-before-scanning }

Always check `sink.wants(name)` before doing expensive extraction:

```rust
// Good: skip dropped fields entirely — no scanning, no decoding,
// no allocation for fields the engine doesn't need.
if sink.wants(name) {
    let value = self.extract_value(bytes);  // expensive scan
    sink.put_field(name, Value::Str(Cow::Borrowed(value)));
}

// Bad: always scan, even for dropped fields — wasted work.
let value = self.extract_value(bytes);  // runs even if user said drop this column
sink.put_field(name, Value::Str(Cow::Borrowed(value)));
```

**Why it helps:** When the user drops columns, your parser skips all work
for those columns (no scanning, no decoding, no allocation). This is
especially significant for formats where field extraction involves
parsing or regex.

**Performance gain:** +66% on `drop_all` workloads.

## Technique 3: Use `scan::find` instead of raw `memchr` { #technique-3-use-scan-find }

The `rypipe_core::scan` module provides byte-search primitives with an
O(1) fast path:

```rust
use rypipe_core::scan;

// Good: uses scan::find with O(1) fast path.
// For single-byte patterns (b'<'), it checks directly without SIMD
// setup overhead. Falls back to memchr for multi-byte patterns.
let pos = scan::find(bytes, 0, b'<');

// Raw memchr: no fast path — pays SIMD setup cost even for single bytes.
let pos = memchr::memchr(b'<', bytes);
```

**Why it helps:** `scan::find` checks for single-byte patterns first
(no SIMD setup cost), then falls back to `memchr` for multi-byte patterns.
This matters when your parser searches for many different single-byte
delimiters in each row.

See [Scan primitives](./scan.md) for details.

## Technique 4: Borrow strings with `Cow::Borrowed` { #technique-4-cow-borrowed }

Always borrow from the input when possible:

```rust
// Good: zero allocation — borrows the &str directly from the input bytes.
// The engine copies into Arrow arrays later (zero-copy when possible).
sink.put_field("name", Value::Str(Cow::Borrowed(name)));

// Bad: allocates a String on the heap for every field value.
sink.put_field("name", Value::Str(Cow::Owned(name.to_string())));
```

**Why it helps:** `Cow::Borrowed` avoids heap allocation. The engine copies
the bytes into the Arrow array later (zero-copy when possible). In a
hot loop processing millions of rows, even small per-field allocations
add up to measurable throughput loss.

**When to use `Cow::Owned`:** Only when you must modify the value (e.g.,
unescape HTML entities, normalize whitespace, decode escaped characters).

## Technique 5: Emit typed values { #technique-5-emit-typed-values }

When the format has numeric or boolean data, parse directly into the
correct `Value` variant:

```rust
// Good: engine builds Arrow Int64 array directly — no string parsing later.
let value: i64 = field.value.parse().unwrap_or(0);
sink.put_field("id", Value::Int64(value));

// Bad: engine must parse the string into a number later — double work.
sink.put_field("id", Value::Str(Cow::Borrowed(field.value)));
```

**Why it helps:** The engine builds typed Arrow arrays directly from
`Int64`/`Float64`/`Bool` values. With strings, it must parse later
(double work). This is the second-largest performance lever after schema
declaration.

**Performance gain:** 10-20% for numeric-heavy workloads.

## Technique 6: Use `resolve` + `put_field_resolved` { #technique-6-resolve }

For hot paths, use single-hash-probe resolution:

```rust
// Good: single hash probe — resolve() returns the column name in one
// lookup, put_field_resolved() uses it directly (no second lookup).
if let Some(resolved) = sink.resolve(name) {
    sink.put_field_resolved(resolved, value);
}

// Slower: two hash probes — wants() does one lookup, put_field() does
// another. Fine for rare fields, but adds up in inner loops.
if sink.wants(name) {
    sink.put_field(name, value);
}
```

**Why it helps:** `resolve` returns the resolved column name with one hash
probe. `put_field_resolved` uses that name directly (no second lookup).
This eliminates one hash-table access per field in your parser's inner loop.

**When to use:** In the inner loop of your parser, for every field.
**When `wants()` is fine:** For fields that appear rarely or are checked
once per row (not per field).

## Technique 7: Implement `parse_chunk_generic` { #technique-7-parse-chunk-generic }

For maximum performance, implement the devirtualized `parse_chunk_generic`:

```rust
// Standard parse_chunk takes a trait object — dynamic dispatch on every
// begin_row/put_field/end_row call.
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    // ... logic here
}

// parse_chunk_generic takes a concrete type — the compiler can inline
// every sink method call. This eliminates vtable lookup overhead.
fn parse_chunk_generic(&self, bytes: &[u8], sink: &mut impl ColumnarSink) -> Result<()> {
    // Same logic as parse_chunk, but the compiler monomorphizes this
    // for each concrete sink type, enabling full inlining.
}
```

**Why it helps:** The standard `parse_chunk` takes `&mut dyn ColumnarSink`
(trait object with dynamic dispatch). `parse_chunk_generic` takes
`&mut impl ColumnarSink` (monomorphized). The compiler can inline
`begin_row`, `put_field`, and `end_row` calls.

**Performance gain:** 5-10% on the hot path.

/// note

`parse_chunk_generic` only helps when the engine knows the concrete sink
type at call time. For adapter-internal sinks (custom `ColumnarSink`
implementations), you must override it explicitly — the default falls back
to the trait-object version.

///

## Technique 8: Implement `skip_regions` { #technique-8-skip-regions }

If your format has comments, CDATA sections, or quoted strings that may
contain false-positive delimiters, implement `skip_regions` by implementing
the `SkipRegionFinder` trait:

```rust
use rypipe_core::decoder::SkipRegionFinder;

// Tells the engine which byte sequences to skip when searching for
// split points. Without this, the splitter may chunk inside a comment
// or CDATA section, causing parse errors.
struct XmlSkipRegions;

impl SkipRegionFinder for XmlSkipRegions {
    // Openers: sequences that begin a skip region.
    fn openers(&self) -> &[&'static [u8]] {
        &[b"<!--", b"<![CDATA["]
    }

    // Closer_for: given an opener, return the matching closer.
    fn closer_for(&self, opener: &[u8]) -> &'static [u8] {
        match opener {
            b"<!--" => b"-->",
            b"<![CDATA[" => b"]]>",
            _ => unreachable!(),
        }
    }
}

// Wire it into your Splitter implementation:
fn skip_regions(&self) -> Option<&dyn SkipRegionFinder> {
    Some(&XmlSkipRegions)
}
```

**Why it helps:** The engine uses skip regions to avoid false-positive
split points. Without them, the splitter may chunk inside a comment,
causing parse errors. This is critical for XML, HTML, and other formats
where delimiters can appear inside non-data regions.

See [Skip regions](./skip-regions.md) for details.

## Technique 9: Use SIMD for scanning { #technique-9-simd-scanning }

For formats with complex delimiters, use SIMD-accelerated scanning:

```rust
// Find a multi-byte pattern using SIMD-accelerated memmem search.
// memchr crate uses AVX2 on x86_64, NEON on ARM — scans 16-32 bytes
// per cycle.
fn find_field_end(&self, bytes: &[u8]) -> usize {
    // Search for closing tag prefix "</" — covers </Field>, </Text>,
    // </Section>, etc.
    let finder = memchr::memmem::Finder::new(b"</");
    match finder.find(bytes) {
        Some(pos) => pos,  // found a closing tag — field ends here
        None => bytes.len(), // no closing tag — field extends to chunk end
    }
}
```

**Why it helps:** SIMD instructions scan 16-32 bytes per cycle. The
`memchr` crate uses AVX2 on x86_64, NEON on ARM. This is especially
beneficial for formats like XML or JSON where you search for closing
tags across large field values.

/// warning

`estimate_bytes_per_row` is called once on the first 64 KB of the file.
Returning a fixed value regardless of data creates unbalanced chunks and
hurts parallel efficiency. Always count delimiters in the sample.

///

## Benchmarking { #benchmarking }

Use criterion to benchmark your pipeline end-to-end:

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

/// tip

Benchmark on representative data, not synthetic strings. The `Cow::Borrowed`
path is faster when the input is already in memory; real files have I/O
overhead, encoding noise, and variable-length fields that change the
cost profile.

///

Profile with `perf` to find hotspots, then re-run the benchmark to verify
improvement. Focus on the top 3-5 hotspots — those are where the real
gains come from.

## Memory considerations { #memory-considerations }

Peak memory occurs when all chunks parse simultaneously in parallel mode.
For a 533 MB file with 16 threads: ~528 MB peak (~33 MB per chunk).
Streaming mode reduces this to ~88 MB. Use `wants()`, typed values, and
`Cow::Borrowed` to minimize per-row overhead.

## Performance checklist { #performance-checklist }

- [ ] Schema declared upfront (`schema_order` + `field_types`)
- [ ] `sink.wants()` checked before expensive extraction
- [ ] `resolve` + `put_field_resolved` used in hot path
- [ ] `Cow::Borrowed` used for all field values
- [ ] Typed `Value` variants emitted for numeric/boolean columns
- [ ] `parse_chunk_generic` implemented for devirtualization
- [ ] `skip_regions` implemented for formats with comments/CDATA
- [ ] No allocations in the hot path
- [ ] `estimate_bytes_per_row` returns accurate estimate

## See also { #see-also }

- [Schema](./schema.md): The biggest performance lever
- [Anti-Patterns](./anti-patterns.md): Common mistakes and fixes
- [Splitter](./splitter.md): `Splitter` trait reference
- [Parser](./parser.md): `RecordParser` trait reference
- [Sink](./sink.md): `ColumnarSink` method reference
- [Scan primitives](./scan.md): Byte-searching utilities
