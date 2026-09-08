# Adapter design { #adapter-design }

A high-performance adapter does as little work as possible per record. This page covers the `Splitter` and `RecordParser` design patterns that keep rypipe fast.

## `Splitter` design { #splitter-design }

The splitter finds safe chunk boundaries for parallel parsing.

```rust
pub trait Splitter: Send + Sync {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize>;
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize;
    fn skip_regions(&self) -> Option<&dyn SkipRegionFinder> { None }
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize>;
}
```

`find_split_points` has a default implementation that uses `next_record_start` to find split points.

Rules:

- The first point must be `0`; the last must be `bytes.len()`.
- Adjacent equal points produce empty ranges that the engine ignores.
- Each chunk must start at a valid row boundary.

A good splitter is cheap. It scans for boundaries with byte searches rather than parsing the whole chunk. For line-oriented formats, `memchr::memchr` finds newlines. For XML, `memchr::memmem` finds row tags.

## Complete Splitter example: newline-delimited log format { #complete-splitter-example-newline-delimited-log-format }

Here is a complete, annotated `Splitter` implementation for a newline-delimited format:

```rust
use rypipe_core::Splitter;
use rypipe_core::decoder::SkipRegionFinder;

/// Splitter for newline-delimited log files.
/// Each line is one record. Fields are separated by `=`.
struct LogSplitter;

impl Splitter for LogSplitter {
    /// Find the next record boundary at or after `from`.
    /// Returns the byte offset of the first byte of the next record,
    /// or `None` if no more records exist.
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        memchr::memchr(b'\n', &bytes[from..])
            .map(|rel| from + rel + 1)  // position past the newline
    }

    /// Estimate average bytes per row from a 64 KB sample.
    /// The bounded executor uses this to plan chunk sizes.
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
        (sample.len() / n).max(1)
    }

    // skip_regions() and find_split_points() use defaults.
    // The default find_split_points handles nominal offsets,
    // parallel search, skip-region rejection, dedup, and the 2 MiB chunk floor.
}
```

Key points:

- `next_record_start` must return a position where a record starts, not the delimiter itself.
- `estimate_bytes_per_row` is called once on a sample. Simple newline-counting suffices for most formats.
- Do **not** override `find_split_points` unless you have a measured reason. The default handles everything including the 2 MiB chunk floor that prevents sub-MB collapse.

## `RecordParser` design { #recordparser-design }

The record parser turns byte chunks into field/value events fed to a `ColumnarSink`.

```rust
pub trait RecordParser: Send + Sync {
    fn validate(&self, bytes: &[u8]) -> Result<()>;
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()>;
    fn parse_chunk_generic<S: ColumnarSink>(&self, bytes: &[u8], sink: &mut S) -> Result<()>
    where Self: Sized;
}
```

### `validate` { #validate }

Called once per chunk before parsing. Use it for upfront checks:

```rust
fn validate(&self, bytes: &[u8]) -> Result<()> {
    simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
    Ok(())
}
```

This is cheap (SIMD-accelerated) and catches malformed input early.

### `parse_chunk` { #parse_chunk }

The main parsing loop. For each record: call `sink.begin_row()`, emit fields with `sink.put_field()`, then `sink.end_row()`.

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
    for line in text.lines() {
        if line.is_empty() { continue; }
        sink.begin_row();
        for (col, val) in self.header.iter().zip(line.split(',')) {
            if sink.wants(col) {           // skip dropped fields
                sink.put_field(col, Value::Str(Cow::Borrowed(val)));
            }
        }
        sink.end_row();
    }
    Ok(())
}
```

### `parse_chunk_generic` { #parse_chunk_generic }

Override for devirtualized sink calls. When the engine knows the concrete sink type, it calls this instead, enabling inlining of `begin_row`/`put_field`/`end_row`:

```rust
fn parse_chunk_generic<S: ColumnarSink>(&self, bytes: &[u8], sink: &mut S) -> Result<()> {
    // Same body as parse_chunk, but sink calls are devirtualized.
    self.parse_chunk(bytes, sink as &mut dyn ColumnarSink)
}
```

Override this for a measurable speedup on hot paths.

### Push method hierarchy (cost model) { #push-method-hierarchy }

The `ColumnarSink` trait offers several ways to emit a field, from most to
least work per call:

| Method | Resolution work | When to use |
|--------|-----------------|-------------|
| `put_field_at(slot, value)` | None | After an `expect_slot` match; the slot is known |
| `put_field_resolved(name, value)` | None | After a successful `resolve(name)` |
| `resolve_and_put(name, value)` | One resolve | Convenience: resolve, then push if wanted |
| `put_field(name, value)` | Full resolve + push | Default path |

Raw-name variants (`resolve_raw`, `resolve_and_put_raw`) do the same jobs
for parsers that hold field names as `&[u8]` and want to avoid a UTF-8
conversion per field. `put_row(&[(name, value)])` emits a whole row in one
call when the parser already has all fields collected.

Use the cheapest method your context allows.

### The slot and scan fast paths { #slot-and-scan-fast-paths }

For maximum throughput, the sink exposes a cooperative protocol that lets a
scanner skip work the engine does not need:

- `needs_value()` returning `false` puts the scanner in locate-only mode:
  it reports field positions via `resolve()` without extracting values.
  Schema discovery uses this.
- `needs_resolve()` returning `false` tells the scanner it can push values
  without resolving names at all.
- `wants(name)` lets the parser skip dropped or projected-out fields.
- `wanted_mask()` exposes the wanted set as a bitmask for very wide rows.
- `expect_slot(ordinal)` / `record_slot(ordinal, slot, raw_name)` /
  `layout_broken(ordinal)` / `reset_child_ordinal()` implement a positional
  slot protocol: when every row has the same field layout, the parser can
  match fields by ordinal and push with `put_field_at`, skipping name
  resolution entirely. If the layout changes mid-file, the parser reports
  it through `layout_broken` and falls back to name-based pushes.
- `row_satisfied()` returning `true` tells the parser the current row
  already fails or passes everything downstream needs, so it can byte-jump
  to the next row without extracting the remaining fields. This is the
  optimization behind crxml's fastest benchmark numbers.

All of these have default implementations, so a simple parser can ignore
them and opt in one at a time.

## Error handling { #error-handling }

### Malformed input { #malformed-input }

Return `Err` from `parse_chunk` to abort parsing. The engine propagates the error to the caller:

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    // If a record is malformed, return an error:
    let value = extract_value(bytes)
        .ok_or_else(|| rypipe_core::Error::Plan("malformed record".into()))?;
    // ...
    Ok(())
}
```

Common error types:

- `rypipe_core::Error::Utf8`: invalid UTF-8 in input
- `rypipe_core::Error::Plan`: invalid plan or configuration
- `rypipe_core::Error::Io`: I/O error

**Do not panic** in `parse_chunk`. Panics are caught by `catch_unwind` in the parallel executor, but they abort the entire parse and produce a hard-to-debug `MergeError`.

### Partial trailing rows { #partial-trailing-rows }

Chunks can start or end inside a row. If your parser reaches the end of the chunk mid-record, just return. The engine discards partial trailing rows automatically during `normalize()`:

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let text = std::str::from_utf8(bytes)?;
    for line in text.lines() {
        if line.is_empty() { continue; }
        sink.begin_row();
        // ... emit fields ...
        sink.end_row();
    }
    // If the last line was partial, end_row() was never called for it.
    // The engine's normalize() discards the incomplete row.
    Ok(())
}
```

### Recovery from bad chunks { #recovery-from-bad-chunks }

In parallel mode, if one chunk fails, the entire parse fails. There is no per-chunk recovery. If you need partial results, use the bounded/streaming path and handle errors per-batch in the consumer.

## Borrowing strings { #borrowing-strings }

If the input chunk is valid UTF-8, hand borrowed `&str` slices to the engine:

```rust
let text = std::str::from_utf8(bytes)?;
for line in text.lines() {
    sink.begin_row();
    sink.put_field("value", Value::Str(line));
    sink.end_row();
}
```

The engine copies the string into its arena only when necessary. Borrowing avoids per-field allocations in the parser.

## Sparse rows { #sparse-rows }

If a field is missing, skip it entirely:

```rust
if let Some(value) = maybe_value {
    sink.put_field("status", Value::Str(value));
}
```

Do not emit `Value::Null` for every missing field. The engine null-fills missing columns at `end_row()`; emitting explicit nulls wastes work.

## Respecting `sink.wants` { #respecting-sinkwants }

`ColumnarSink::wants` lets the parser skip fields that will be dropped:

```rust
if sink.wants("internal_id") {
    sink.put_field("internal_id", Value::Str(extract_id(...)));
}
```

For expensive extractions (deep XML paths, regex captures), this is a major win. Always check `wants` before doing work that the engine will discard.

## Split regions and chunk floors { #split-regions-and-chunk-floors }

Two splitter-related pieces are easy to miss:

- `skip_regions()` returns a `SkipRegionFinder` (with `openers()`,
  `closer_for(opener)`, and an optional `window()`) that describes regions
  where a candidate boundary must be rejected, such as comments, CDATA
  sections, or quoted strings. The engine checks candidates with
  `in_skip_region`, so a `<Row` inside a comment never becomes a split
  point.
- `plan_chunk_count` (used by the default `find_split_points`) keeps the
  chunk count in `[threads, 1024]` and enforces `MIN_CHUNK_BYTES` (2 MiB),
  so tiny inputs do not collapse into per-row chunks.

## Chunk-boundary rows { #parse_tail-fallback }

Chunks can start or end inside a row. A robust adapter does not need a
serial pre-pass to handle this: the splitter only emits boundaries at valid
row starts, and the engine discards the incomplete trailing row of each
chunk during `TableBuilder::normalize()`. Your parser just returns when it
runs out of complete records, as shown in
[Partial trailing rows](#partial-trailing-rows).

## Summary { #summary }

- Split cheaply with `memchr`; defer full decoding.
- Declare skip regions (comments, CDATA, quotes) so false boundaries are rejected.
- Borrow UTF-8 slices into the engine.
- Emit sparse rows and respect `sink.wants`.
- Opt into the sink fast paths (`put_field_at` slots, locate-only scans, `row_satisfied`) when profiles justify them.
- Handle trailing partial rows cleanly; the engine discards the incomplete row.
- Return `Err` for malformed input; never panic in `parse_chunk`.
- Use the cheapest push method your context allows (`put_field_at` > `put_field_resolved` > `resolve_and_put` > `put_field`).
