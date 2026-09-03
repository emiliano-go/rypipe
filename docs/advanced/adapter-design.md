# Adapter design

A high-performance adapter does as little work as possible per record. This page covers the `Splitter` and `RecordParser` design patterns that keep rypipe fast.

## `Splitter` design

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

## Complete Splitter example: newline-delimited log format

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

## `RecordParser` design

The record parser turns byte chunks into field/value events fed to a `ColumnarSink`.

```rust
pub trait RecordParser: Send + Sync {
    fn validate(&self, bytes: &[u8]) -> Result<()>;
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()>;
    fn parse_chunk_generic<S: ColumnarSink>(&self, bytes: &[u8], sink: &mut S) -> Result<()>
    where Self: Sized;
}
```

### `validate`

Called once per chunk before parsing. Use it for upfront checks:

```rust
fn validate(&self, bytes: &[u8]) -> Result<()> {
    simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
    Ok(())
}
```

This is cheap (SIMD-accelerated) and catches malformed input early.

### `parse_chunk`

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

### `parse_chunk_generic`

Override for devirtualized sink calls. When the engine knows the concrete sink type, it calls this instead, enabling inlining of `begin_row`/`put_field`/`end_row`:

```rust
fn parse_chunk_generic<S: ColumnarSink>(&self, bytes: &[u8], sink: &mut S) -> Result<()> {
    // Same body as parse_chunk, but sink calls are devirtualized.
    self.parse_chunk(bytes, sink as &mut dyn ColumnarSink)
}
```

Override this for a measurable speedup on hot paths.

### Push method hierarchy (cost model)

| Method | Cost | When to use |
|--------|------|-------------|
| `put_field_at(slot, value)` | ~5 ns | After `expect_slot` match, fastest, no resolution |
| `put_field_resolved(name, value)` | ~10 ns | After `resolve()`: skips rename lookup |
| `resolve_and_put(name, value)` | ~15 ns | Default, single resolve + push |
| `put_field(name, value)` | ~20 ns | Slowest, full resolve + push |

Use the fastest method your context allows.

## Error handling

### Malformed input

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

### Partial trailing rows

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

### Recovery from bad chunks

In parallel mode, if one chunk fails, the entire parse fails. There is no per-chunk recovery. If you need partial results, use the bounded/streaming path and handle errors per-batch in the consumer.

## Borrowing strings

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

## Sparse rows

If a field is missing, skip it entirely:

```rust
if let Some(value) = maybe_value {
    sink.put_field("status", Value::Str(value));
}
```

Do not emit `Value::Null` for every missing field. The engine null-fills missing columns at `end_row()`; emitting explicit nulls wastes work.

## Respecting `sink.wants`

`ColumnarSink::wants` lets the parser skip fields that will be dropped:

```rust
if sink.wants("internal_id") {
    sink.put_field("internal_id", Value::Str(extract_id(...)));
}
```

For expensive extractions (deep XML paths, regex captures), this is a major win. Always check `wants` before doing work that the engine will discard.

## `parse_tail` fallback

Chunks can start or end inside a row. A robust adapter has a fallback path that rescans from the nearest safe row start. crxml uses `parse_tail` to handle orphan close-tags at chunk boundaries without a serial pre-pass.

## Summary

- Split cheaply with `memchr`; defer full decoding.
- Skip comments, CDATA, quotes, and strings to avoid false boundaries.
- Borrow UTF-8 slices into the engine.
- Emit sparse rows and respect `sink.wants`.
- Handle trailing partial rows cleanly.
- Return `Err` for malformed input; never panic in `parse_chunk`.
- Use the fastest push method your context allows (`put_field_at` > `put_field_resolved` > `resolve_and_put` > `put_field`).
