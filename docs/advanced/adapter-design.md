# Adapter design

A high-performance adapter does as little work as possible per record. This page covers the `Splitter` and `RecordParser` design patterns that keep rypipe fast.

## `Splitter` design

The splitter finds safe chunk boundaries for parallel parsing.

```rust
pub trait Splitter: Send + Sync {
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize>;
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize;
}
```

Rules:

- The first point must be `0`; the last must be `bytes.len()`.
- Adjacent equal points produce empty ranges that the engine ignores.
- Each chunk must start at a valid row boundary.

A good splitter is cheap. It scans for boundaries with byte searches rather than parsing the whole chunk. For line-oriented formats, `memchr::memchr` finds newlines. For XML, `memchr::memmem` finds row tags.

## Finding split points with `memchr`

`memchr` is SIMD-accelerated on most platforms. A single `memmem` scan is much faster than iterating byte-by-byte.

Example from crxml:

```rust
use memchr;

let row_tag_count = memchr::memmem::find_iter(&sample[..sample_end], &self.row_tag).count();
```

For CSV, you also need to track quote state so a newline inside a quoted field does not become a false boundary.

## Handling comments, CDATA, and quoted fields

False split points corrupt chunks. A robust splitter skips regions that look like row boundaries but are not:

- XML comments: `<!-- ... -->`
- XML CDATA: `<![CDATA[ ... ]]>`
- CSV quoted fields: `"..."`
- JSON strings: `"..."`

In crxml, the splitter skips comments and CDATA while scanning for `<Row` tags. It also validates that a candidate tag is followed by whitespace, `>`, or `/` to avoid prefix collisions such as `<RowItem`.

## `RecordParser` tips

```rust
pub trait RecordParser: Send + Sync {
    fn validate(&self, bytes: &[u8]) -> Result<()>;
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()>;
}
```

Best practices:

- Validate UTF-8 once per chunk with `simdutf8` in `validate`.
- In `parse_chunk`, walk events or lines and emit fields.
- Call `sink.wants(name)` before expensive extraction to skip dropped fields.
- Do not call `end_row()` for partial trailing rows; the engine discards them.

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
