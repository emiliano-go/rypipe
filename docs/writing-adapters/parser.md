# The RecordParser Trait

The `RecordParser` trait turns a byte chunk into field/value events fed to a
`ColumnarSink`. This is where format-specific parsing lives.

See [Architecture](../architecture/index.md) for how the engine calls
the parser and how the sink accumulates values.

## Trait definition

```rust
pub trait RecordParser: Send + Sync {
    /// Validate that the whole byte slice is well-formed.
    fn validate(&self, bytes: &[u8]) -> Result<()>;

    /// Parse one chunk and feed all row events into `sink`.
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()>;

    /// Parse one chunk with monomorphized sink (inlinable, devirtualized).
    /// Default delegates to parse_chunk.
    fn parse_chunk_generic<S: ColumnarSink>(&self, bytes: &[u8], sink: &mut S) -> Result<()>
    where Self: Sized;
}
```

## `validate`

Called once per chunk before parsing. Use it for upfront checks like UTF-8
validation. This is cheap (SIMD-accelerated) and catches malformed input early.

```rust
fn validate(&self, bytes: &[u8]) -> Result<()> {
    simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
    Ok(())
}
```

## `parse_chunk`

The main parsing loop. For each record in the chunk:

1. Call `sink.begin_row()` to start a new row
2. For each field, call `sink.put_field(name, value)` or faster alternatives
3. Call `sink.end_row()` to finalize the row

The engine calls this once per chunk. Your parser sees a contiguous byte range
that starts and ends at record boundaries (guaranteed by the `Splitter`).

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
    for line in text.lines() {
        if line.is_empty() { continue; }
        sink.begin_row();
        for (col, value) in self.header.iter().zip(line.split(',')) {
            sink.put_field(col, Value::Str(Cow::Borrowed(value)));
        }
        sink.end_row();
    }
    Ok(())
}
```

## `parse_chunk_generic`

The generic version allows the compiler to devirtualize `sink` calls (no vtable
dispatch). When the engine knows the concrete sink type (e.g., `TableBuilder`),
it calls `parse_chunk_generic` instead of `parse_chunk`.

```rust
fn parse_chunk_generic<S: ColumnarSink>(&self, bytes: &[u8], sink: &mut S) -> Result<()> {
    // Same body as parse_chunk, but sink calls are devirtualized.
    self.parse_chunk(bytes, sink as &mut dyn ColumnarSink)
}
```

Override this method for a measurable speedup on hot paths. The compiler can
then inline `sink.begin_row()`, `sink.put_field()`, and `sink.end_row()` into
the parsing loop, eliminating vtable dispatch overhead.

## Performance tips

### 1. Use `Cow::Borrowed` for non-entity text

Borrow from the input buffer when possible:

```rust
sink.put_field(col, Value::Str(Cow::Borrowed(value)));
```

The buffered filter path may hold values past the end of your parse function,
so a borrow of a temporary would dangle. `Cow::Borrowed` is safe because it
borrows from the chunk's byte slice, which lives long enough.

### 2. Check `wants()` before expensive extraction

Skip dropped fields entirely:

```rust
if sink.wants(col) {
    // expensive extraction here
    sink.put_field(col, value);
}
```

When `wants()` returns `false`, the engine will drop the column. Checking
before extraction saves the cost of parsing, decoding, and allocating.

### 3. Use `resolve` + `put_field_resolved` for expensive extraction

When extraction is costly (entity unescaping, base64 decode, date parsing),
resolve once and push with the resolved name to avoid a second hash probe:

```rust
if let Some(resolved) = sink.resolve(col) {
    let decoded = expensive_decode(value);
    sink.put_field_resolved(resolved, Value::Str(Cow::Owned(decoded)));
}
```

This pays the `rename→drop` hash lookup once instead of twice (once in
`resolve_and_put`'s internal `resolve`, once in `put_field`'s internal
`ensure_column_idx`).

### 4. Emit typed `Value` variants when possible

```rust
// Instead of:
sink.put_field("amount", Value::Str(Cow::Borrowed("123.45")));

// Do:
sink.put_field("amount", Value::Float64(123.45));
```

Typed variants skip the string-to-number conversion in the engine. The engine
still stores the value correctly and exports it as the right Arrow type.

### 5. Do not call `end_row()` for partial trailing rows

If your parser reaches the end of the chunk mid-record, just return. The engine
discards partial trailing rows automatically during `normalize()`.

### 6. Consider `parse_chunk_generic` for hot paths

If your parser is called millions of times (e.g., small files, streaming),
override `parse_chunk_generic` to get devirtualized sink calls:

```rust
fn parse_chunk_generic<S: ColumnarSink>(&self, bytes: &[u8], sink: &mut S) -> Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
    for line in text.lines() {
        if line.is_empty() { continue; }
        sink.begin_row();
        for (col, value) in self.header.iter().zip(line.split(',')) {
            if sink.wants(col) {
                sink.put_field(col, Value::Str(Cow::Borrowed(value)));
            }
        }
        sink.end_row();
    }
    Ok(())
}
```

## The parsing lifecycle

```
validate(bytes)                    ← called once per chunk
  │
  ▼
parse_chunk(bytes, sink)           ← called once per chunk
  │
  ├─ sink.begin_row()              ← clear per-row state
  ├─ sink.put_field("a", val)      ← push field (engine resolves + stores)
  ├─ sink.put_field("b", val)      ← push field
  ├─ sink.end_row()                ← null-fill missing, evaluate filter
  ├─ sink.begin_row()              ← next row
  ├─ ...
  └─ sink.end_row()               ← last row
  │
  ▼
sink.finish()                      ← called once after all chunks
  │
  ▼
Arrow RecordBatch                  ← zero-copy export
```

## Error handling

Return `Err` from `parse_chunk` to abort parsing. The engine will propagate
the error to the caller. Common error types:

- `rypipe_core::Error::Utf8`: invalid UTF-8 in input
- `rypipe_core::Error::Plan`: invalid plan or configuration
- `rypipe_core::Error::Io`: I/O error

Do not panic in `parse_chunk`. Panics are caught by `catch_unwind` in the
parallel executor, but they abort the entire parse.
