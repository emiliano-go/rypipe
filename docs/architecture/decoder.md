# Decoder API

`decoder.rs` defines the boundary between format-specific and format-agnostic
code. Adapters implement two traits (`Splitter`, `RecordParser`); the engine
implements the third (`ColumnarSink`). This page documents all three traits
in depth, including every method, its cost model, and how to squeeze maximum
performance from each.

See [Writing adapters](../writing-adapters/) for the step-by-step guide to
implementing these traits.

## Splitter

```rust
pub trait Splitter: Send + Sync {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize>;
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize;
    fn skip_regions(&self) -> Option<&dyn SkipRegionFinder> { None }
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize>;
}
```

### next_record_start (required)

The only required method. Given a byte position, return where the next record
starts at or after that position.

```rust
fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
    memchr::memchr(b'\n', &bytes[from..]).map(|r| from + r + 1)
}
```

The engine calls this at nominal offsets (`bytes.len() * i / n`) to find
record boundaries. Your implementation answers "where does the next record
start from here?" The engine handles deduplication, sorting, and chunk planning.

### estimate_bytes_per_row (required)

Called once on a sample (first 64 KB) to estimate row size. Used by the
bounded executor to plan chunk sizes.

```rust
fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
    let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
    (sample.len() / n).max(1)
}
```

### skip_regions (optional)

Returns a `SkipRegionFinder` for rejecting split points inside comments,
CDATA, quoted fields, or string literals. Default: `None`.

```rust
fn skip_regions(&self) -> Option<&dyn SkipRegionFinder> {
    Some(&CsvSkipRegions)
}
```

See [Skip regions](../writing-adapters/skip-regions.md) for the full interface.

### find_split_points (default, do not override)

The default implementation handles everything:

1. `plan_chunk_count` determines chunk count (2 MiB floor, thread caps)
2. Nominal offsets at `bytes.len() * i / n`
3. `par_iter` over nominals calling `next_record_start`
4. Skip-region rejection via `in_skip_region`
5. Dedup, sort, prepend 0, append `bytes.len()`

Override only with a measured reason. The default applies the 2 MiB floor
that prevents sub-MB chunk collapse.

## RecordParser

```rust
pub trait RecordParser: Send + Sync {
    fn validate(&self, bytes: &[u8]) -> Result<()>;
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()>;
    fn parse_chunk_generic<S: ColumnarSink>(&self, bytes: &[u8], sink: &mut S) -> Result<()>
    where Self: Sized;
}
```

### validate

Called once per chunk. Use for upfront checks like UTF-8 validation:

```rust
fn validate(&self, bytes: &[u8]) -> Result<()> {
    simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
    Ok(())
}
```

### parse_chunk

The main parsing loop. For each record: `begin_row`, `put_field` × N,
`end_row`.

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
    for line in text.lines() {
        if line.is_empty() { continue; }
        sink.begin_row();
        for (col, val) in header.iter().zip(line.split(',')) {
            sink.put_field(col, Value::Str(Cow::Borrowed(val)));
        }
        sink.end_row();
    }
    Ok(())
}
```

### parse_chunk_generic

Override for devirtualized sink calls. The engine calls this when it knows
the concrete sink type, enabling inlining of `begin_row`/`put_field`/`end_row`.

## ColumnarSink

```rust
pub trait ColumnarSink {
    fn begin_row(&mut self);
    fn put_field(&mut self, name: &str, value: Value<'_>);
    fn end_row(&mut self);
    fn finish(&mut self) -> Result<RecordBatch>;
    // ... 17 more methods with defaults
}
```

### Required methods (4)

- **`begin_row`**: Start a new row. Clears per-row state.
- **`put_field`**: Push a field value. Engine resolves name and stores.
- **`end_row`**: End the row. Null-fills missing, evaluates filter.
- **`finish`**: Finalize into Arrow RecordBatch.

### Field resolution (4)

- **`wants(name)`**: `false` to signal the engine will drop this field.
- **`resolve(name)`**: Map raw name to output column, or `None` if dropped.
- **`put_field_resolved(name, value)`**: Push with pre-resolved name.
- **`resolve_and_put(name, value)`**: Combined resolve + push.

### Tier control (3)

- **`needs_value()`**: `false` = locate-only (skip text extraction).
- **`needs_resolve()`**: `false` = traverse-only (skip resolve).
- **`row_rejected()`**: `true` = filter rejected; scanner byte-jumps.

### Projection (3)

- **`row_satisfied()`**: `true` = all wanted columns present; byte-jump.
- **`wanted_mask()`**: Bitmask of wanted columns for O(1) membership test.
- **`reset_child_ordinal()`**: Reset ordinal after row-tag attributes.

### Layout prediction (4)

- **`expect_slot(ordinal)`**: `(slot, raw_name)` for memcmp fast path.
- **`put_field_at(slot, value)`**: Direct slot push, no name resolution.
- **`record_slot(ordinal, slot, raw_name)`**: Cache slot for next row.
- **`layout_broken(ordinal)`**: Invalidate cached layout.

### Batch (1)

- **`put_row(fields)`**: Push a complete row in one call.

### Raw-byte methods (2)

- **`resolve_raw(raw_name)`**: Resolve a field name still in raw byte form.
  Default converts via `from_utf8` then delegates to `resolve`.
- **`resolve_and_put_raw(raw_name, value)`**: Combined raw-name resolve +
  push. Default converts via `from_utf8` then delegates to `resolve_and_put`.

### Fast path hierarchy

| Method | Cost | When to use |
|--------|------|-------------|
| `put_field_at(slot, value)` | ~5 ns | After expect_slot match |
| `put_field_resolved(name, value)` | ~10 ns | After resolve() |
| `resolve_and_put(name, value)` | ~15 ns | Default |
| `put_field(name, value)` | ~20 ns | Slowest, full resolution |

### Projection fast path

```rust
// Scanner checks after each field:
if sink.row_satisfied() {
    let after = find_row_close(bytes, cur, row_tag, regions);
    sink.end_row();
    return Flow::At(after);
}
```

`wanted_mask()` provides the bitmask: `(mask >> slot) & 1 == 1` means wanted.

### Layout prediction fast path

```rust
expect_slot(ordinal) → Some((slot, expected))
  memcmp(raw, expected) == 0 → put_field_at(slot, value)
  memcmp(raw, expected) != 0 → layout_broken(ordinal)
```

Skips: attribute scan, UTF-8 decode, hash lookup. Cost: ~8 ns vs ~25 ns.

### Predicate-first fast path

```rust
begin_row → [put_field × N] → end_row
               │
               ▼
        check predicate slot
        ├── Pass → direct mode
        ├── Fail → discard buffer
        └── Undecided → continue buffering
```

Adaptive: if predicate column is late (> 4/5 of columns), disable buffering.

### Thread safety

`ColumnarSink` is `Send` but not `Sync`. Each chunk gets its own instance.
The engine creates one `TableBuilder` per chunk and merges after completion.

## Helper functions

### split_points_to_ranges

```rust
pub fn split_points_to_ranges(points: &[usize], len: usize) -> Vec<Range<usize>>
```

Converts split points to non-empty ranges. Points must be sorted with `0`
first and `len` last.

### plan_chunk_count

```rust
pub fn plan_chunk_count(bytes: usize, threads: usize, mode: SplitMode) -> usize
```

Determines chunk count with 2 MiB floor, thread caps, and 1024 maximum.
See [Chunk planning](../writing-adapters/chunk-planning.md).

### in_skip_region

```rust
pub fn in_skip_region(bytes: &[u8], at: usize, finder: &dyn SkipRegionFinder) -> bool
```

Bounded backward scan to check if a position is inside a skip region.
See [Skip regions](../writing-adapters/skip-regions.md).
