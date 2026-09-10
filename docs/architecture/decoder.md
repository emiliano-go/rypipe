# Decoder API { #decoder-api }

`decoder.rs` defines the boundary between format-specific and format-agnostic
code. Adapters implement two traits (`Splitter`, `RecordParser`); the engine
implements the third (`ColumnarSink`). This page documents all three traits
in depth, including every method, its cost model, and how to squeeze maximum
performance from each.

See [Writing adapters](../building-adapters/) for the step-by-step guide to
implementing these traits.

## Splitter { #splitter }

```rust
pub trait Splitter: Send + Sync {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize>;
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize;
    fn skip_regions(&self) -> Option<&dyn SkipRegionFinder> { None }
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize>;
}
```

### next_record_start (required) { #next_record_start }

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

### estimate_bytes_per_row (required) { #estimate_bytes_per_row }

Called once on a sample (first 64 KB) to estimate row size. Used by the
bounded executor to plan chunk sizes.

```rust
fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
    let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
    (sample.len() / n).max(1)
}
```

### skip_regions (optional) { #skip_regions }

Returns a `SkipRegionFinder` for rejecting split points inside comments,
CDATA, quoted fields, or string literals. Default: `None`.

```rust
fn skip_regions(&self) -> Option<&dyn SkipRegionFinder> {
    Some(&CsvSkipRegions)
}
```

See [Skip regions](../building-adapters/skip-regions.md) for the full interface.

### find_split_points (default, do not override) { #find_split_points }

The default implementation handles everything:

1. Early return `vec![0, bytes.len()]` when `max_chunks <= 1` or input is empty
2. `plan_chunk_count` determines chunk count (2 MiB floor, thread caps)
3. Nominal offsets at `bytes.len() * i / n`
4. `par_iter` over nominals calling `next_record_start`
5. Skip-region rejection via `in_skip_region`
6. Dedup, sort, prepend 0, append `bytes.len()`

Override only with a measured reason. The default applies the 2 MiB floor
that prevents sub-MB chunk collapse.

## RecordParser { #recordparser }

```rust
pub trait RecordParser: Send + Sync {
    fn validate(&self, bytes: &[u8]) -> Result<()>;
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()>;
    fn parse_chunk_generic<S: ColumnarSink>(&self, bytes: &[u8], sink: &mut S) -> Result<()>
    where Self: Sized;
}
```

### validate { #validate }

Called once per chunk. Use for upfront checks like UTF-8 validation:

```rust
fn validate(&self, bytes: &[u8]) -> Result<()> {
    simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
    Ok(())
}
```

### parse_chunk { #parse_chunk }

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

### parse_chunk_generic { #parse_chunk_generic }

Override for devirtualized sink calls. The engine calls this when it knows
the concrete sink type, enabling inlining of `begin_row`/`put_field`/`end_row`.

## ColumnarSink { #columnarsink }

```rust
pub trait ColumnarSink {
    fn begin_row(&mut self);
    fn put_field(&mut self, name: &str, value: Value<'_>);
    fn end_row(&mut self);
    fn finish(&mut self) -> Result<RecordBatch>;
    // ... 17 more methods with defaults
}
```

### Required methods { #required-methods }

- **`begin_row`**: Start a new row. Clears per-row state.
- **`put_field`**: Push a field value. Engine resolves name and stores.
- **`end_row`**: End the row. Null-fills missing, evaluates filter.
- **`finish`**: Finalize into Arrow RecordBatch.

### Field resolution { #field-resolution }

- **`wants(name)`**: `false` to signal the engine will drop this field.
- **`resolve(name)`**: Map raw name to output column, or `None` if dropped.
- **`put_field_resolved(name, value)`**: Push with pre-resolved name.
- **`resolve_and_put(name, value)`**: Combined resolve + push.

### Tier control { #tier-control }

- **`needs_value()`**: `false` = locate-only (skip text extraction).
- **`needs_resolve()`**: `false` = traverse-only (skip resolve).
  `#[doc(hidden)]` and experimental: intended for benchmarking/profiling
  harnesses, so do not rely on it in production adapters.
- **`row_rejected()`**: `true` = filter rejected; scanner byte-jumps.

### Projection { #projection }

- **`row_satisfied()`**: `true` = all wanted columns present; byte-jump.
- **`wanted_mask()`**: Bitmask of wanted columns for O(1) membership test.
- **`reset_child_ordinal()`**: Reset ordinal after row-tag attributes.

### Layout prediction { #layout-prediction }

- **`expect_slot(ordinal)`**: `(slot, raw_name)` for memcmp fast path.
- **`put_field_at(slot, value)`**: Direct slot push, no name resolution.
- **`record_slot(ordinal, slot, raw_name)`**: Cache slot for next row.
- **`layout_broken(ordinal)`**: Invalidate cached layout.

### Batch { #batch }

- **`put_row(fields)`**: Push a complete row in one call.

### Raw-byte methods { #raw-byte-methods }

- **`resolve_raw(raw_name)`**: Resolve a field name still in raw byte form.
  Default converts via `from_utf8` then delegates to `resolve`.
- **`resolve_and_put_raw(raw_name, value)`**: Combined raw-name resolve +
  push. Default converts via `from_utf8` then delegates to `resolve_and_put`.

The raw variants exist for parsers that already hold the name as `&[u8]`
(scanner output, zero-copy slices). Overriding them lets you compare or
hash the bytes directly and skip the per-field UTF-8 validation. The
constraint: you only win if your format guarantees ASCII or UTF-8 names;
otherwise the default `from_utf8` path is the safe choice.

### Fast path hierarchy { #fast-path-hierarchy }

| Method | Cost | When to use |
|--------|------|-------------|
| `put_field_at(slot, value)` | ~5 ns | After expect_slot match |
| `put_field_resolved(name, value)` | ~10 ns | After resolve() |
| `resolve_and_put(name, value)` | ~15 ns | Default |
| `put_field(name, value)` | ~20 ns | Slowest, full resolution |

The cheaper methods are not free upgrades: each one shifts work onto the
parser. `put_field_at` is only valid after `expect_slot` has confirmed the
row layout, so the parser must detect layout changes and call
`layout_broken`. `put_field_resolved` requires the parser to call
`resolve()` itself and cache the result. `put_field` has no preconditions
at all. Start with `put_field`; only move down the table when profiling
shows name resolution is hot. See
[Fast paths in order](../building-adapters/sink.md#fast-paths-in-order)
for the full trade-off discussion.

### Projection fast path { #projection-fast-path }

```rust
// Scanner checks after each field:
if sink.row_satisfied() {
    let after = find_row_close(bytes, cur, row_tag, regions);
    sink.end_row();
    return Flow::At(after);
}
```

`wanted_mask()` provides the bitmask: `(mask >> slot) & 1 == 1` means wanted.

### Layout prediction fast path { #layout-prediction-fast-path }

```rust
expect_slot(ordinal) → Some((slot, expected))
  memcmp(raw, expected) == 0 → put_field_at(slot, value)
  memcmp(raw, expected) != 0 → layout_broken(ordinal)
```

Skips: attribute scan, UTF-8 decode, hash lookup. Cost: ~8 ns vs ~25 ns.

### Predicate-first fast path { #predicate-first-fast-path }

```rust
begin_row → [put_field × N] → end_row
               │
               ▼
        check predicate slot
        ├── Pass → direct mode
        ├── Fail → discard buffer
        └── Undecided → continue buffering
```

Adaptive: if the predicate column is late (`ordinal >= ncols * 4 / 5`), disable
buffering. The check runs after the first committed row (earlier rows may not
have created all columns yet) and prefers the frozen schema or `schema_order`
column count over `columns.len()`, which is unreliable for sparse files.

### Thread safety { #thread-safety }

`ColumnarSink` declares no `Send`/`Sync` supertraits; the concrete
`TableBuilder` is both `Send` and `Sync`. Each chunk gets its own instance.
In the parallel executor's fast path (no `auto_dict`, schema-consistent
chunks) each chunk's builder is exported as its own batch with no merge; the
merge path only runs for `auto_dict` upgrades or schema-inconsistent chunks.
Bounded and streaming executors emit batches incrementally via `split_off`
and never run a final merge.

### Observer hooks { #observer-hooks }

An optional `plan.observer` (`RowObserver: Send + Sync`) receives hooks from
`TableBuilder` during parsing: `on_begin_row`, `on_put_field`,
`on_row_accepted`, `on_row_rejected`, and `on_chunk_finished`. All default to
no-ops; implement only what you need. Two subtleties: values buffered by the
predicate-first path fire `on_put_field` only when the row is drained
(accepted), and on the late-predicate path values are pushed directly and
popped on reject, so `on_put_field` can fire for a row that is later
rejected (`on_row_rejected` still reports it). Hooks run on the parse hot
path from whichever thread parses the chunk, so keep them cheap.

## Helper functions { #helper-functions }

### split_points_to_ranges { #split_points_to_ranges }

```rust
pub fn split_points_to_ranges(points: &[usize], len: usize) -> Vec<Range<usize>>
```

Converts split points to non-empty ranges. Points must be sorted with `0`
first and `len` last.

### plan_chunk_count { #plan_chunk_count }

```rust
pub fn plan_chunk_count(bytes: usize, threads: usize, mode: SplitMode) -> usize
```

Determines chunk count with 2 MiB floor, thread caps, and 1024 maximum.
See [Chunk planning](../building-adapters/chunk-planning.md).

### in_skip_region { #in_skip_region }

```rust
pub fn in_skip_region(bytes: &[u8], at: usize, finder: &dyn SkipRegionFinder) -> bool
```

Bounded backward scan to check if a position is inside a skip region.
See [Skip regions](../building-adapters/skip-regions.md).
