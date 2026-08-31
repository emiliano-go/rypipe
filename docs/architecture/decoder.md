# Decoder API

`crates/rypipe-core/src/decoder.rs` (~170 lines) defines the boundary between format specific and format agnostic code. Adapters implement two traits; the engine implements the third.

## Splitter

```rust
pub trait Splitter: Send + Sync {
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize>;
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize;
}
```

* `find_split_points` returns sorted byte offsets where the input may be split. The first should be `0` and the last should be `bytes.len()`. Adjacent offsets produce one `Range`. The helper `split_points_to_ranges(&points, len) -> Vec<Range<usize>>` turns points into non empty ranges via `windows(2)` and `filter_map(|w| if start < end { Some(start..end) } else { None })`.

* `estimate_bytes_per_row` is used by `BoundedExecutor` to size batches (`rows_per_batch = budget / bytes_per_row`). It is called once on the whole input (or on `bytes` for `run_bytes`).

Rules for a correct splitter: points are sorted, start at a valid row boundary, and point at the first byte of a record, not at the delimiter itself (see `docs/writing-adapters.md` for the CSV example with `i + 1` after `\n`).

## RecordParser

```rust
pub trait RecordParser: Send + Sync {
    fn validate(&self, bytes: &[u8]) -> Result<()>;
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()>;
}
```

* `validate` is called once per chunk before `parse_chunk`. For stringly formats it is `simdutf8::basic::from_utf8(bytes)?` (SIMD). For typed formats it may be a no op.

* `parse_chunk` turns a chunk into `begin_row`, `put_field`, `end_row` calls on the sink. It must not call `end_row` for a partial trailing row; the engine discards it via `normalize`. It should handle sparse rows (skip missing) and last write wins is handled by the sink, not the parser.

Parsers never see `ExecutionPlan`. They emit raw field names as they appear in the format. The sink resolves them.

## ColumnarSink

```rust
pub trait ColumnarSink {
    fn begin_row(&mut self);
    fn put_field(&mut self, name: &str, value: Value<'_>);
    fn end_row(&mut self);
    fn wants(&self, _name: &str) -> bool { true }
    fn resolve<'a>(&'a self, name: &'a str) -> Option<&'a str> { Some(name) }
    fn put_field_resolved(&mut self, resolved_name: &str, value: Value<'_>) { self.put_field(resolved_name, value) }
    fn needs_value(&self) -> bool { true }
    fn needs_resolve(&self) -> bool { true }
    fn finish(&mut self) -> Result<RecordBatch>;
}
```

This is the event sink that decoders drive.

* `begin_row` and `end_row` bracket a row. `TableBuilder` uses `row_count` plus `row_dirty` bitmask to track the row, so `begin_row` is a no op.

* `put_field(&mut self, name: &str, value: Value<'_>)` resolves `name` via `ExecutionPlan::resolve_field` (rename then drop) and stores the value. If `resolve` returns `None` (dropped), it returns immediately.

* `put_row(&mut self, fields: &[(&str, Value<'_>)])` is a convenience for adapters that already have all fields as slices. It iterates `fields` and calls `put_field` for each. Default implementation.

* `resolve_raw<'a>(&'a self, raw_name: &'a [u8]) -> Option<&'a str>` resolves a field name that is still in raw byte form (e.g. from XML scanners that have not yet decoded to `&str`). Default converts via `from_utf8` then delegates to `resolve`. Avoids an intermediate `String` allocation when the parser holds raw bytes.

* `resolve_and_put_raw(&mut self, raw_name: &[u8], value: Value<'_>)` combines `resolve_raw` and `put_field_resolved` in one call for raw-byte scanners. Default converts via `from_utf8` then delegates to `resolve_and_put`.

* `resolve_and_put(&mut self, name: &str, value: Value<'_>)` resolves and pushes in one call, avoiding the double `resolve_field` that `wants` + `put_field` would perform. Default resolves then calls `put_field_resolved` (with an owned clone to satisfy the borrow checker). `TableBuilder` overrides to bypass the allocation.

* `wants(&self, name: &str) -> bool` is the hint: return false to signal the engine will drop this field. Default is true. Adapters that do expensive extraction (entity unescaping, base64, decompression) call `if sink.wants(col) { /* decode */ sink.put_field(col, val) }` to skip work.

* `resolve<'a>(&'a self, name: &'a str) -> Option<&'a str>` is the single lookup version. Default returns `Some(name)` (keep as is). `TableBuilder` overrides to `self.plan.resolve_field(name)` which returns `Some(resolved)` or `None` for dropped, borrowing from `field_map` where possible. This lets adapters do `if let Some(r) = sink.resolve(k) { /* expensive decode */ sink.put_field_resolved(r, v) }` with one hash instead of two (`wants` plus `put_field`).

* `put_field_resolved(&mut self, resolved_name: &str, value: Value<'_>)` pushes a field that is already resolved. Default delegates to `put_field` (which will resolve again). `TableBuilder` overrides to `push_field_resolved` which calls `ensure_column_idx` directly and sets the dirty bit for column `i` without re hashing `field_map`/`drop_fields`. This is the fast path for adapters that already called `resolve`.

* `needs_value(&self) -> bool` controls whether the parser decodes values. Default is `true`. When `false`, the parser skips value extraction entirely (e.g. `raw_text_until` in XML scanners) and does not call `put_field`. Adapters must NOT decode values when `needs_value()` returns `false`; the parser will emit `put_field` with an empty value or skip the call entirely. This enables locate-only and traversal-only tiers for profiling.

* `needs_resolve(&self) -> bool` controls whether the parser resolves field names. Default is `true`. When `false`, the parser skips `wants()` and `resolve()` calls entirely; it only locates the byte extents of each field within a row. Adapters must NOT call `wants()` or `resolve()` when `needs_resolve()` returns `false`. Combined with `needs_value() = false`, this gives a pure traversal tier that measures XML tree walking cost without any sink interaction.

* `finish(&mut self) -> Result<RecordBatch>` finalizes the sink. For `TableBuilder` it does `normalize`, early `new_empty` if no columns, `auto_dict_upgrade`, `sort_columns`, and builds `Schema` plus arrays.

### Why multiple APIs for the same thing

`wants` plus `put_field` is backward compatible and simple for stringly adapters (CSV header loop). `resolve` plus `put_field_resolved` is the same semantics with one hash instead of two, and it avoids the extra `String` allocation in `push_field` when `field_map` is non empty. `resolve_and_put` combines both in one call. For raw-byte scanners (XML), `resolve_raw` and `resolve_and_put_raw` avoid the `from_utf8` + `to_owned` two-step. `put_row` is a batch convenience for adapters that already have fields as slices. The Python fusion layer and `merge.rs` already use the single lookup Vec path; adapters can choose any pair and the engine guarantees the same result. Tests in `tests/data_integrity_test.rs` (`resolve_put_field_resolved_identical_to_put_field`) assert bit identical batches across `LineParser` vs `LineParserResolved` for 1000 rows with rename, drop, filter, and typed columns across single, parallel, and bounded modes.

## Value

`crates/rypipe-core/src/value.rs` (`Value<'a>`):

```rust
pub enum Value<'a> {
    Str(&'a str),
    Int64(i64),
    Float64(f64),
    Bool(bool),
    Date32(i32),
    Timestamp(i64),
    Null,
}
```

`Str(&str)` borrows from the input buffer (zero allocation for stringly formats). Typed variants let JSON adapters emit native numbers without string round tripping. `ColumnBuilder::push_value` handles cross type coercion (for example `Int64` into `Float64` widens, into `String` stringifies).

## Typical adapter loop

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let text = std::str::from_utf8(bytes).map_err(|e| crate::Error::Plan(e.to_string()))?;
    for line in text.lines() {
        if line.is_empty() { continue; }
        sink.begin_row();
        for (col, value) in self.header.iter().zip(line.split(',')) {
            // Simple path:
            // if sink.wants(col) { sink.put_field(col, Value::Str(value)); }

            // Fast path when extraction is expensive:
            if let Some(resolved) = sink.resolve(col) {
                // ... heavy decode of `value` ...
                sink.put_field_resolved(resolved, Value::Str(value));
            }
        }
        sink.end_row();
    }
    Ok(())
}
```

Either pattern is correct. The second saves one `ExecutionPlan::resolve_field` hash per field when a filter or rename is active.

## Performance decomposition (measured)

The six-tier scanner ladder isolates each cost layer additively. Measured on `test_1gb.xml` (1024 MB, 926k rows, 10 cols, ~116 bytes/field) in release mode with lto, median-of-7. All times are cumulative; deltas are derived from consecutive tiers to avoid double-counting:

```
scan_only    0.066 ms/MB  (15,188 MB/s)  ─ row boundary scan
traverse     0.634         (1,578 MB/s)  ─ +0.568 = XML walk + field extents
locate       0.645         (1,550 MB/s)  ─ +0.011 (noise) = field-name resolution
push_only    1.267         (  789 MB/s)  ─ +0.622 = per-field push (ensure_column_idx + push_value)
build_only   1.343         (  745 MB/s)  ─ +0.076 = finish_row (null-fill, dirty mask, filter)
full_parse   1.417         (  723 MB/s)  ─ +0.074 = Arrow export (finish → to_arrow memcpy)
```

Derived shares (against measured 1.417 ms/MB total):

| Phase | ms/MB | cycles/field | cycles/byte | Share |
|---|---|---|---|---|
| scan | 0.066 | 28 | 0.2 | 4.7% |
| traverse | 0.568 | 238 | 2.1 | 40.1% |
| locate | ≤0 (noise) | ≤5 | ≤0.04 | 0.8% |
| **per-field push** | **0.622** | **261** | **2.3** | **43.9%** |
| finish_row | 0.076 | 32 | 0.3 | 5.4% |
| Arrow export | 0.074 | 31 | 0.3 | 5.2% |
| **total** | **1.417** | **595** | **5.1** | **100%** |

### The sixth rung: per-field push vs per-row finalization

The `push_only` tier runs the full push path (`ensure_column_idx` + `push_value`) but skips `finish_row` (null-fill, dirty-mask clear, filter check). The sixth rung splits the old "extract+sink" 52% into:
- **Per-field push (44%, 261 cyc/f):** `ensure_column_idx` FxHash probe + `push_value` into `StrColumn` (data.extend_from_slice + offsets.push + validity.push)
- **finish_row (5%, 32 cyc/f):** null-fill + dirty-mask clear + filter check
- **Arrow export (5%, 31 cyc/f):** `finish()` → `to_arrow` memcpy

The 261 cycles/field in per-field push is the dominant unexplained cost. Expected from first principles: ~25 cycles/field. **235 cycles/field unaccounted for**; a 10× gap, likely L1 cache thrashing from 30 concurrent write streams (10 columns × 3 buffers). Column diagnostics confirm no reallocation (production `estimated_rows = bytes.len() / 512` over-allocates by 2.3×), but 440 MB allocated for 100 MB used means data buffers are spread across many pages.

### Why FieldId perfect hash is off the roadmap permanently

Field-name resolution (`wants` + `resolve`, two `FxHashMap` probes per field) is ≤3% of parse time on real data; below the measurement noise floor. The locate tier's delta is zero within noise. The ceiling is too low for a perfect hash to measurably improve.

### Tier design via `needs_value()` / `needs_resolve()`

The `ColumnarSink` trait exposes two opt-out knobs:

| Tier | `needs_value()` | `needs_resolve()` | What the parser does |
|------|----------------|-------------------|---------------------|
| scan_only | | | Pure `memmem` row boundary scan, no parser |
| traverse | `false` | `false` | Walk XML tree, find field extents, skip resolve + sink |
| locate | `false` | `true` | + `wants()` + `resolve()` via `ExecutionPlan`, no `put_field` |
| push_only | `true` | `true` | + extract text, `put_field` with push_value, skip finish_row |
| build_only | `true` | `true` | + `finish_row` (null-fill, dirty mask, filter) |
| full | `true` | `true` | + extract text, `put_field`, Arrow sink via `finish()` |

Each tier is a strict superset of the one above. Cross-tier assertions verify that `row_count` and `field_count` match across all tiers.

### Caveats

- The scan_only tier uses `memmem::find(row_tag)`, which is a different algorithm from the parser's row scan. It measures the theoretical floor for row-boundary detection.
- The synthetic test (90 MB, 5 fields/row, ~22 bytes/field) produces different shares than real data (~116 bytes/field, 10 cols). Field-dense synthetics exaggerate per-field traversal costs.
- `needs_resolve()` is `#[doc(hidden)]`; it has exactly one consumer (the benchmark harness) and no stable public use case yet.
