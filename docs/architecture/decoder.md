# Decoder API

`crates/rypipe-core/src/decoder.rs` (63 lines) defines the boundary between format specific and format agnostic code. Adapters implement two traits; the engine implements the third.

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
    fn finish(&mut self) -> Result<RecordBatch>;
}
```

This is the event sink that decoders drive.

* `begin_row` and `end_row` bracket a row. `TableBuilder` uses `row_count` plus `row_dirty` to track the row, so `begin_row` is a no op.

* `put_field(&mut self, name: &str, value: Value<'_>)` resolves `name` via `ExecutionPlan::resolve_field` (rename then drop) and stores the value. If `resolve` returns `None` (dropped), it returns immediately.

* `wants(&self, name: &str) -> bool` is the hint: return false to signal the engine will drop this field. Default is true. Adapters that do expensive extraction (entity unescaping, base64, decompression) call `if sink.wants(col) { /* decode */ sink.put_field(col, val) }` to skip work.

* `resolve<'a>(&'a self, name: &'a str) -> Option<&'a str>` is the single lookup version. Default returns `Some(name)` (keep as is). `TableBuilder` overrides to `self.plan.resolve_field(name)` which returns `Some(resolved)` or `None` for dropped, borrowing from `field_map` where possible. This lets adapters do `if let Some(r) = sink.resolve(k) { /* expensive decode */ sink.put_field_resolved(r, v) }` with one hash instead of two (`wants` plus `put_field`).

* `put_field_resolved(&mut self, resolved_name: &str, value: Value<'_>)` pushes a field that is already resolved. Default delegates to `put_field` (which will resolve again). `TableBuilder` overrides to `push_field_resolved` which calls `ensure_column_idx` directly and sets `row_dirty[idx] = true` without re hashing `field_map`/`drop_fields`. This is the fast path for adapters that already called `resolve`.

* `finish(&mut self) -> Result<RecordBatch>` finalizes the sink. For `TableBuilder` it does `normalize`, early `new_empty` if no columns, `auto_dict_upgrade`, `sort_columns`, and builds `Schema` plus arrays.

### Why two APIs for the same thing

`wants` plus `put_field` is backward compatible and simple for stringly adapters (CSV header loop). `resolve` plus `put_field_resolved` is the same semantics with one hash instead of two, and it avoids the extra `String` allocation in `push_field` when `field_map` is non empty (`owned = n.to_owned()`). The Python fusion layer and `merge.rs` already use the single lookup Vec path; adapters can choose either pair and the engine guarantees the same result. Tests in `tests/data_integrity_test.rs` (`resolve_put_field_resolved_identical_to_put_field`) assert bit identical batches across `LineParser` vs `LineParserResolved` for 1000 rows with rename, drop, filter, and typed columns across single, parallel, and bounded modes.

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
