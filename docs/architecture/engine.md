# Engine: TableBuilder

`TableBuilder` (`crates/rypipe-core/src/engine.rs:16`) is the central structure. It implements `ColumnarSink` and is the only production sink that most adapters need.

## Structure

```rust
pub struct TableBuilder {
    pub(crate) columns: Vec<ColumnBuilder>,
    pub(crate) field_index: HashMap<String, usize>,
    pub(crate) column_order: Vec<String>,
    pub(crate) row_count: usize,
    pub(crate) estimated_rows: usize,
    pub(crate) plan: Arc<ExecutionPlan>,
    pub(crate) row_dirty: Vec<u64>,
}
```

Why this shape:

* `columns: Vec<ColumnBuilder>` holds dense column storage. Indexing `columns[idx]` is a bounds checked array access, not a hash probe. This replaces the earlier `HashMap<String, ColumnBuilder>` that required two hashes per field (one in `ensure_column`, one in `get_mut`). See [Optimizations](./optimizations.md) for the before and after.

* `field_index: HashMap<String, usize>` maps resolved column name to `Vec` index. One hash per field in steady state. `FxHashMap` (rustc_hash) is used for speed on short strings.

* `column_order: Vec<String>` records first appearance order, then reordered by `schema_order` in `sort_columns`. It is independent of `Vec` order, which is insertion order. `schema_insert_index` computes the insertion position for a new column based on the desired output order.

* `row_count: usize` is the number of committed rows. A row is not counted until `finish_row` succeeds (including filter).

* `row_dirty: Vec<u64>` is a bitmask word array. `row_dirty` has `(columns.len() + 63) / 64` words. A set bit at column `i` means the column received a value in the current uncommitted row. It lets `finish_row` null fill only missing columns and avoids a per column `while len < target` check for touched columns.

* `estimated_rows: usize` and `plan: Arc<ExecutionPlan>` are carried from `Pipeline::with_plan` and used for capacity hints and per row decisions.

Constructors (`new`, `with_capacity`, `with_plan`) all initialize the three Vectors and the map as empty.

## Helpers

* `get_column(&self, name: &str) -> Option<&ColumnBuilder>` and `get_column_mut(&mut self, name: &str) -> Option<&mut ColumnBuilder>` are the single lookup path: `field_index.get(name).map(|&i| &self.columns[i])`. Tests and `merge.rs` use these instead of HashMap `get`.

* `take_column(&mut self, name: &str) -> Option<ColumnBuilder>` removes and returns ownership. It does `field_index.remove(name)` to get `idx`, then `columns.swap_remove(idx)` (or `pop` if last). If the removed index was not the last, the element that was at `last` moves to `idx`; the code finds its key in `field_index` (value `== old_last`) and repoints it to `idx`. It also keeps `row_dirty` in sync with `swap_remove` (or `pop`). This is used only by `merge::extend` where `other` is consumed.

## Core row protocol

Adapters call `begin_row`, `put_field` (or `put_field_resolved`), `end_row` in a loop. `TableBuilder` implements these as:

* `begin_row` does nothing. Row boundaries are tracked by `row_count` and `row_dirty`.

* `push_field` resolves the raw name (`plan.field_map` then `plan.drop_fields`) and delegates to `push_field_resolved`. Fast path: if both maps are empty, it uses `name` directly and avoids allocation and hashing in `resolve_field`.

* `push_field_resolved` is the hot path (see [Optimizations](./optimizations.md) for the single lookup version). It calls `ensure_column_idx(resolved)` to get `idx`, sets the dirty bit for column `idx` via `row_dirty[idx/64] |= 1u64 << (idx%64)`, then handles last write wins: if `columns[idx].len() > row_count`, the column already has a value for this row (duplicate field in the same row), so it pops before pushing the new value. Then `push_value` is called on the builder.

* `ensure_column_idx(&mut self, name: &str) -> usize` does one hash lookup. If `field_index.get(name)` exists, it returns immediately. Otherwise it creates a `ColumnBuilder::with_capacity(est, &col_type)` where `est = estimated_rows.max(64)` and `col_type = plan.column_type(name)`, backfills `row_count` nulls (`for _ in 0..row_count { b.push(None) }`), pushes to `columns`, inserts into `field_index`, clears the dirty bit for the new column (ensuring `row_dirty` has enough words), and inserts into `column_order` at `schema_insert_index(name)`.

* `finish_row` is where the dirty optimization matters (2C-S1). Instead of looping over all columns and doing `while b.len() < target { b.push(None) }`, it does:

```rust
// bitmask version: row_dirty is Vec<u64> with (columns.len()+63)/64 words
let full_words = self.row_dirty.len() - 1;
let rem_bits = self.columns.len() % 64;
for (i, b) in self.columns.iter_mut().enumerate() {
    let word = i / 64;
    let bit = i % 64;
    if (self.row_dirty[word] >> bit) & 1 == 0 {
        b.push(None);
    } else {
        self.row_dirty[word] &= !(1u64 << bit);
    }
}
if let Some(ref filter) = self.plan.filter {
    if !filter.check(&self.columns, &self.field_index, self.row_count, &self.plan) {
        for b in &mut self.columns { b.pop(); }
        return;
    }
}
self.row_count += 1;
```

Only missing columns get a `push(None)`; touched columns are just cleared for the next row. For 10 columns where 8 are present each row, this saves 80% of the null fill pushes and the associated `len` checks. The bitmask word load plus bit test is cheaper than a `Vec` push per column.

Filter is evaluated per row via `FilterPredicate::check` with `(&columns, &field_index, row_index, &plan)`. If it fails, each column is popped (undoing the row) and `row_count` is not advanced. Dirty was already cleared, so the next row starts clean. For `And`/`Or`/`Not` trees, `check` short circuits.

## Other methods

* `reset` clears `columns`, `field_index`, `column_order`, `row_dirty`, and resets `row_count` to zero while keeping `plan` and `estimated_rows`.

* `normalize` truncates any column with `len > row_count` (partial row from a truncated chunk) and zeros `row_dirty`. Idempotent.

* `auto_dict_upgrade` iterates `&mut self.columns` and calls `try_upgrade_to_dict(512, max_ratio, max_size)` when `plan.auto_dict` is true. Threshold defaults are 0.05 ratio and 256 entries. It is called from `finish` before sorting.

* `sort_columns` reorders `column_order` by `schema_order` rank. It does not reorder `columns` or `field_index`; those stay insertion ordered and are looked up by name. Only the output order changes.

* `schema_insert_index(&self, name: &str) -> usize` computes where a new column should be inserted into `column_order` to respect `schema_order`. If `schema_order` is empty, it returns `column_order.len()` (append). Otherwise it finds the position of `name` in `schema_order` and returns the position of the first existing column that appears later in that order.

* `finish(&mut self) -> Result<RecordBatch>` (also `ColumnarSink::finish`) does `normalize`, early return with `RecordBatch::new_empty` if `column_order` is empty, `auto_dict_upgrade`, `sort_columns`, then builds `fields` and `arrays` by iterating `column_order` and looking up each builder via `get_column`, calling `arrow_datatype` and `to_arrow_array`. It creates `Arc::new(Schema::new(fields))` and `RecordBatch::try_new`.

## ColumnarSink implementation

```rust
impl ColumnarSink for TableBuilder {
    fn begin_row(&mut self) {}
    fn put_field(&mut self, name: &str, value: Value<'_>) { self.push_field(name, value) }
    fn end_row(&mut self) { self.finish_row() }
    fn wants(&self, name: &str) -> bool { self.resolve(name).is_some() }
    fn resolve<'a>(&'a self, name: &'a str) -> Option<&'a str> { self.plan.resolve_field(name) }
    fn put_field_resolved(&mut self, resolved_name: &str, value: Value<'_>) { self.push_field_resolved(resolved_name, value) }
    fn finish(&mut self) -> Result<RecordBatch> { ... }
}
```

`wants` now delegates to `resolve`, so an adapter that does `if sink.wants(k) { sink.put_field(k, v) }` pays one `resolve_field` hash. The faster pattern is `if let Some(r) = sink.resolve(k) { /* expensive decode */ sink.put_field_resolved(r, v) }` which pays one hash total (see [Decoder](./decoder.md) and [Optimizations](./optimizations.md)).

## Invariants

* `columns.len() == field_index.len()` and `row_dirty.len() == (columns.len() + 63) / 64` always.
* `columns.len() == column_order.len()` after each successful `finish_row` or `extend`, but during a row `columns` may be larger than `row_count+1` before `finish_row` completes.
* `row_dirty` bit `i` is set exactly when `columns[i].len() == row_count + 1` and the column was touched this row; after `finish_row` all bits are clear.
* `take_column` keeps the three vectors in sync via swap remove patching.

## Tests

Inside `engine::tests`, `LineParser` plus `LineSplitter` exercise the same `put_field` path used by real adapters. Tests cover `extend` (no duplicates, multi chunk same as single, ragged late debut), last write wins, rename, drop, filter `eq`/`ne`/missing, typed columns, dictionary, and `apply_compare_filter`.
