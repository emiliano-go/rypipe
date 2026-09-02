# Engine: TableBuilder

`TableBuilder` (`engine/table_builder.rs`) is the central structure. It implements
`ColumnarSink` and is the only production sink that most adapters need. Every
row passes through it; every column is stored in it; every Arrow array is
exported from it.

See [Data flow](./data-flow.md) for how `TableBuilder` is created and called
in each execution mode.

## Structure

```rust
pub struct TableBuilder {
    columns: Vec<ColumnBuilder>,
    field_index: HashMap<String, usize>,
    column_order: Vec<String>,
    row_count: usize,
    estimated_rows: usize,
    plan: Arc<ExecutionPlan>,
    row_dirty: Vec<u64>,
    frozen: Option<Arc<FrozenSchema>>,
    unknown_error: Option<String>,
    row_buf: Option<Box<RowBuffer>>,
    ordinal_expect: Vec<Option<(u32, Vec<u8>)>>,
    current_ordinal: u32,
}
```

### Field explanations

- **`columns: Vec<ColumnBuilder>`** — Dense column storage. Indexing
  `columns[idx]` is a bounds-checked array access, not a hash probe. This
  replaces the earlier `HashMap<String, ColumnBuilder>` that required two
  hashes per field. See [Optimizations](./optimizations.md).

- **`field_index: HashMap<String, usize>`** — Maps resolved column name to
  `Vec` index. One hash per field in steady state. Uses `FxHashMap`
  (rustc_hash) for speed on short strings.

- **`column_order: Vec<String>`** — Records first appearance order, then
  reordered by `schema_order` in `sort_columns`. Independent of `Vec` order.
  `schema_insert_index` computes insertion position for new columns.

- **`row_count: usize`** — Number of committed rows. A row is not counted
  until `finish_row` succeeds (including filter evaluation).

- **`row_dirty: Vec<u64>`** — Bitmask word array. `(columns.len() + 63) / 64`
  words. A set bit at column `i` means the column received a value in the
  current uncommitted row. Enables null-fill of only missing columns and
  avoids per-column `while len < target` checks.

- **`estimated_rows: usize`** and **`plan: Arc<ExecutionPlan>`** — Carried
  from `Pipeline::with_plan` for capacity hints and per-row decisions.

- **`frozen: Option<Arc<FrozenSchema>>`** — When set (parallel streaming),
  enforces that no unknown fields appear. Discovered via sampled windows.

- **`row_buf: Option<Box<RowBuffer>>`** — Predicate-first buffer. Only
  allocated when `plan.filter` is `Some`. Boxed to avoid 1 KB of inline
  SmallVec in every unfiltered `TableBuilder`.

- **`ordinal_expect: Vec<Option<(u32, Vec<u8>)>>`** — Per-ordinal layout
  cache for the expect_slot fast path. Populated on first row.

- **`current_ordinal: u32`** — Tracks which ordinal is being processed
  within the current row.

## Constructors

- **`new()`** — Empty, default plan.
- **`with_capacity(cap)`** — Pre-sizes column storage.
- **`with_plan(cap, plan)`** — Pre-sizes with a specific plan. The filter
  plan determines whether `row_buf` is allocated.

All constructors initialize the three vectors and the map as empty.

## Core row protocol

Adapters call `begin_row`, `put_field` (or `put_field_resolved`), `end_row`.
The engine implements these as:

### begin_row

```rust
fn begin_row(&mut self) {
    self.current_ordinal = 0;
    // Clear row_buf if present, reset predicate state
}
```

No-op for the common case (no filter). Row boundaries are tracked by
`row_count` and `row_dirty`.

### push_field (called by put_field)

```rust
fn push_field(&mut self, name: &str, value: Value<'_>) {
    // Fast path: no rename/drop configured
    if self.plan.field_map.is_empty() && self.plan.drop_fields.is_empty() {
        self.push_field_resolved(name, value);
        return;
    }
    // Resolve via plan: rename then drop
    if let Some(resolved) = self.plan.resolve_field(name) {
        self.push_field_resolved(resolved, value);
    }
}
```

### push_field_resolved (the hot path)

```rust
fn push_field_resolved(&mut self, resolved_name: &str, value: Value<'_>) {
    let idx = self.ensure_column_idx(resolved_name);
    // Set dirty bit
    let word = idx / 64;
    let bit = idx % 64;
    self.row_dirty[word] |= 1u64 << bit;
    // Last-write-wins: pop if column already has a value for this row
    let b = &mut self.columns[idx];
    if b.len() > self.row_count {
        b.pop();
    }
    b.push_value(value);
}
```

### ensure_column_idx

```rust
fn ensure_column_idx(&mut self, name: &str) -> usize {
    if let Some(&idx) = self.field_index.get(name) {
        return idx;
    }
    // New column: create builder, backfill nulls, insert
    let est = self.estimated_rows.max(64);
    let col_type = self.plan.column_type(name);
    let mut b = ColumnBuilder::with_capacity(est, &col_type);
    for _ in 0..self.row_count {
        b.push(None);
    }
    let idx = self.columns.len();
    self.columns.push(b);
    self.field_index.insert(name.to_owned(), idx);
    // Ensure row_dirty has enough words
    let needed = self.columns.len().div_ceil(64);
    if self.row_dirty.len() < needed {
        self.row_dirty.push(0);
    }
    // Insert into column_order at schema position
    let order_idx = self.schema_insert_index(name);
    self.column_order.insert(order_idx, name.to_owned());
    idx
}
```

### finish_row (the dirty optimization)

Instead of looping over all columns and pushing `None` for missing ones, the
engine uses the `row_dirty` bitmask:

```rust
fn finish_row(&mut self) {
    for (i, b) in self.columns.iter_mut().enumerate() {
        let word = i / 64;
        let bit = i % 64;
        if (self.row_dirty[word] >> bit) & 1 == 0 {
            b.push(None);  // Column missing: null fill
        } else {
            self.row_dirty[word] &= !(1u64 << bit);  // Clear for next row
        }
    }
    // Evaluate filter if present
    if let Some(ref filter) = self.plan.filter {
        if !filter.check(&self.columns, &self.field_index, self.row_count, &self.plan) {
            for b in &mut self.columns {
                b.pop();
            }
            return;
        }
    }
    self.row_count += 1;
}
```

For 10 columns where 8 are present each row, this saves 80% of null-fill
pushes. The bitmask word load plus bit test is cheaper than a `Vec` push
per column.

### finish (Arrow export)

```rust
fn finish(&mut self) -> Result<RecordBatch> {
    self.normalize();
    if self.column_order.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }
    self.auto_dict_upgrade();
    self.sort_columns();
    let fields: Vec<ArrowField> = self.column_order.iter()
        .filter_map(|name| {
            let idx = self.field_index.get(name)?;
            let b = &self.columns[*idx];
            Some(ArrowField::new(name, b.arrow_datatype(), true))
        })
        .collect();
    let arrays: Vec<ArrayRef> = self.column_order.iter()
        .filter_map(|name| {
            let idx = self.field_index.get(name)?;
            let b = &mut self.columns[*idx];
            Some(b.to_arrow_array()?)
        })
        .collect();
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}
```

## Predicate-first evaluation

When a filter is active, `RowBuffer` holds `(slot, Value<'static>)` pairs
instead of pushing to columns. After each field, the predicate is evaluated
against the buffered values. If it passes, the engine switches to direct mode
and drains the buffer.

The adaptive strategy: if the predicate column appears late (> 4/5 of
columns), buffering is a net loss. The engine switches to direct push +
pop-on-reject.

See [Optimizations](./optimizations.md) for the full predicate-first design.

## Layout prediction (expect_slot)

After the first row, the engine caches `(slot, raw_name_bytes)` per ordinal.
On subsequent rows, the adapter calls `expect_slot(ordinal)` and compares
raw bytes via memcmp. On match, `put_field_at(slot, value)` pushes directly.

See [Decoder API](./decoder.md) for the adapter-side interface.

## Invariants

- `columns.len() == field_index.len()` always
- `row_dirty.len() == (columns.len() + 63) / 64` always
- After `finish_row`: all dirty bits are clear
- `take_column` keeps vectors in sync via swap_remove patching

## Tests

Inside `engine::tests`: `LineParser` plus `LineSplitter` exercise the same
`put_field` path used by real adapters. Tests cover `extend`, last-write-wins,
rename, drop, filter eq/ne/missing, typed columns, dictionary, and
`apply_compare_filter`.
