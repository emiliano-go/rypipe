# Engine: TableBuilder { #engine-tablebuilder }

`TableBuilder` (`engine/table_builder.rs`) is the central structure. It implements
`ColumnarSink` and is the only production sink that most adapters need. Every
row passes through it; every column is stored in it; every Arrow array is
exported from it.

See [Data flow](./data-flow.md) for how `TableBuilder` is created and called
in each execution mode.

## Structure { #structure }

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
    cached_wanted_mask: u64,
    ordinal_expect: Vec<Option<(u32, Vec<u8>)>>,
    current_ordinal: u32,
    strict_error: Option<crate::Error>,
    rows_accepted: usize,
    rows_rejected: usize,
}
```

### Field explanations { #field-explanations }

- **`columns: Vec<ColumnBuilder>`**: Dense column storage. Indexing
  `columns[idx]` is a bounds-checked array access, not a hash probe. This
  replaces the earlier `HashMap<String, ColumnBuilder>` that required two
  hashes per field. See [Optimizations](./optimizations.md).

- **`field_index: HashMap<String, usize>`**: Maps resolved column name to
  `Vec` index. One hash per field in steady state. Uses `FxHashMap`
  (rustc_hash) for speed on short strings.

- **`column_order: Vec<String>`**: Records first appearance order, then
  reordered by `schema_order` in `sort_columns`. Independent of `Vec` order.
  `schema_insert_index` computes insertion position for new columns.

- **`row_count: usize`**: Number of committed rows. A row is not counted
  until `finish_row` succeeds (including filter evaluation).

- **`row_dirty: Vec<u64>`**: Bitmask word array. `(columns.len() + 63) / 64`
  words. A set bit at column `i` means the column received a value in the
  current uncommitted row. Enables null-fill of only missing columns and
  avoids per-column `while len < target` checks.

- **`estimated_rows: usize`** and **`plan: Arc<ExecutionPlan>`**: Carried
  from `Pipeline::with_plan` for capacity hints and per-row decisions.

- **`frozen: Option<Arc<FrozenSchema>>`**: When set (parallel streaming),
  enforces that no unknown fields appear. Discovered via sampled windows.

- **`row_buf: Option<Box<RowBuffer>>`**: Predicate-first buffer. Only
  allocated when `plan.filter` is `Some`. Boxed to avoid 1 KB of inline
  SmallVec in every unfiltered `TableBuilder`.

- **`cached_wanted_mask: u64`**: Projection short-circuit mask for
  `wanted_mask()`/`row_satisfied()`. Bit `i` is set iff column `i` is wanted
  by the schema projection (declared columns plus filter-referenced fields).
  Computed once in `with_plan` after `predeclare_schema_columns`, so it is
  complete from the first row. 0 when no projection is active.

- **`strict_error: Option<crate::Error>`**: First `strict_types` violation,
  recorded during puts (the `ColumnarSink` trait is infallible) and surfaced
  as an error by `finish()`.

- **`rows_accepted` / `rows_rejected: usize`**: Row counters since the last
  `finish()`/`reset()`, feeding the observer's `on_chunk_finished`.

The plan carries an optional `observer: Arc<dyn RowObserver>` with
`on_begin_row`, `on_put_field`, `on_row_accepted`, `on_row_rejected`, and
`on_chunk_finished` callbacks; the pseudocode below shows where each fires.

- **`ordinal_expect: Vec<Option<(u32, Vec<u8>)>>`**: Per-ordinal layout
  cache for the expect_slot fast path. Populated on first row.

- **`current_ordinal: u32`**: Tracks which ordinal is being processed
  within the current row.

## Constructors { #constructors }

- **`new()`**: Empty, default plan.
- **`with_capacity(cap)`**: Pre-sizes column storage.
- **`with_plan(cap, plan)`**: Pre-sizes with a specific plan. The filter
  plan determines whether `row_buf` is allocated. Also calls
  `predeclare_schema_columns()` and computes `cached_wanted_mask`.

`new()` and `with_capacity()` initialize the three vectors and the map as
empty. `with_plan` additionally populates `columns`, `field_index`, and
`column_order` with all declared schema columns plus filter-referenced
fields when a schema projection is active.

## Core row protocol { #core-row-protocol }

Adapters call `begin_row`, `put_field` (or `put_field_resolved`), `end_row`.
The engine implements these as:

### begin_row { #begin_row }

```rust
fn begin_row(&mut self) {
    if let Some(ref obs) = self.plan.observer {
        obs.on_begin_row(self.row_count);
    }
    self.current_ordinal = 0;
    if let Some(ref mut buf) = self.row_buf {
        // Adaptive decision: once row_count > 0, if the predicate column's
        // ordinal is >= 4/5 of the column count, buffering is a net loss.
        if buf.predicate_ordinal.is_some() && buf.buffer_worthwhile && self.row_count > 0 {
            // ncols: frozen schema, else schema_order len, else columns.len()
            if buf.predicate_ordinal.unwrap() >= ncols * 4 / 5 {
                buf.buffer_worthwhile = false;
            }
        }
        buf.fields.clear();
        buf.state = PredicateState::Undecided;
        buf.direct = false;
    }
}
```

No-op for the common case (no filter, no observer). Row boundaries are
tracked by `row_count` and `row_dirty`.

### push_field (called by put_field) { #push_field }

```rust
fn push_field(&mut self, name: &str, value: Value<'_>) {
    // Fast path: no rename/drop/projection configured
    if self.plan.field_map.is_empty()
        && self.plan.drop_fields.is_empty()
        && self.plan.schema_order.is_empty()
    {
        self.push_field_resolved(name, value);
        return;
    }
    // Try zero-allocation fast path: column already exists
    if let Some(idx) = Self::resolve_and_slot(&self.plan, &self.field_index, name) {
        // Track ordinal for expect_slot on first row
        if self.row_count == 0 {
            let ord = self.current_ordinal as usize;
            if ord >= self.ordinal_expect.len() {
                self.ordinal_expect.resize_with(ord + 1, || None);
            }
            let resolved = self.plan.resolve_field(name).unwrap_or(name);
            self.ordinal_expect[ord] = Some((idx as u32, resolved.as_bytes().to_vec()));
        }
        self.current_ordinal += 1;
        // Set dirty bit, handle last-write-wins, push
        let word = idx / 64;
        let bit = idx % 64;
        self.row_dirty[word] |= 1u64 << bit;
        let b = &mut self.columns[idx];
        if b.len() > self.row_count { b.pop(); }
        b.push_value(value);
    } else {
        // Column doesn't exist yet or field was dropped
        if let Some(resolved) = self.plan.resolve_field(name) {
            let owned = resolved.to_owned();
            self.push_field_resolved(&owned, value);
        }
    }
}
```

### push_field_resolved (the hot path) { #push_field_resolved }

```rust
fn push_field_resolved(&mut self, resolved_name: &str, value: Value<'_>) {
    // Frozen exact schema: unknown field sets unknown_error and returns
    if self.frozen.is_some() && !self.field_index.contains_key(resolved_name) {
        if frozen.is_exact() {
            self.unknown_error.get_or_insert(...);
            return;
        }
    }
    let idx = self.ensure_column_idx(resolved_name);
    self.record_strict_violation_at(idx, &value);  // strict_types check
    self.notify_put_field(idx, &value);            // observer.on_put_field
    // Track ordinal→slot mapping on first row for expect_slot fast path
    if self.row_count == 0 {
        self.ordinal_expect[self.current_ordinal as usize] =
            Some((idx as u32, resolved_name.as_bytes().to_vec()));
    }
    self.current_ordinal += 1;
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

### ensure_column_idx { #ensure_column_idx }

```rust
fn ensure_column_idx(&mut self, name: &str) -> usize {
    if let Some(&idx) = self.field_index.get(name) {
        return idx;
    }
    // Frozen exact schema: unknown field sets unknown_error, returns dummy 0
    if self.frozen.is_some() && frozen.is_exact() {
        self.unknown_error.get_or_insert(...);
        return 0;
    }
    // New column: create builder, backfill nulls, insert
    let est = self.estimated_rows.max(64);
    let col_type = self.plan.column_type(name);
    let mut b = ColumnBuilder::with_capacity(est, &col_type);
    // Mid-row predicate drain already incremented row_count, so backfill
    // one less null in that case.
    let backfill = if row_buf.direct {
        self.row_count.saturating_sub(1)
    } else {
        self.row_count
    };
    for _ in 0..backfill {
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

### finish_row (the dirty optimization) { #finish_row }

Instead of looping over all columns and pushing `None` for missing ones, the
engine uses the `row_dirty` bitmask with a fast path for dense rows:

```rust
fn finish_row(&mut self) {
    let ncols = self.columns.len();
    let full_words = ncols / 64;
    let rem_bits = ncols % 64;
    // Fast path: all bits set (dense row)
    let is_full = (0..full_words).all(|w| self.row_dirty[w] == u64::MAX)
        && (rem_bits == 0
            || self.row_dirty.get(full_words).copied().unwrap_or(0)
               == (1u64 << rem_bits) - 1);
    if is_full {
        self.row_dirty.fill(0);
    } else {
        for (i, b) in self.columns.iter_mut().enumerate() {
            let word = i / 64;
            let bit = i % 64;
            if (self.row_dirty[word] >> bit) & 1 == 0 {
                b.push(None);  // Column missing: null fill
            }
        }
        self.row_dirty.fill(0);
    }
    // Evaluate filter if present
    if let Some(ref filter) = self.plan.filter {
        if !filter.check(&self.columns, &self.field_index, self.row_count, &self.plan) {
            for b in &mut self.columns { b.pop(); }
            self.rows_rejected += 1;
            if let Some(ref obs) = self.plan.observer {
                obs.on_row_rejected(self.row_count);
            }
            return;
        }
    }
    self.row_count += 1;
    self.rows_accepted += 1;
    if let Some(ref obs) = self.plan.observer {
        obs.on_row_accepted(self.row_count - 1);
    }
}
```

The bitmask avoids a `for col in 0..ncols` length check per column: only
missing columns get a `None` push, and the fast path skips the loop entirely
for dense rows.

### finish (Arrow export) { #finish }

```rust
fn finish(&mut self) -> Result<RecordBatch> {
    // Surface deferred errors from the infallible put path
    if let Some(err) = self.unknown_error.take() {
        return Err(crate::Error::Merge(err));
    }
    if let Some(err) = self.strict_error.take() {
        return Err(err);
    }
    if let Some(ref obs) = self.plan.observer {
        obs.on_chunk_finished(
            self.rows_accepted + self.rows_rejected,
            self.rows_accepted,
            self.rows_rejected,
        );
    }
    self.normalize();
    if self.column_order.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }
    self.auto_dict_upgrade();
    // Schema projection: emit exactly the declared schema_order columns,
    // in that order. Declared-but-unseen fields become all-null columns;
    // filter-only columns are projected out. No sort_columns() here.
    if !self.plan.schema_order.is_empty() {
        for name in &self.plan.schema_order {
            // skip if in drop_fields; create all-null column if never seen;
            // push ArrowField + to_arrow_array()
        }
        return Ok(RecordBatch::try_new(schema, arrays)?);
    }
    self.sort_columns();
    for name in &self.column_order {
        if let Some(&idx) = self.field_index.get(name.as_str()) {
            fields.push(ArrowField::new(name, columns[idx].arrow_datatype(), true));
            arrays.push(columns[idx].to_arrow_array()?);
        }
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}
```

## Predicate-first evaluation { #predicate-first-evaluation }

When a filter is active, `RowBuffer` holds `(slot, Value<'static>)` pairs
instead of pushing to columns. Each arriving field is buffered; only when the
field is a predicate column (`is_predicate_slot`) is the predicate evaluated
against the buffered values. If it passes, the engine switches to direct mode
and drains the buffer.

The adaptive strategy: if the predicate column appears late (ordinal >= 4/5
of columns), buffering is a net loss. The check runs in `begin_row`, only
after the first committed row, and the engine then switches to direct push +
pop-on-reject.

See [Optimizations](./optimizations.md) for the full predicate-first design.

## Layout prediction (expect_slot) { #layout-prediction }

After the first row, the engine caches `(slot, raw_name_bytes)` per ordinal.
On subsequent rows, the adapter calls `expect_slot(ordinal)` and compares
raw bytes via memcmp. On match, `put_field_at(slot, value)` pushes directly.

See [Decoder API](./decoder.md) for the adapter-side interface.

## Invariants { #invariants }

- `columns.len() == field_index.len()` always
- `row_dirty.len() >= (columns.len() + 63) / 64` (equality holds except
  transiently after `take_column`, which resizes before shrinking `columns`)
- After `finish_row`: all dirty bits are clear
- `take_column` keeps vectors in sync via swap_remove patching

## Tests { #tests }

Inside `engine::tests`: `LineParser` plus `LineSplitter` exercise the same
`put_field` path used by real adapters. Tests cover `extend`, last-write-wins,
rename, drop, filter eq/ne/missing, typed columns, dictionary, and
`apply_compare_filter`.
