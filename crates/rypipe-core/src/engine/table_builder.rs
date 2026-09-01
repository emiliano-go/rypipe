#[cfg(test)]
use std::borrow::Cow;
use std::sync::Arc;

#[cfg(feature = "profile")]
use std::sync::atomic::{AtomicUsize, Ordering};

/// Diagnostic: counts resolve_and_put calls across all TableBuilder instances.
#[cfg(feature = "profile")]
pub static RESOLVE_AND_PUT_COUNT: AtomicUsize = AtomicUsize::new(0);
/// Diagnostic: counts predicate evaluations across all TableBuilder instances.
#[cfg(feature = "profile")]
pub static PREDICATE_EVALUATIONS: AtomicUsize = AtomicUsize::new(0);
/// Diagnostic: counts how many times evaluate_predicate_state returns Fail.
#[cfg(feature = "profile")]
pub static PREDICATE_FAILS: AtomicUsize = AtomicUsize::new(0);
/// Diagnostic: counts how many times evaluate_predicate_state returns Undecided.
#[cfg(feature = "profile")]
pub static PREDICATE_UNDECIDED: AtomicUsize = AtomicUsize::new(0);
/// Diagnostic: counts how many times is_predicate_slot returns true.
#[cfg(feature = "profile")]
pub static IS_PRED_TRUE: AtomicUsize = AtomicUsize::new(0);
/// Diagnostic: counts how many times is_predicate_slot returns false.
#[cfg(feature = "profile")]
pub static IS_PRED_FALSE: AtomicUsize = AtomicUsize::new(0);

use arrow::datatypes::{Field as ArrowField, Schema};
use arrow::record_batch::RecordBatch;
use rustc_hash::FxHashMap as HashMap;

use smallvec::SmallVec;

use crate::columnar::ColumnBuilder;
use crate::decoder::ColumnarSink;
use crate::engine::predicate::{PredicateState, RowBuffer};
use crate::plan::{ExecutionPlan, FieldType, FilterPredicate};
use crate::value::Value;
use crate::Result;

/// Generic columnar table builder.  Implements `ColumnarSink` so any decoder
/// can feed it field/value events; at the end it produces an Arrow
/// `RecordBatch`.
pub struct TableBuilder {
    pub(crate) columns: Vec<ColumnBuilder>,
    pub(crate) field_index: HashMap<String, usize>,
    pub(crate) column_order: Vec<String>,
    pub(crate) row_count: usize,
    pub(crate) estimated_rows: usize,
    pub(crate) plan: Arc<ExecutionPlan>,
    /// Dirty mask for the current row: bit `i` set iff column `i` received a
    /// value in this row. `Vec<u64>` word array so >64 columns work (e.g.,
    /// Crystal Reports exports with >64 fields). One compare `mask != full`
    /// replaces `for col in 0..ncols` loop when every field is present (dense).
    pub(crate) row_dirty: Vec<u64>,
    /// Frozen schema for parallel streaming. When Some, any field not in the
    /// schema is an unknown field and hard-errors on `finish()` (data loss
    /// would otherwise be silent). Discovery samples 16×2 MiB for >128 MiB.
    pub(crate) frozen: Option<std::sync::Arc<crate::schema::FrozenSchema>>,
    pub(crate) unknown_error: Option<String>,
    /// Heap-allocated row buffer for predicate-first deferred materialization.
    /// `None` when `plan.filter` is `None` — unfiltered parses carry zero
    /// overhead from predicate machinery.
    pub(crate) row_buf: Option<Box<RowBuffer>>,
    /// Per-ordinal layout expectation: after row 1, each ordinal maps to a
    /// (slot_index, raw_name) pair.  The adapter can memcmp the raw bytes
    /// in-place instead of running the full attribute scan → UTF-8 decode →
    /// hash → lookup path.  `None` after `layout_broken` or before row 1.
    pub(crate) ordinal_expect: Vec<Option<(u32, Vec<u8>)>>,
    /// Current ordinal within the row (incremented per put_field call).
    /// Used to populate `ordinal_expect` on the first row.
    pub(crate) current_ordinal: u32,
}

impl TableBuilder {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            field_index: HashMap::default(),
            column_order: Vec::new(),
            row_count: 0,
            estimated_rows: 0,
            plan: Arc::new(ExecutionPlan::new()),
            row_dirty: Vec::new(),
            frozen: None,
            unknown_error: None,
            row_buf: None,
            ordinal_expect: Vec::new(),
            current_ordinal: 0,
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            columns: Vec::new(),
            field_index: HashMap::default(),
            column_order: Vec::new(),
            row_count: 0,
            estimated_rows: cap,
            plan: Arc::new(ExecutionPlan::new()),
            row_dirty: Vec::new(),
            frozen: None,
            unknown_error: None,
            row_buf: None,
            ordinal_expect: Vec::new(),
            current_ordinal: 0,
        }
    }

    pub fn with_plan(cap: usize, plan: Arc<ExecutionPlan>) -> Self {
        Self {
            columns: Vec::new(),
            field_index: HashMap::default(),
            column_order: Vec::new(),
            row_count: 0,
            estimated_rows: cap,
            plan: Arc::clone(&plan),
            row_dirty: Vec::new(),
            frozen: None,
            unknown_error: None,
            row_buf: if plan.filter.is_some() {
                Some(Box::new(RowBuffer {
                    fields: SmallVec::new(),
                    state: PredicateState::Undecided,
                    predicate_mask: SmallVec::new(),
                    direct: false,
                    predicate_ordinal: None,
                    buffer_worthwhile: true,
                    pred_names: SmallVec::new(),
                }))
            } else {
                None
            },
            ordinal_expect: Vec::new(),
            current_ordinal: 0,
        }
    }

    /// Pre-size all columns from a `FrozenSchema`.
    ///
    /// Called by parallel streaming workers so that every chunk's
    /// `TableBuilder` has the full column set from construction,
    /// regardless of which fields appear in that chunk.
    pub fn ensure_schema(&mut self, schema: &crate::schema::FrozenSchema) -> Result<()> {
        for (slot, name) in schema.column_names().iter().enumerate() {
            let name_str: &str = name;
            if self.field_index.contains_key(name_str) {
                continue; // already present
            }
            let ty = schema.column_types()[slot].clone();
            let col = ColumnBuilder::with_capacity(self.estimated_rows, &ty);
            self.columns.push(col);
            self.field_index
                .insert(name_str.to_string(), self.columns.len() - 1);
            self.column_order.push(name_str.to_string());
        }
        // Resize row_dirty bitmask to cover all columns.
        let words = self.columns.len().div_ceil(64);
        self.row_dirty.resize(words, 0);
        // Freeze: any later field not in this schema is an unknown field and
        // must hard-error (otherwise data loss would be silent on a default
        // path). Sampling 16×2 MiB covers ~6% of a 533 MB file, so 1% Text21
        // is expected ~280 hits, but 0.05% would be missed.
        self.frozen = Some(std::sync::Arc::new(schema.clone()));
        Ok(())
    }

    /// Lookup a column by resolved name.
    pub(crate) fn get_column(&self, name: &str) -> Option<&ColumnBuilder> {
        self.field_index.get(name).map(|&i| &self.columns[i])
    }

    /// Mutable lookup by resolved name.
    pub(crate) fn get_column_mut(&mut self, name: &str) -> Option<&mut ColumnBuilder> {
        if let Some(&i) = self.field_index.get(name) {
            Some(&mut self.columns[i])
        } else {
            None
        }
    }

    pub(crate) fn bytes_used(&self) -> usize {
        self.columns.iter().map(|c| c.bytes_used()).sum::<usize>()
            + self.column_order.iter().map(|s| s.len()).sum::<usize>()
            + self.row_dirty.len() * 8
    }

    /// Split off the first `n` rows into a new `TableBuilder`.
    ///
    /// Leaves `n` rows in `self`'s remainder as `self - n`. Used for
    /// 64KB streaming where a single file chunk may contain many more rows
    /// than `rows_per_batch`.
    pub(crate) fn split_off(&mut self, n: usize) -> Self {
        assert!(n <= self.row_count, "split_off beyond row_count");
        assert!(n > 0);
        let mut other = Self {
            columns: Vec::with_capacity(self.columns.len()),
            field_index: self.field_index.clone(),
            column_order: self.column_order.clone(),
            row_count: n,
            estimated_rows: n,
            plan: Arc::clone(&self.plan),
            row_dirty: vec![0; self.columns.len().div_ceil(64)],
            frozen: self.frozen.clone(),
            unknown_error: None,
            row_buf: if self.plan.filter.is_some() {
                Some(Box::new(RowBuffer {
                    fields: SmallVec::new(),
                    state: PredicateState::Undecided,
                    predicate_mask: self
                        .row_buf
                        .as_ref()
                        .map_or_else(SmallVec::new, |b| b.predicate_mask.clone()),
                    direct: false,
                    predicate_ordinal: None,
                    buffer_worthwhile: true,
                    pred_names: self
                        .row_buf
                        .as_ref()
                        .map_or_else(SmallVec::new, |b| b.pred_names.clone()),
                }))
            } else {
                None
            },
            ordinal_expect: Vec::new(),
            current_ordinal: 0,
        };
        for (idx, col) in self.columns.iter_mut().enumerate() {
            let drain = col.split_off(n);
            other.columns.push(drain);
            // Remainder stays in self.columns[idx]
            let _ = idx;
        }
        self.row_count -= n;
        // row_dirty for self should be all false (no dirty in remainder yet)
        self.row_dirty = vec![0; self.columns.len().div_ceil(64)];
        // row_dirty for other is also false (just finished batch)
        other
    }

    /// Remove and return a column by name, fixing the Vec index map.
    /// Used by `merge::extend` to move builders out of the `other` table.
    pub(crate) fn take_column(&mut self, name: &str) -> Option<ColumnBuilder> {
        let idx = self.field_index.remove(name)?;
        // Keep row_dirty in sync: Vec<u64> bitmask, need to handle bit removal
        // For simplicity, just rebuild row_dirty as all zeros after column removal
        // (row_dirty is per-row, not per-column persistent, so clearing is fine)
        // Rebuild to correct length
        let new_len = self.columns.len().div_ceil(64);
        self.row_dirty.resize(new_len, 0);
        // Clear all bits (no dirty in remainder for take_column case)
        self.row_dirty.fill(0);
        let _ = idx; // suppress unused warning
                     // After removal, columns.len() == old_len; last index = old_len - 1.
                     // swap_remove will move the last element into idx (if not already last).
        let last = self.columns.len() - 1;
        let col = if idx == last {
            self.columns.pop().unwrap()
        } else {
            let col = self.columns.swap_remove(idx);
            // The element that was at `last` is now at `idx`; fix its map entry.
            let old_last = self.columns.len(); // == last, new len after pop/swap
                                               // Find the key that pointed to old_last and repoint it to idx.
                                               // We must not borrow field_index mutably while iterating, so clone the key first.
            let moved_name = self.field_index.iter().find_map(|(k, &v)| {
                if v == old_last {
                    Some(k.clone())
                } else {
                    None
                }
            });
            if let Some(k) = moved_name {
                self.field_index.insert(k, idx);
            }
            col
        };
        Some(col)
    }

    pub fn num_rows(&self) -> usize {
        self.row_count
    }

    pub fn num_columns(&self) -> usize {
        self.column_order.len()
    }

    pub fn column_names(&self) -> &[String] {
        &self.column_order
    }

    /// Diagnostic: (name, bytes_used, bytes_capacity) for each column.
    pub fn column_diagnostics(&self) -> Vec<(String, usize, usize)> {
        self.column_order
            .iter()
            .map(|name| {
                let idx = self.field_index[name];
                let col = &self.columns[idx];
                (name.clone(), col.bytes_used(), col.capacity_bytes())
            })
            .collect()
    }

    /// The estimated_rows capacity hint passed to with_plan.
    pub fn estimated_rows(&self) -> usize {
        self.estimated_rows
    }

    /// Finalize the builder into an Arrow `RecordBatch`.
    ///
    /// This is also available as the `ColumnarSink::finish` trait method.
    pub fn finish(&mut self) -> Result<RecordBatch> {
        ColumnarSink::finish(self)
    }

    /// Reset all data while preserving the plan and estimated rows.
    pub fn reset(&mut self) {
        self.columns.clear();
        self.field_index.clear();
        self.column_order.clear();
        self.row_dirty.fill(0);
        self.row_count = 0;
        if let Some(ref mut buf) = self.row_buf {
            buf.fields.clear();
            buf.state = PredicateState::Undecided;
        }
        // Keep frozen/row_buf (per builder, not per row)
        self.unknown_error = None;
    }

    /// Truncate every column back to `row_count`, dropping any partial-row
    /// values from a mid-field EOF.  Idempotent.
    pub fn normalize(&mut self) {
        for b in &mut self.columns {
            while b.len() > self.row_count {
                b.pop();
            }
        }
        // Discard any dirty state for the partial row.
        self.row_dirty.fill(0);
    }

    /// If `auto_dict` is set, upgrade low-cardinality string columns using the
    /// plan's threshold/max-size tuning (defaults: 5% ratio, max size 256).
    pub fn auto_dict_upgrade(&mut self) {
        if self.plan.auto_dict {
            let max_ratio = self.plan.dict_threshold.unwrap_or(0.05);
            let max_size = self.plan.dict_max_size.unwrap_or(256);
            for b in &mut self.columns {
                b.try_upgrade_to_dict(512, max_ratio, max_size);
            }
        }
    }

    /// Sort columns according to `schema_order`.  Columns named in
    /// `schema_order` appear in that order; any other columns keep their
    /// relative first-appearance order after the ordered ones.
    pub fn sort_columns(&mut self) {
        if self.plan.schema_order.is_empty() {
            return;
        }
        let order = &self.plan.schema_order;
        let rank = |name: &String| order.iter().position(|n| n == name).unwrap_or(usize::MAX);
        self.column_order.sort_by_key(rank);
    }

    pub(crate) fn schema_insert_index(&self, name: &str) -> usize {
        let order = &self.plan.schema_order;
        if order.is_empty() {
            return self.column_order.len();
        }
        let pos = order.iter().position(|n| n == name);
        match pos {
            Some(p) => self
                .column_order
                .iter()
                .position(|existing| {
                    order
                        .iter()
                        .position(|n| n == existing)
                        .is_some_and(|ep| ep > p)
                })
                .unwrap_or(self.column_order.len()),
            None => self.column_order.len(),
        }
    }

    /// Ensure the column exists and return its Vec index.
    /// Single hash lookup for the hot path; new columns are created and
    /// inserted into `column_order` via `schema_insert_index`.
    fn ensure_column_idx(&mut self, name: &str) -> usize {
        if let Some(&idx) = self.field_index.get(name) {
            return idx;
        }
        // Frozen schema: any new column not pre-sized by ensure_schema is an
        // unknown field (sampling miss or file has column absent during
        // discovery). Don't silently create it — hard-error on finish().
        // We still return a dummy idx to keep the hot path inlinable, but
        // push_field_resolved will have already early-returned after setting
        // unknown_error, so this path is only for non-frozen builders.
        if self.frozen.is_some() {
            // Should have been caught in push_field_resolved; if we reach
            // here, treat as unknown and record.
            if let Some(frozen) = &self.frozen {
                if self.unknown_error.is_none() {
                    self.unknown_error = Some(format!(
                        "unknown field {:?} not in frozen schema ({} columns, exact={}); pass schema=[...] with full column list or use full-scan discovery",
                        name,
                        frozen.num_columns(),
                        frozen.is_exact()
                    ));
                }
            }
            // Return 0 as dummy to avoid panic; caller will have returned.
            return 0;
        }
        let est = self.estimated_rows.max(64);
        let col_type = self.plan.column_type(name);
        let mut b = ColumnBuilder::with_capacity(est, &col_type);
        // When the predicate passes mid-row, drain_buffered increments
        // row_count before all fields are pushed. New columns created after
        // that must backfill row_count-1 Nones (for the already-committed
        // rows), not row_count, because the current row's value follows.
        let backfill = if self.row_buf.as_ref().is_some_and(|b| b.direct) {
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
        // Ensure row_dirty has enough words for new column
        let needed = self.columns.len().div_ceil(64);
        if self.row_dirty.len() < needed {
            self.row_dirty.resize(needed, 0);
        }
        let order_idx = self.schema_insert_index(name);
        self.column_order.insert(order_idx, name.to_owned());
        idx
    }

    /// Push a field value without resolving renames/drops.
    /// Caller must have already resolved `resolved_name` (or know it is kept).
    #[inline]
    fn push_field_resolved(&mut self, resolved_name: &str, value: Value<'_>) {
        // #[inline]: small hot path, called per-field. ensure_column_idx is not
        // inlined (too large); push_value is the real work and benefits from
        // being in the same compilation unit as its caller.
        // Frozen check: hard error on unknown field (data loss would be silent
        // on a default path). Sampling 16×2 MiB covers ~6% of 533 MB, so 0.05%
        // column would be missed.
        if self.frozen.is_some() && !self.field_index.contains_key(resolved_name) {
            if let Some(frozen) = &self.frozen {
                if self.unknown_error.is_none() {
                    self.unknown_error = Some(format!(
                        "unknown field {:?} not in frozen schema ({} columns, exact={}); pass schema=[...] with full column list or use full-scan discovery",
                        resolved_name,
                        frozen.num_columns(),
                        frozen.is_exact()
                    ));
                }
            }
            return;
        }
        let idx = self.ensure_column_idx(resolved_name);
        // Track ordinal→slot mapping on first row for expect_slot fast path.
        if self.row_count == 0 {
            let ord = self.current_ordinal as usize;
            if ord >= self.ordinal_expect.len() {
                self.ordinal_expect.resize_with(ord + 1, || None);
            }
            self.ordinal_expect[ord] = Some((idx as u32, resolved_name.as_bytes().to_vec()));
        }
        self.current_ordinal += 1;
        let word = idx / 64;
        let bit = idx % 64;
        self.row_dirty[word] |= 1u64 << bit;
        let b = &mut self.columns[idx];
        let row_count = self.row_count;
        if b.len() > row_count {
            b.pop();
        }
        b.push_value(value);
    }

    /// Push a field value, resolving renames/drops and applying last-write-wins
    /// within the current uncommitted row.
    #[inline]
    fn push_field(&mut self, name: &str, value: Value<'_>) {
        // #[inline]: fast-path (no rename/drop) is a single branch + delegate.
        // Fast path: no rename/drop configured — zero allocation.
        if self.plan.field_map.is_empty() && self.plan.drop_fields.is_empty() {
            self.push_field_resolved(name, value);
            return;
        }
        // Resolve name via plan's field_map (borrows &self.plan).
        // Try the zero-allocation fast path: column already exists.
        if let Some(idx) = Self::resolve_and_slot(&self.plan, &self.field_index, name) {
            // Track ordinal→slot mapping on first row for expect_slot fast path.
            if self.row_count == 0 {
                let ord = self.current_ordinal as usize;
                if ord >= self.ordinal_expect.len() {
                    self.ordinal_expect.resize_with(ord + 1, || None);
                }
                // Store the resolved name (what plan.resolve_field would return).
                let resolved = self.plan.resolve_field(name).unwrap_or(name);
                self.ordinal_expect[ord] = Some((idx as u32, resolved.as_bytes().to_vec()));
            }
            self.current_ordinal += 1;
            let word = idx / 64;
            let bit = idx % 64;
            self.row_dirty[word] |= 1u64 << bit;
            let b = &mut self.columns[idx];
            let row_count = self.row_count;
            if b.len() > row_count {
                b.pop();
            }
            b.push_value(value);
        } else {
            // Column doesn't exist yet (first row with rename) or field was
            // dropped. Only allocate when the field is kept.
            if let Some(resolved) = self.plan.resolve_field(name) {
                let owned = resolved.to_owned();
                self.push_field_resolved(&owned, value);
            }
        }
    }

    /// Resolve a field name through the plan's field_map and look up its slot
    /// index. Returns `Some(slot)` only if the column already exists in
    /// field_index. Zero allocation — borrows plan and field_index as
    /// separate fields.
    #[inline]
    fn resolve_and_slot(
        plan: &ExecutionPlan,
        field_index: &HashMap<String, usize>,
        name: &str,
    ) -> Option<usize> {
        let resolved = plan.resolve_field(name)?;
        field_index.get(resolved).copied()
    }

    /// Advance the row counter without null-fill, filter, or dirty-mask clear.
    /// For benchmarking only — separates per-field push cost from per-row
    /// finalization.
    #[doc(hidden)]
    pub fn advance_row(&mut self) {
        self.row_count += 1;
    }

    /// Null-fill any column missing this row, then apply the per-row filter.
    /// If the filter rejects the row, undo it by popping values.
    /// Uses the dirty bitmask so only missing columns are touched; fast path
    /// when every column was set (dense data, 10 cols) skips the loop.
    fn finish_row(&mut self) {
        // No #[inline]: 30+ lines with loops; inlining causes code bloat.
        let ncols = self.columns.len();
        // Fast path: check if all bits set
        let full_words = ncols / 64;
        let rem_bits = ncols % 64;
        let is_full = (0..full_words).all(|w| self.row_dirty[w] == u64::MAX)
            && (rem_bits == 0
                || self.row_dirty.get(full_words).copied().unwrap_or(0) == (1u64 << rem_bits) - 1);
        if is_full {
            self.row_dirty.fill(0);
        } else {
            for (i, b) in self.columns.iter_mut().enumerate() {
                let word = i / 64;
                let bit = i % 64;
                let is_set = (self.row_dirty[word] >> bit) & 1 == 1;
                if !is_set {
                    b.push(None);
                }
            }
            self.row_dirty.fill(0);
        }

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

    // Predicate-first helpers
    /// Build the predicate bitmask from field_index. Called lazily on first
    /// `is_predicate_slot` call when the mask is still empty.
    fn build_predicate_mask(&mut self) {
        let buf = match self.row_buf {
            Some(ref mut b) => b,
            None => return,
        };
        if !buf.pred_names.is_empty() {
            return; // already built (pred_names populated)
        }
        // Collect predicate field names (one-time clone of the filter).
        let mut names: SmallVec<[String; 4]> = SmallVec::new();
        if let Some(ref f) = self.plan.filter.clone() {
            Self::collect_predicate_field_names(f, &self.plan, &mut names);
        }
        // Cache the names so future calls can check without cloning.
        buf.pred_names.clone_from(&names);
        // Grow mask to cover all columns.
        let ncols = self.columns.len();
        let words = ncols.div_ceil(64);
        buf.predicate_mask.resize(words, 0);
        // Mark slots for predicate fields that already exist.
        for name in &names {
            if let Some(&slot) = self.field_index.get(name.as_str()) {
                let word = slot / 64;
                let bit = slot % 64;
                if word < buf.predicate_mask.len() {
                    buf.predicate_mask[word] |= 1u64 << bit;
                }
            }
        }
    }

    /// Mark a slot as a predicate column by checking cached predicate names.
    /// Called once per new column to handle fields created after the initial
    /// mask build (e.g., the predicate column appears in the data after
    /// `build_predicate_mask` already ran for earlier columns).
    #[inline]
    fn mark_predicate_slot(&mut self, slot: u32) {
        if self.plan.filter.is_none() {
            return;
        }
        // Phase 1: check fast path (already marked) via immutable borrow,
        // then check if pred_names are populated.  Avoid holding &mut buf
        // across build_predicate_mask (which needs &mut self).
        let needs_build = self.row_buf.as_ref().is_some_and(|buf| {
            let word = slot as usize / 64;
            let bit = slot as usize % 64;
            if word < buf.predicate_mask.len() && (buf.predicate_mask[word] >> bit) & 1 == 1 {
                return false; // already marked — no-op
            }
            buf.pred_names.is_empty() // need to build the mask
        });
        if needs_build {
            self.build_predicate_mask();
        }
        // Phase 2: check if this slot is now marked after build.
        if let Some(ref buf) = self.row_buf {
            let word = slot as usize / 64;
            let bit = slot as usize % 64;
            if word < buf.predicate_mask.len() && (buf.predicate_mask[word] >> bit) & 1 == 1 {
                return; // marked during build or was already marked
            }
        }
        // Phase 3: mask is built but this slot isn't in it. Check if the
        // column name matches any cached predicate name (no filter clone).
        if let Some(col_name) = self.column_order.get(slot as usize) {
            let is_pred = self
                .row_buf
                .as_ref()
                .is_some_and(|buf| buf.pred_names.iter().any(|n| n == col_name));
            if is_pred {
                let buf = self.row_buf.as_mut().unwrap();
                let word = slot as usize / 64;
                let bit = slot as usize % 64;
                if word >= buf.predicate_mask.len() {
                    buf.predicate_mask.resize(word + 1, 0);
                }
                buf.predicate_mask[word] |= 1u64 << bit;
            }
        }
    }

    fn collect_predicate_field_names(
        pred: &FilterPredicate,
        plan: &ExecutionPlan,
        names: &mut SmallVec<[String; 4]>,
    ) {
        match pred {
            FilterPredicate::Equal { field, .. } | FilterPredicate::NotEqual { field, .. } => {
                let resolved = plan.resolve_field(field).unwrap_or(field);
                names.push(resolved.to_string());
            }
            FilterPredicate::Compare {
                field_a, field_b, ..
            } => {
                for f in [field_a, field_b] {
                    let resolved = plan.resolve_field(f).unwrap_or(f);
                    names.push(resolved.to_string());
                }
            }
            FilterPredicate::And(a, b) | FilterPredicate::Or(a, b) => {
                Self::collect_predicate_field_names(a, plan, names);
                Self::collect_predicate_field_names(b, plan, names);
            }
            FilterPredicate::Not(inner) => Self::collect_predicate_field_names(inner, plan, names),
        }
    }

    #[inline]
    fn is_predicate_slot(&self, slot: u32) -> bool {
        self.row_buf.as_ref().is_some_and(|b| {
            let word = slot as usize / 64;
            let bit = slot as usize % 64;
            word < b.predicate_mask.len() && (b.predicate_mask[word] >> bit) & 1 == 1
        })
    }

    fn get_buffered_value(&self, field: &str) -> Option<&Value<'static>> {
        let buf = self.row_buf.as_ref()?;
        let resolved = self.plan.resolve_field(field)?;
        let slot = *self.field_index.get(resolved)? as u32;
        for (s, v) in buf.fields.iter().rev() {
            if *s == slot {
                return Some(v);
            }
        }
        None
    }

    fn get_buffered_str(&self, field: &str) -> Option<String> {
        match self.get_buffered_value(field) {
            Some(Value::Str(s)) => Some(s.to_string()),
            Some(Value::Int64(i)) => Some(i.to_string()),
            Some(Value::Float64(f)) => Some(f.to_string()),
            Some(Value::Bool(b)) => Some(b.to_string()),
            Some(v) => Some(format!("{:?}", v)),
            None => None,
        }
    }

    fn evaluate_predicate_state(&self) -> PredicateState {
        let Some(ref pred) = self.plan.filter else {
            return PredicateState::Pass;
        };
        #[cfg(feature = "profile")]
        {
            crate::engine::PREDICATE_EVALUATIONS.fetch_add(1, Ordering::Relaxed);
        }
        let result = Self::eval_predicate(pred, self);
        #[cfg(feature = "profile")]
        match result {
            PredicateState::Fail => {
                crate::engine::PREDICATE_FAILS.fetch_add(1, Ordering::Relaxed);
            }
            PredicateState::Undecided => {
                crate::engine::PREDICATE_UNDECIDED.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        result
    }

    fn eval_predicate(pred: &FilterPredicate, tb: &TableBuilder) -> PredicateState {
        match pred {
            FilterPredicate::Equal { field, value } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    if actual == *value {
                        PredicateState::Pass
                    } else {
                        PredicateState::Fail
                    }
                }
                None => PredicateState::Undecided,
            },
            FilterPredicate::NotEqual { field, value } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    if actual != *value {
                        PredicateState::Pass
                    } else {
                        PredicateState::Fail
                    }
                }
                None => PredicateState::Undecided,
            },
            FilterPredicate::Compare {
                field_a,
                op,
                field_b,
            } => {
                let normalize = |field: &str, value: &Value<'static>| {
                    let field = tb.plan.resolve_field(field).unwrap_or(field);
                    match tb.plan.column_type(field) {
                        FieldType::Int64 => match value.as_str() {
                            Some(s) => lexical::parse::<i64, _>(s.as_bytes())
                                .ok()
                                .map(Value::Int64),
                            _ => Some(value.clone()),
                        },
                        FieldType::Float64 => match value.as_str() {
                            Some(s) => lexical::parse::<f64, _>(s.as_bytes())
                                .ok()
                                .map(Value::Float64),
                            _ => Some(value.clone()),
                        },
                        FieldType::Boolean => match value.as_str() {
                            Some(s) => s.parse::<bool>().ok().map(Value::Bool),
                            _ => Some(value.clone()),
                        },
                        FieldType::Date32 => match value.as_str() {
                            Some(s) => crate::columnar::parse_date32(s).map(Value::Date32),
                            _ => Some(value.clone()),
                        },
                        FieldType::Timestamp(unit) => match value.as_str() {
                            Some(s) => {
                                crate::columnar::parse_timestamp(s, unit).map(Value::Timestamp)
                            }
                            _ => Some(value.clone()),
                        },
                        FieldType::String | FieldType::Dictionary => Some(value.clone()),
                    }
                };
                let va = tb
                    .get_buffered_value(field_a)
                    .and_then(|value| normalize(field_a, value));
                let vb = tb
                    .get_buffered_value(field_b)
                    .and_then(|value| normalize(field_b, value));
                match (va, vb) {
                    (Some(ref a), Some(ref b)) => {
                        let ord = match (a, b) {
                            (crate::value::Value::Int64(ai), crate::value::Value::Int64(bi)) => Some(ai.cmp(bi).into()),
                            (crate::value::Value::Float64(af), crate::value::Value::Float64(bf)) => af.partial_cmp(bf),
                            (crate::value::Value::Int64(ai), crate::value::Value::Float64(bf)) => ((*ai as f64).partial_cmp(bf)),
                            (crate::value::Value::Float64(af), crate::value::Value::Int64(bi)) => af.partial_cmp(&(*bi as f64)),
                            (crate::value::Value::Str(a), crate::value::Value::Str(b)) => {
                                let resolved_a = tb.plan.resolve_field(field_a).unwrap_or(field_a.as_str());
                                let resolved_b = tb.plan.resolve_field(field_b).unwrap_or(field_b.as_str());
                                let type_a = tb.plan.field_types.get(resolved_a);
                                let type_b = tb.plan.field_types.get(resolved_b);
                                match (type_a, type_b) {
                                    (Some(crate::plan::FieldType::Int64), Some(crate::plan::FieldType::Int64)) => {
                                        let ai: i64 = lexical::parse(a.as_bytes()).ok().unwrap_or(0);
                                        let bi: i64 = lexical::parse(b.as_bytes()).ok().unwrap_or(0);
                                        Some(ai.cmp(&bi).into())
                                    }
                                    (Some(crate::plan::FieldType::Float64), Some(crate::plan::FieldType::Float64)) => {
                                        let af: f64 = lexical::parse(a.as_bytes()).ok().unwrap_or(0.0);
                                        let bf: f64 = lexical::parse(b.as_bytes()).ok().unwrap_or(0.0);
                                        af.partial_cmp(&bf)
                                    }
                                    (Some(crate::plan::FieldType::Int64), Some(crate::plan::FieldType::Float64)) => {
                                        let ai: f64 = lexical::parse::<i64, _>(a.as_bytes()).ok().unwrap_or(0) as f64;
                                        let bf: f64 = lexical::parse(b.as_bytes()).ok().unwrap_or(0.0);
                                        ai.partial_cmp(&bf)
                                    }
                                    (Some(crate::plan::FieldType::Float64), Some(crate::plan::FieldType::Int64)) => {
                                        let af: f64 = lexical::parse(a.as_bytes()).ok().unwrap_or(0.0);
                                        let bi: f64 = lexical::parse::<i64, _>(b.as_bytes()).ok().unwrap_or(0) as f64;
                                        af.partial_cmp(&bi)
                                    }
                                    // Mixed typed/untyped or String vs non-numeric type:
                                    // type mismatch — fail the comparison.
                                    (Some(_), None) | (None, Some(_)) => None,
                                    // Both untyped (String): fall back to lexicographic.
                                    (None, None) => Some(a.cmp(&b).into()),
                                    // Both Timestamp: parse and compare as i64.
                                    (Some(crate::plan::FieldType::Timestamp(ua)), Some(crate::plan::FieldType::Timestamp(ub))) => {
                                        let ta = crate::columnar::parse_timestamp(a.as_ref(), *ua);
                                        let tb_val = crate::columnar::parse_timestamp(b.as_ref(), *ub);
                                        match (ta, tb_val) {
                                            (Some(ai), Some(bi)) => Some(ai.cmp(&bi).into()),
                                            _ => None,
                                        }
                                    }
                                    // Both Date32: parse and compare as i32.
                                    (Some(crate::plan::FieldType::Date32), Some(crate::plan::FieldType::Date32)) => {
                                        let da = crate::columnar::parse_date32(a.as_ref());
                                        let db = crate::columnar::parse_date32(b.as_ref());
                                        match (da, db) {
                                            (Some(ai), Some(bi)) => Some(ai.cmp(&bi).into()),
                                            _ => None,
                                        }
                                    }
                                    // Different non-numeric types (e.g. String vs Bool): fail.
                                    _ => None,
                                }
                            }
                            (crate::value::Value::Bool(a), crate::value::Value::Bool(b)) => Some(a.cmp(b).into()),
                            _ => {
                                let av = format!("{:?}", a);
                                let bv = format!("{:?}", b);
                                Some(av.cmp(&bv).into())
                            }
                        };
                        let pass = ord.is_some_and(|ord| match op {
                            crate::plan::CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                            crate::plan::CompareOp::Lt => ord == std::cmp::Ordering::Less,
                            crate::plan::CompareOp::Ge => ord != std::cmp::Ordering::Less,
                            crate::plan::CompareOp::Le => ord != std::cmp::Ordering::Greater,
                            crate::plan::CompareOp::Eq => ord == std::cmp::Ordering::Equal,
                            crate::plan::CompareOp::Ne => ord != std::cmp::Ordering::Equal,
                        });
                        if pass {
                            PredicateState::Pass
                        } else {
                            PredicateState::Fail
                        }
                    }
                    _ => PredicateState::Undecided,
                }
            }
            FilterPredicate::And(a, b) => {
                let sa = Self::eval_predicate(a, tb);
                let sb = Self::eval_predicate(b, tb);
                match (sa, sb) {
                    (PredicateState::Fail, _) | (_, PredicateState::Fail) => PredicateState::Fail,
                    (PredicateState::Pass, PredicateState::Pass) => PredicateState::Pass,
                    _ => PredicateState::Undecided,
                }
            }
            FilterPredicate::Or(a, b) => {
                let sa = Self::eval_predicate(a, tb);
                let sb = Self::eval_predicate(b, tb);
                match (sa, sb) {
                    (PredicateState::Pass, _) | (_, PredicateState::Pass) => PredicateState::Pass,
                    (PredicateState::Fail, PredicateState::Fail) => PredicateState::Fail,
                    _ => PredicateState::Undecided,
                }
            }
            FilterPredicate::Not(inner) => match Self::eval_predicate(inner, tb) {
                PredicateState::Pass => PredicateState::Fail,
                PredicateState::Fail => PredicateState::Pass,
                PredicateState::Undecided => PredicateState::Undecided,
            },
        }
    }

    fn drain_buffered(&mut self, pass: bool) {
        let buf = match self.row_buf {
            Some(ref mut b) => b,
            None => return,
        };
        if pass {
            // Deduplicate last-write-wins by slot index (u32).
            // O(n) reverse scan with a u64 bitmask — first hit in reverse is
            // the last write in forward order (last-write-wins).
            let ncols = self.columns.len();
            let words = ncols.div_ceil(64);
            if self.row_dirty.len() < words {
                self.row_dirty.resize(words, 0);
            }
            let mut seen: u64 = 0;
            let mut seen_extra: SmallVec<[u64; 1]> = SmallVec::new();
            if words > 1 {
                seen_extra.resize(words - 1, 0);
            }
            for &(slot, ref val) in buf.fields.iter().rev() {
                let word = slot as usize / 64;
                let bit = slot as usize % 64;
                let mask = 1u64 << bit;
                let already = if word == 0 {
                    seen & mask != 0
                } else {
                    seen_extra.get(word - 1).is_some_and(|w| w & mask != 0)
                };
                if already {
                    continue;
                }
                if word == 0 {
                    seen |= mask;
                } else if let Some(w) = seen_extra.get_mut(word - 1) {
                    *w |= mask;
                }
                self.row_dirty[word] |= mask;
                let b = &mut self.columns[slot as usize];
                if b.len() > self.row_count {
                    b.pop();
                }
                b.push_value(val.clone());
            }
            // Null-fill missing columns and handle filter/dirty, then increment row_count.
            // Skip null-fill when called mid-row (buf.direct = true): the remaining
            // fields will be pushed via the direct path, and end_row's
            // null_fill_missing handles any columns that don't appear.
            if !buf.direct {
                let ncols = self.columns.len();
                let full_words = ncols / 64;
                let rem_bits = ncols % 64;
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
                        let is_set = (self.row_dirty[word] >> bit) & 1 == 1;
                        if !is_set {
                            b.push(None);
                        }
                    }
                    self.row_dirty.fill(0);
                }
            } else {
                // Mid-row: keep dirty bits set so null_fill_missing in end_row
                // can take the is_full fast path when all columns were pushed.
            }
            self.row_count += 1;
        } else {
            // Fail: discard buffered fields, no row increment, clear dirty
            buf.fields.clear();
            self.row_dirty.fill(0);
        }
        // Ensure fields are cleared for next row
        if !buf.fields.is_empty() {
            buf.fields.clear();
        }
        buf.state = PredicateState::Undecided;
    }

    fn evaluate_against_null(&self) -> PredicateState {
        // At end_row, any predicate field still missing is NULL.
        // For Equal with missing => Fail, NotEqual with missing => Pass (as per old finish_row check where get_value returns None)
        // For Compare with missing => Fail.
        // We can reuse eval_predicate which returns Undecided for missing, then map Undecided to Pass/Fail per leaf semantics.
        let Some(ref pred) = self.plan.filter else {
            return PredicateState::Pass;
        };
        Self::eval_predicate_with_null(pred, self)
    }

    fn eval_predicate_with_null(pred: &FilterPredicate, tb: &TableBuilder) -> PredicateState {
        match pred {
            FilterPredicate::Equal { field, value } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    if actual == *value {
                        PredicateState::Pass
                    } else {
                        PredicateState::Fail
                    }
                }
                None => PredicateState::Fail, // missing => None != Some(value) => Fail for Equal
            },
            FilterPredicate::NotEqual { field, value } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    if actual != *value {
                        PredicateState::Pass
                    } else {
                        PredicateState::Fail
                    }
                }
                None => PredicateState::Pass, // missing => None != Some(value) => Pass
            },
            FilterPredicate::Compare { .. } => {
                // Compare with missing => Fail (as per old check where get_typed_value returns None)
                match Self::eval_predicate(pred, tb) {
                    PredicateState::Undecided => PredicateState::Fail,
                    other => other,
                }
            }
            FilterPredicate::And(a, b) => {
                let sa = Self::eval_predicate_with_null(a, tb);
                let sb = Self::eval_predicate_with_null(b, tb);
                match (sa, sb) {
                    (PredicateState::Fail, _) | (_, PredicateState::Fail) => PredicateState::Fail,
                    (PredicateState::Pass, PredicateState::Pass) => PredicateState::Pass,
                    _ => PredicateState::Undecided, // Should not happen after null mapping, but treat as Fail?
                }
            }
            FilterPredicate::Or(a, b) => {
                let sa = Self::eval_predicate_with_null(a, tb);
                let sb = Self::eval_predicate_with_null(b, tb);
                match (sa, sb) {
                    (PredicateState::Pass, _) | (_, PredicateState::Pass) => PredicateState::Pass,
                    (PredicateState::Fail, PredicateState::Fail) => PredicateState::Fail,
                    _ => PredicateState::Fail,
                }
            }
            FilterPredicate::Not(inner) => match Self::eval_predicate_with_null(inner, tb) {
                PredicateState::Pass => PredicateState::Fail,
                PredicateState::Fail => PredicateState::Pass,
                PredicateState::Undecided => PredicateState::Fail, // Not Undecided -> Fail?
            },
        }
    }

    /// Null-fill any column missing the current row, then clear row_dirty.
    /// Called when the predicate resolved to Pass mid-row (direct mode).
    fn null_fill_missing(&mut self) {
        let ncols = self.columns.len();
        let full_words = ncols / 64;
        let rem_bits = ncols % 64;
        let is_full = (0..full_words).all(|w| self.row_dirty[w] == u64::MAX)
            && (rem_bits == 0
                || self.row_dirty.get(full_words).copied().unwrap_or(0) == (1u64 << rem_bits) - 1);
        if is_full {
            self.row_dirty.fill(0);
        } else {
            for (i, b) in self.columns.iter_mut().enumerate() {
                let word = i / 64;
                let bit = i % 64;
                let is_set = (self.row_dirty[word] >> bit) & 1 == 1;
                if !is_set {
                    b.push(None);
                }
            }
            self.row_dirty.fill(0);
        }
        // Note: row_count was already incremented by drain_buffered(true)
        // when the predicate resolved to Pass mid-row. Do NOT increment here.
    }
}

impl Default for TableBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnarSink for TableBuilder {
    #[inline]
    fn begin_row(&mut self) {
        self.current_ordinal = 0;
        if let Some(ref mut buf) = self.row_buf {
            // Adaptive strategy: after at least one committed row, decide
            // whether buffering is worthwhile based on the predicate column's
            // ordinal relative to the total number of columns.  We must wait
            // until row_count > 0 because early-rejected rows don't create
            // all columns, making ncols unreliable for the ordinal comparison.
            if buf.predicate_ordinal.is_some() && buf.buffer_worthwhile && self.row_count > 0 {
                // Use the most accurate column count available. self.columns.len()
                // is unreliable for sparse files where not all columns exist yet.
                let ncols = self
                    .frozen
                    .as_ref()
                    .map(|f| f.num_columns() as u32)
                    .or_else(|| {
                        (!self.plan.schema_order.is_empty())
                            .then(|| self.plan.schema_order.len() as u32)
                    })
                    .unwrap_or(self.columns.len() as u32);
                if let Some(ordinal) = buf.predicate_ordinal {
                    if ordinal >= ncols * 4 / 5 {
                        buf.buffer_worthwhile = false;
                    }
                }
            }
            buf.fields.clear();
            buf.state = PredicateState::Undecided;
            buf.direct = false;
        }
    }

    #[inline]
    fn put_field(&mut self, name: &str, value: Value<'_>) {
        if self.plan.filter.is_none() {
            self.push_field(name, value);
            return;
        }
        // Adaptive: late predicate → push directly, pop-on-reject at end_row.
        let buf_worthwhile = self.row_buf.as_ref().is_none_or(|b| b.buffer_worthwhile);
        if !buf_worthwhile {
            self.push_field(name, value);
            return;
        }
        // Buffered path: resolve name → slot, buffer (slot, value).
        let resolved = match self.plan.resolve_field(name) {
            Some(r) => r.to_owned(),
            None => return,
        };
        // Check frozen unknown (as in push_field_resolved)
        if self.frozen.is_some()
            && !self.field_index.contains_key(&resolved)
            && self.unknown_error.is_none()
        {
            if let Some(ref frozen) = self.frozen {
                if !frozen.column_names().iter().any(|n| n.as_ref() == resolved) {
                    self.unknown_error = Some(format!(
                            "unknown field {:?} not in frozen schema ({} columns, exact={}); pass schema=[...]",
                            resolved,
                            frozen.num_columns(),
                            frozen.is_exact()
                        ));
                    return;
                }
            }
        }
        let slot = self.ensure_column_idx(&resolved) as u32;
        self.mark_predicate_slot(slot);
        let val_static: Value<'static> = value.into_static();
        let is_pred = self.is_predicate_slot(slot);
        #[cfg(feature = "profile")]
        if is_pred {
            crate::engine::IS_PRED_TRUE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        } else {
            crate::engine::IS_PRED_FALSE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        // Push value to buffer BEFORE evaluating predicate (value must be findable).
        if let Some(ref mut buf) = self.row_buf {
            if let Some(pos) = buf.fields.iter().position(|(s, _)| *s == slot) {
                buf.fields[pos].1 = val_static;
            } else {
                buf.fields.push((slot, val_static));
            }
        }
        if is_pred {
            // Record predicate ordinal for adaptive strategy (once)
            if let Some(ref mut buf) = self.row_buf {
                if buf.predicate_ordinal.is_none() {
                    buf.predicate_ordinal = Some(slot);
                }
            }
            let state = self.evaluate_predicate_state();
            if let Some(ref mut buf) = self.row_buf {
                buf.state = state;
            }
        }
    }

    #[inline]
    fn end_row(&mut self) {
        if self.plan.filter.is_none() {
            self.finish_row();
            return;
        }
        // Check adaptive strategy: if buffering is not worthwhile (late
        // predicate), use direct push + pop-on-reject (finish_row).
        let buf_worthwhile = self.row_buf.as_ref().is_none_or(|b| b.buffer_worthwhile);
        if !buf_worthwhile {
            // Late predicate: values were pushed directly to columns.
            // finish_row null-fills missing, evaluates filter against columns,
            // and pops on reject — exactly the pre-buffering behavior.
            self.finish_row();
            return;
        }
        // Buffered path
        let (state, direct) = self
            .row_buf
            .as_ref()
            .map_or((PredicateState::Pass, false), |b| (b.state, b.direct));
        if direct {
            // Already flushed to columns; just null-fill missing columns.
            self.null_fill_missing();
            return;
        }
        let state = if state == PredicateState::Undecided {
            self.evaluate_against_null()
        } else {
            state
        };
        let pass = state == PredicateState::Pass;
        self.drain_buffered(pass);
    }

    #[inline]
    fn wants(&self, name: &str) -> bool {
        self.resolve(name).is_some()
    }

    #[inline]
    fn resolve<'a>(&'a self, name: &'a str) -> Option<&'a str> {
        self.plan.resolve_field(name)
    }

    #[inline]
    fn put_field_resolved(&mut self, resolved_name: &str, value: Value<'_>) {
        if self.plan.filter.is_none() {
            self.push_field_resolved(resolved_name, value);
            return;
        }
        // Adaptive: late predicate → push directly, pop-on-reject at end_row.
        let buf_worthwhile = self.row_buf.as_ref().is_none_or(|b| b.buffer_worthwhile);
        if !buf_worthwhile {
            self.push_field_resolved(resolved_name, value);
            return;
        }
        // Buffered path for already-resolved name (no rename lookup)
        if self.frozen.is_some() && !self.field_index.contains_key(resolved_name) {
            if let Some(ref frozen) = self.frozen {
                if !frozen
                    .column_names()
                    .iter()
                    .any(|n| n.as_ref() == resolved_name)
                {
                    if self.unknown_error.is_none() {
                        self.unknown_error = Some(format!(
                            "unknown field {:?} not in frozen schema ({} columns, exact={}); pass schema=[...]",
                            resolved_name,
                            frozen.num_columns(),
                            frozen.is_exact()
                        ));
                    }
                    return;
                }
            }
        }
        let slot = self.ensure_column_idx(resolved_name) as u32;
        self.mark_predicate_slot(slot);
        let val_static: Value<'static> = value.into_static();
        let is_pred = self.is_predicate_slot(slot);
        #[cfg(feature = "profile")]
        if is_pred {
            crate::engine::IS_PRED_TRUE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        } else {
            crate::engine::IS_PRED_FALSE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if let Some(ref mut buf) = self.row_buf {
            if let Some(pos) = buf.fields.iter().position(|(s, _)| *s == slot) {
                buf.fields[pos].1 = val_static;
            } else {
                buf.fields.push((slot, val_static));
            }
        }
        if is_pred {
            // Record predicate ordinal for adaptive strategy (once)
            if let Some(ref mut buf) = self.row_buf {
                if buf.predicate_ordinal.is_none() {
                    buf.predicate_ordinal = Some(slot);
                }
            }
            let state = self.evaluate_predicate_state();
            if let Some(ref mut buf) = self.row_buf {
                buf.state = state;
            }
        }
    }

    /// Push a value directly to a known slot index, bypassing all name
    /// resolution and hash lookups.  Used by adapters that verified the
    /// field identity via `expect_slot` + memcmp on raw bytes.
    #[inline]
    fn put_field_at(&mut self, slot: u32, value: Value<'_>) {
        let idx = slot as usize;
        if idx >= self.columns.len() {
            // Slot doesn't exist yet (sparse row). Fall back to ensure.
            // This shouldn't happen after row 1 with a stable layout.
            return;
        }
        let word = idx / 64;
        let bit = idx % 64;
        self.row_dirty[word] |= 1u64 << bit;
        let b = &mut self.columns[idx];
        let row_count = self.row_count;
        if b.len() > row_count {
            b.pop();
        }
        b.push_value(value);
    }

    #[inline]
    fn resolve_and_put(&mut self, name: &str, value: Value<'_>) {
        #[cfg(feature = "profile")]
        RESOLVE_AND_PUT_COUNT.fetch_add(1, Ordering::Relaxed);
        if self.plan.filter.is_none() {
            if self.plan.field_map.is_empty() && self.plan.drop_fields.is_empty() {
                self.push_field_resolved(name, value);
            } else {
                if let Some(resolved) = self.plan.resolve_field(name) {
                    let owned = resolved.to_owned();
                    self.push_field_resolved(&owned, value);
                }
            }
            return;
        }
        // Adaptive: late predicate → push directly, pop-on-reject at end_row.
        if self.row_buf.as_ref().is_some_and(|b| !b.buffer_worthwhile) {
            if self.plan.field_map.is_empty() && self.plan.drop_fields.is_empty() {
                self.push_field_resolved(name, value);
            } else {
                if let Some(resolved) = self.plan.resolve_field(name) {
                    let owned = resolved.to_owned();
                    self.push_field_resolved(&owned, value);
                }
            }
            return;
        }
        // Direct mode: predicate already passed, push directly to columns.
        if let Some(ref buf) = self.row_buf {
            if buf.direct {
                let resolved = match self.plan.resolve_field(name) {
                    Some(r) => r.to_owned(),
                    None => return, // field is dropped
                };
                if self.frozen.is_some() && !self.field_index.contains_key(resolved.as_str()) {
                    if let Some(ref frozen) = self.frozen {
                        if !frozen.column_names().iter().any(|n| n.as_ref() == resolved.as_str()) {
                            if self.unknown_error.is_none() {
                                self.unknown_error = Some(format!(
                                    "unknown field {:?} not in frozen schema ({} columns, exact={}); pass schema=[...]",
                                    resolved, frozen.num_columns(), frozen.is_exact()
                                ));
                            }
                            return;
                        }
                    }
                }
                self.push_field_resolved(&resolved, value);
                return;
            }
        }
        // Buffered path: resolve → slot, buffer (slot, value).
        if self.plan.field_map.is_empty() && self.plan.drop_fields.is_empty() {
            // No rename/drop, raw == resolved
            let resolved = name;
            if self.frozen.is_some() && !self.field_index.contains_key(resolved) {
                if let Some(ref frozen) = self.frozen {
                    if !frozen.column_names().iter().any(|n| n.as_ref() == resolved) {
                        if self.unknown_error.is_none() {
                            self.unknown_error = Some(format!(
                                "unknown field {:?} not in frozen schema ({} columns, exact={}); pass schema=[...]",
                                resolved,
                                frozen.num_columns(),
                                frozen.is_exact()
                            ));
                        }
                        return;
                    }
                }
            }
            let slot = self.ensure_column_idx(resolved) as u32;
            self.mark_predicate_slot(slot);
            let val_static: Value<'static> = value.into_static();
            let is_pred = self.is_predicate_slot(slot);
            #[cfg(feature = "profile")]
            if is_pred {
                crate::engine::IS_PRED_TRUE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                crate::engine::IS_PRED_FALSE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            if let Some(ref mut buf) = self.row_buf {
                if let Some(pos) = buf.fields.iter().position(|(s, _)| *s == slot) {
                    buf.fields[pos].1 = val_static;
                } else {
                    buf.fields.push((slot, val_static));
                }
            }
            if is_pred {
                // Record predicate ordinal for adaptive strategy (once)
                if let Some(ref mut buf) = self.row_buf {
                    if buf.predicate_ordinal.is_none() {
                        buf.predicate_ordinal = Some(slot);
                    }
                }
                let state = self.evaluate_predicate_state();
                if let Some(ref mut buf) = self.row_buf {
                    buf.state = state;
                    if state == PredicateState::Pass {
                        buf.direct = true;
                    }
                }
                if state == PredicateState::Pass {
                    // Predicate passed: drain buffered fields, switch to direct mode.
                    self.drain_buffered(true);
                }
            }
        } else {
            if let Some(resolved) = self.plan.resolve_field(name) {
                let owned = resolved.to_owned();
                if self.frozen.is_some() && !self.field_index.contains_key(&owned) {
                    if let Some(ref frozen) = self.frozen {
                        if !frozen.column_names().iter().any(|n| n.as_ref() == owned) {
                            if self.unknown_error.is_none() {
                                self.unknown_error = Some(format!(
                                    "unknown field {:?} not in frozen schema ({} columns, exact={}); pass schema=[...]",
                                    owned,
                                    frozen.num_columns(),
                                    frozen.is_exact()
                                ));
                            }
                            return;
                        }
                    }
                }
                let slot = self.ensure_column_idx(&owned) as u32;
                self.mark_predicate_slot(slot);
                let val_static: Value<'static> = value.into_static();
                let is_pred = self.is_predicate_slot(slot);
                #[cfg(feature = "profile")]
                if is_pred {
                    crate::engine::IS_PRED_TRUE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    crate::engine::IS_PRED_FALSE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                if let Some(ref mut buf) = self.row_buf {
                    if let Some(pos) = buf.fields.iter().position(|(s, _)| *s == slot) {
                        buf.fields[pos].1 = val_static;
                    } else {
                        buf.fields.push((slot, val_static));
                    }
                }
                if is_pred {
                    // Record predicate ordinal for adaptive strategy (once)
                    if let Some(ref mut buf) = self.row_buf {
                        if buf.predicate_ordinal.is_none() {
                            buf.predicate_ordinal = Some(slot);
                        }
                    }
                    let state = self.evaluate_predicate_state();
                    if let Some(ref mut buf) = self.row_buf {
                        buf.state = state;
                    }
                }
            }
        }
    }

    #[inline]
    fn row_rejected(&self) -> bool {
        self.plan.filter.is_some()
            && self
                .row_buf
                .as_ref()
                .is_some_and(|b| b.state == PredicateState::Fail)
    }

    #[inline]
    fn row_satisfied(&self) -> bool {
        let mask = self.wanted_mask();
        if mask == 0 {
            return false;
        }
        // Check that every wanted bit is set in row_dirty.
        let dirty = self.row_dirty.iter().copied().reduce(|a, b| a | b).unwrap_or(0);
        (dirty & mask) == mask
    }

    #[inline]
    fn wanted_mask(&self) -> u64 {
        // Build mask from plan schema_order or field_index.
        if !self.plan.schema_order.is_empty() {
            let mut mask = 0u64;
            for name in &self.plan.schema_order {
                if let Some(&idx) = self.field_index.get(name.as_str()) {
                    if idx < 64 {
                        mask |= 1u64 << idx;
                    }
                }
            }
            mask
        } else if self.plan.drop_fields.is_empty() && self.plan.field_map.is_empty() {
            // No projection: all columns wanted. Return 0 to disable short-circuit.
            0
        } else {
            // drop_fields active: wanted = all columns minus dropped.
            let mut mask = 0u64;
            for (name, &idx) in &self.field_index {
                if idx < 64 && !self.plan.drop_fields.contains(name) {
                    mask |= 1u64 << idx;
                }
            }
            mask
        }
    }

    #[inline]
    fn finish(&mut self) -> Result<RecordBatch> {
        if let Some(err) = self.unknown_error.take() {
            return Err(crate::Error::Merge(err));
        }
        self.normalize();

        if self.column_order.is_empty() {
            let schema = Arc::new(Schema::empty());
            return Ok(RecordBatch::new_empty(schema));
        }

        self.auto_dict_upgrade();
        self.sort_columns();

        let mut fields = Vec::with_capacity(self.column_order.len());
        let mut arrays = Vec::with_capacity(self.column_order.len());
        // Iterate by index so we can borrow columns mutably for zero-copy export.
        for name in &self.column_order {
            if let Some(&idx) = self.field_index.get(name.as_str()) {
                let b = &mut self.columns[idx];
                fields.push(ArrowField::new(name.as_str(), b.arrow_datatype(), true));
                arrays.push(b.to_arrow_array()?);
            }
        }

        let schema = Arc::new(Schema::new(fields));
        Ok(RecordBatch::try_new(schema, arrays)?)
    }

    #[inline]
    fn expect_slot(&self, ordinal: u32) -> Option<(u32, &[u8])> {
        self.ordinal_expect
            .get(ordinal as usize)
            .and_then(|entry| entry.as_ref())
            .map(|(slot, raw)| (*slot, raw.as_slice()))
    }

    #[inline]
    fn record_slot(&mut self, ordinal: u32, slot: u32, raw_name: &[u8]) {
        let idx = ordinal as usize;
        if idx >= self.ordinal_expect.len() {
            self.ordinal_expect.resize_with(idx + 1, || None);
        }
        self.ordinal_expect[idx] = Some((slot, raw_name.to_vec()));
    }

    #[inline]
    fn layout_broken(&mut self, ordinal: u32) {
        if let Some(entry) = self.ordinal_expect.get_mut(ordinal as usize) {
            *entry = None;
        }
    }

    #[inline]
    fn reset_child_ordinal(&mut self) {
        self.current_ordinal = 0;
    }
}

// ---------------------------------------------------------------------------
// LocateOnly sink — walk rows, resolve field names, decode nothing.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::ColumnBuilder;
    use crate::decoder::{ColumnarSink, RecordParser, Splitter};
    use crate::plan::{CompareOp, ExecutionPlan, FieldType, FilterPredicate};
    use crate::value::Value;
    use crate::Result;
    use arrow::array::{Array, AsArray};

    /// Simple newline-delimited parser for test data.
    /// Each line is a row; fields are `key=value` separated by spaces.
    struct LineParser;

    impl RecordParser for LineParser {
        fn validate(&self, bytes: &[u8]) -> Result<()> {
            simdutf8::basic::from_utf8(bytes)?;
            Ok(())
        }

        fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
            let text = std::str::from_utf8(bytes).map_err(|e| crate::Error::Plan(e.to_string()))?;
            for line in text.lines() {
                if line.is_empty() {
                    continue;
                }
                sink.begin_row();
                for token in line.split_whitespace() {
                    if let Some((k, v)) = token.split_once('=') {
                        sink.put_field(k, Value::Str(Cow::Borrowed(v)));
                    }
                }
                sink.end_row();
            }
            Ok(())
        }
    }

    struct LineSplitter;

    impl Splitter for LineSplitter {
        fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
            if from >= bytes.len() {
                return None;
            }
            // If we're at a newline, advance past it.
            let start = if bytes[from] == b'\n' { from + 1 } else { from };
            if start >= bytes.len() {
                return None;
            }
            memchr::memchr(b'\n', &bytes[start..]).map(|rel| start + rel + 1)
        }

        fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize> {
            if max_chunks <= 1 || bytes.is_empty() {
                return vec![0, bytes.len()];
            }
            let mut points = vec![0usize];
            let mut last = 0;
            for (i, &b) in bytes.iter().enumerate() {
                if b == b'\n' {
                    let next = i + 1;
                    if next > last && points.len() < max_chunks {
                        points.push(next);
                        last = next;
                    }
                }
            }
            if *points.last().unwrap() != bytes.len() {
                points.push(bytes.len());
            }
            points
        }

        fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
            let newline_count = sample.iter().filter(|&&b| b == b'\n').count().max(1);
            (sample.len() / newline_count).max(1)
        }
    }

    fn parse_bytes(bytes: &[u8], plan: ExecutionPlan) -> TableBuilder {
        let mut sink = TableBuilder::with_plan((bytes.len() / 16).max(4), Arc::new(plan));
        LineParser.parse_chunk(bytes, &mut sink).unwrap();
        sink
    }

    #[test]
    fn test_extend_no_duplicates() {
        let e1 = parse_bytes(b"A=1 B=2\n", ExecutionPlan::new());
        let mut merged = TableBuilder::new();
        merged.extend(e1).unwrap();
        assert_eq!(merged.num_rows(), 1);
        for col in &merged.columns {
            assert_eq!(col.len(), merged.num_rows(), "column length mismatch");
        }
    }

    #[test]
    fn test_multi_chunk_same_as_single() {
        let data = b"A=1\nA=2\nA=3\n";
        let single = parse_bytes(data, ExecutionPlan::new());
        assert_eq!(single.num_rows(), 3);

        let splitter = LineSplitter;
        let points = splitter.find_split_points(data, 2);
        let mut merged = TableBuilder::new();
        for w in points.windows(2) {
            let chunk = &data[w[0]..w[1]];
            let engine = parse_bytes(chunk, ExecutionPlan::new());
            merged.extend(engine).unwrap();
        }
        assert_eq!(merged.num_rows(), single.num_rows());
    }

    #[test]
    fn test_last_write_wins_duplicate_field() {
        let engine = parse_bytes(b"X=10 X=20\n", ExecutionPlan::new());
        assert_eq!(engine.num_rows(), 1);
        let col = engine.get_column("X").unwrap();
        assert_eq!(col.as_str_vec(), vec![Some("20".into())]);
    }

    #[test]
    fn test_build_plan_rename() {
        let mut plan = ExecutionPlan::new();
        plan.field_map.insert("X".to_string(), "Y".to_string());
        let engine = parse_bytes(b"X=hello\n", plan);
        assert_eq!(engine.num_rows(), 1);
        assert!(engine.get_column("Y").is_some());
        assert!(engine.get_column("X").is_none());
        assert_eq!(
            engine.get_column("Y").unwrap().as_str_vec(),
            vec![Some("hello".into())]
        );
    }

    #[test]
    fn test_build_plan_drop() {
        let mut plan = ExecutionPlan::new();
        plan.drop_fields.insert("X".to_string());
        let engine = parse_bytes(b"X=hello Y=world\n", plan);
        assert_eq!(engine.num_rows(), 1);
        assert!(engine.get_column("X").is_none());
        assert!(engine.get_column("Y").is_some());
    }

    #[test]
    fn test_build_plan_filter_ne() {
        let mut plan = ExecutionPlan::new();
        plan.filter = Some(FilterPredicate::NotEqual {
            field: "X".to_string(),
            value: "42".to_string(),
        });
        let engine = parse_bytes(b"X=10\nX=42\nX=30\n", plan);
        assert_eq!(engine.num_rows(), 2);
        let col = engine.get_column("X").unwrap();
        assert_eq!(col.as_str_vec(), vec![Some("10".into()), Some("30".into())]);
    }

    #[test]
    fn test_build_plan_filter_eq() {
        let mut plan = ExecutionPlan::new();
        plan.filter = Some(FilterPredicate::Equal {
            field: "X".to_string(),
            value: "10".to_string(),
        });
        let engine = parse_bytes(b"X=10\nX=20\nX=10\n", plan);
        assert_eq!(engine.num_rows(), 2);
        let col = engine.get_column("X").unwrap();
        assert_eq!(col.as_str_vec(), vec![Some("10".into()), Some("10".into())]);
    }

    #[test]
    fn test_build_plan_filter_missing_field() {
        let mut plan = ExecutionPlan::new();
        plan.filter = Some(FilterPredicate::NotEqual {
            field: "X".to_string(),
            value: "10".to_string(),
        });
        let engine = parse_bytes(b"X=10\nY=99\n", plan);
        assert_eq!(engine.num_rows(), 1);
        let col = engine.get_column("Y").unwrap();
        assert_eq!(col.as_str_vec(), vec![Some("99".into())]);
    }

    #[test]
    fn test_typed_int64_column() {
        let mut plan = ExecutionPlan::new();
        plan.field_types.insert("X".to_string(), FieldType::Int64);
        let engine = parse_bytes(b"X=42\nX=bad\nX=100\n", plan);
        assert_eq!(engine.num_rows(), 3);
        if let ColumnBuilder::Int64(v) = engine.get_column("X").unwrap() {
            assert_eq!(v.get(0), Some(42));
            assert_eq!(v.get(1), None);
            assert_eq!(v.get(2), Some(100));
        } else {
            panic!("expected Int64 builder");
        }
    }

    #[test]
    fn test_typed_float64_column() {
        let mut plan = ExecutionPlan::new();
        plan.field_types.insert("X".to_string(), FieldType::Float64);
        let engine = parse_bytes(b"X=1.5\n", plan);
        if let ColumnBuilder::Float64(v) = engine.get_column("X").unwrap() {
            assert!((v.get(0).unwrap() - 1.5).abs() < 1e-9);
        } else {
            panic!("expected Float64 builder");
        }
    }

    #[test]
    fn test_dictionary_column() {
        let mut plan = ExecutionPlan::new();
        plan.dictionary_columns.insert("P".to_string());
        let engine = parse_bytes(b"P=Widget\nP=Gadget\nP=Widget\n", plan);
        assert_eq!(engine.num_rows(), 3);
        if let ColumnBuilder::Dictionary { codes, dict, .. } = engine.get_column("P").unwrap() {
            assert_eq!(dict.len(), 2);
            assert_eq!(
                codes.iter().map(|o| o.copied()).collect::<Vec<_>>(),
                vec![Some(0), Some(1), Some(0)]
            );
        } else {
            panic!("expected Dictionary builder");
        }
    }

    #[test]
    fn test_ragged_late_chunk_column_debut() {
        let e1 = parse_bytes(b"A=1 B=2\nA=3\n", ExecutionPlan::new());
        let e2 = parse_bytes(b"B=4 C=5\n", ExecutionPlan::new());
        let mut merged = TableBuilder::new();
        merged.extend(e1).unwrap();
        merged.extend(e2).unwrap();

        assert_eq!(
            merged.get_column("A").unwrap().as_str_vec(),
            vec![Some("1".into()), Some("3".into()), None]
        );
        assert_eq!(
            merged.get_column("B").unwrap().as_str_vec(),
            vec![Some("2".into()), None, Some("4".into())]
        );
        assert_eq!(
            merged.get_column("C").unwrap().as_str_vec(),
            vec![None, None, Some("5".into())]
        );
    }

    #[test]
    fn test_auto_dict_upgrade_only_post_merge() {
        let mut plan = ExecutionPlan::new();
        plan.auto_dict = true;
        let a = parse_bytes(b"P=x\nP=y\n", plan.clone());
        let b = parse_bytes(b"P=x\nP=y\n", plan.clone());
        let mut merged = TableBuilder::with_plan(64, Arc::new(plan));
        merged.extend(a).unwrap();
        merged.extend(b).unwrap();
        merged.auto_dict_upgrade();
        assert_eq!(merged.num_rows(), 4);
    }

    #[test]
    fn test_extend_string_dictionary_promotes_not_panics() {
        let mut e1 = parse_bytes(b"P=x\n", ExecutionPlan::new());
        let mut plan = ExecutionPlan::new();
        plan.dictionary_columns.insert("P".to_string());
        let e2 = parse_bytes(b"P=x\n", plan);
        // Safe promotion: string + dictionary reconcile to dictionary.
        e1.extend(e2).expect("String/Dictionary must reconcile");
        assert!(matches!(
            e1.get_column("P").unwrap(),
            crate::columnar::ColumnBuilder::Dictionary { .. }
        ));
    }

    #[test]
    fn test_extend_irreconcilable_variants_error_not_panic() {
        let mut e1 = parse_bytes(b"S=x\n", ExecutionPlan::new());
        let mut plan = ExecutionPlan::new();
        plan.field_types.insert("S".to_string(), FieldType::Int64);
        let e2 = parse_bytes(b"S=7\n", plan);
        let result = e1.extend(e2);
        match result {
            Err(crate::Error::Merge(msg)) => {
                assert!(msg.contains("'S'"), "error should name the column: {msg}");
            }
            other => panic!("String/Int64 mismatch must return Merge error, got {other:?}"),
        }
    }

    #[test]
    fn test_compare_filter_via_arrow_compute() {
        use crate::arrow_export::apply_compare_filter;

        let mut plan = ExecutionPlan::new();
        plan.filter = Some(FilterPredicate::Compare {
            field_a: "A".to_string(),
            op: CompareOp::Gt,
            field_b: "B".to_string(),
        });
        let mut engine = parse_bytes(b"A=3 B=1\nA=2 B=2\nA=5 B=4\n", plan);
        let batch = engine.finish().unwrap();
        let filtered = apply_compare_filter(
            batch,
            &FilterPredicate::Compare {
                field_a: "A".to_string(),
                op: CompareOp::Gt,
                field_b: "B".to_string(),
            },
        )
        .unwrap();
        assert_eq!(filtered.num_rows(), 2);
        let a = filtered.column_by_name("A").unwrap().as_string::<i32>();
        assert_eq!(a.value(0), "3");
        assert_eq!(a.value(1), "5");
    }

    #[test]
    fn test_row_dirty_is_full_edge_cases() {
        // Directly test the is_full logic for ncols = 1,63,64,65,127,128,129
        // We do this via TableBuilder with varying column counts
        for ncols in [1, 63, 64, 65, 127, 128, 129] {
            let mut tb = TableBuilder::new();
            for i in 0..ncols {
                let name = format!("col{i}");
                tb.field_index.insert(name.clone(), i);
                tb.columns.push(crate::columnar::ColumnBuilder::String(
                    crate::columnar::StrColumn::default(),
                ));
                tb.column_order.push(name);
            }
            // row_dirty should be vec![0; (ncols+63)/64]
            tb.row_dirty = vec![0; ncols.div_ceil(64)];
            // Test is_full when all bits set
            for i in 0..ncols {
                let word = i / 64;
                let bit = i % 64;
                tb.row_dirty[word] |= 1u64 << bit;
            }
            let full_words = ncols / 64;
            let rem = ncols % 64;
            let is_full = (0..full_words).all(|w| tb.row_dirty[w] == u64::MAX)
                && (rem == 0
                    || tb.row_dirty.get(full_words).copied().unwrap_or(0) == (1u64 << rem) - 1);
            assert!(
                is_full,
                "ncols={ncols} should be full, row_dirty={:?}",
                tb.row_dirty
            );
            // Clear one bit and check not full
            if ncols > 0 {
                let word = (ncols - 1) / 64;
                let bit = (ncols - 1) % 64;
                tb.row_dirty[word] &= !(1u64 << bit);
                let is_full2 = (0..full_words).all(|w| tb.row_dirty[w] == u64::MAX)
                    && (rem == 0
                        || tb.row_dirty.get(full_words).copied().unwrap_or(0) == (1u64 << rem) - 1);
                assert!(
                    !is_full2,
                    "ncols={ncols} should not be full after clearing one bit"
                );
            }
        }
    }

    #[test]
    fn test_finish_twice_does_not_panic() {
        // L7: zero-copy export (mem::take) leaves builders empty. Calling
        // finish() twice should return an empty batch, not panic.
        let mut tb = parse_bytes(b"A=1 B=2\nA=3 B=4\n", ExecutionPlan::new());
        let batch1 = tb.finish().unwrap();
        assert_eq!(batch1.num_rows(), 2);
        // Second call: columns are empty after mem::take.
        let batch2 = tb.finish().unwrap();
        assert_eq!(batch2.num_rows(), 0);
    }

    #[test]
    fn test_c2_compound_reorder() {
        // L7: C2 reorder — Field2 == x AND Field1 == y must give the same
        // result as Field1 == y AND Field2 == x.
        let data = b"A=1 B=2\nA=2 B=2\nA=1 B=3\nA=2 B=3\n";
        let make_filter = |a: &str, va: &str, b: &str, vb: &str| -> ExecutionPlan {
            let mut plan = ExecutionPlan::new();
            plan.filter = Some(FilterPredicate::And(
                Box::new(FilterPredicate::Equal {
                    field: a.to_string(),
                    value: va.to_string(),
                }),
                Box::new(FilterPredicate::Equal {
                    field: b.to_string(),
                    value: vb.to_string(),
                }),
            ));
            plan
        };
        let r1 = parse_bytes(data, make_filter("A", "1", "B", "2"));
        let r2 = parse_bytes(data, make_filter("B", "2", "A", "1"));
        assert_eq!(
            r1.num_rows(),
            r2.num_rows(),
            "C2 reorder must produce identical results"
        );
        assert_eq!(r1.num_rows(), 1); // A=1 B=2
    }

    #[test]
    fn test_c2_or_reorder() {
        // L7: Or reorder — same result regardless of operand order.
        // Row 1: A=1 B=2 → A==1 → Pass
        // Row 2: A=2 B=2 → A!=1, B!=3 → Fail
        // Row 3: A=3 B=3 → B==3 → Pass
        let data = b"A=1 B=2\nA=2 B=2\nA=3 B=3\n";
        let make_filter = |a: &str, va: &str, b: &str, vb: &str| -> ExecutionPlan {
            let mut plan = ExecutionPlan::new();
            plan.filter = Some(FilterPredicate::Or(
                Box::new(FilterPredicate::Equal {
                    field: a.to_string(),
                    value: va.to_string(),
                }),
                Box::new(FilterPredicate::Equal {
                    field: b.to_string(),
                    value: vb.to_string(),
                }),
            ));
            plan
        };
        let r1 = parse_bytes(data, make_filter("A", "1", "B", "3"));
        let r2 = parse_bytes(data, make_filter("B", "3", "A", "1"));
        assert_eq!(
            r1.num_rows(),
            r2.num_rows(),
            "Or reorder must produce identical results"
        );
        assert_eq!(r1.num_rows(), 2); // rows 1 and 3
    }

    #[test]
    fn test_predicate_slot_created_after_mask_build() {
        // Regression: when the predicate column appears AFTER other columns
        // in document order, build_predicate_mask runs before the predicate
        // column exists in field_index, so its bit is never set. The old
        // early-return optimization prevented it from being added later.
        //
        // Data: A comes first, B is the predicate column (comes second).
        // Filter: B == "reject" → all rows should be rejected.
        let data = b"A=1 B=ok\nA=2 B=ok\nA=3 B=ok\n";
        let mut plan = ExecutionPlan::new();
        plan.filter = Some(FilterPredicate::Equal {
            field: "B".to_string(),
            value: "reject".to_string(),
        });
        let engine = parse_bytes(data, plan);
        assert_eq!(
            engine.num_rows(),
            0,
            "all rows should be rejected (B != reject)"
        );

        // Positive case: some rows pass
        let data2 = b"A=1 B=ok\nA=2 B=reject\nA=3 B=ok\n";
        let mut plan2 = ExecutionPlan::new();
        plan2.filter = Some(FilterPredicate::Equal {
            field: "B".to_string(),
            value: "ok".to_string(),
        });
        let engine2 = parse_bytes(data2, plan2);
        assert_eq!(engine2.num_rows(), 2, "rows 1 and 3 pass (B == ok)");
        let col = engine2.get_column("A").unwrap();
        assert_eq!(col.as_str_vec(), vec![Some("1".into()), Some("3".into())]);
    }

    #[test]
    fn test_predicate_first_via_resolve_and_put() {
        // Uses resolve_and_put (scanner path) instead of put_field (LineParser path)
        // to verify the predicate machinery works through the scanner's code path.
        let mut plan = ExecutionPlan::new();
        plan.filter = Some(FilterPredicate::Equal {
            field: "B".to_string(),
            value: "reject".to_string(),
        });
        let mut tb = TableBuilder::with_plan(16, Arc::new(plan));
        // Row 1: B=ok → should pass
        tb.begin_row();
        tb.resolve_and_put("A", Value::Str(Cow::Borrowed("1")));
        tb.resolve_and_put("B", Value::Str(Cow::Borrowed("ok")));
        tb.end_row();
        // Row 2: B=reject → should fail
        tb.begin_row();
        tb.resolve_and_put("A", Value::Str(Cow::Borrowed("2")));
        tb.resolve_and_put("B", Value::Str(Cow::Borrowed("reject")));
        tb.end_row();
        // Row 3: B=ok → should pass
        tb.begin_row();
        tb.resolve_and_put("A", Value::Str(Cow::Borrowed("3")));
        tb.resolve_and_put("B", Value::Str(Cow::Borrowed("ok")));
        tb.end_row();
        let batch = tb.finish().unwrap();
        assert_eq!(batch.num_rows(), 1, "only row 2 (B==reject) should pass");
    }

    #[test]
    fn test_owned_cow_values_survive_buffered_filter() {
        // Regression for the use-after-free behind this change: Value::Str
        // used to be `&'a str`, and the buffered filtered path transmutes
        // `Value<'a>` to `Value<'static>` to hold values until the predicate
        // is decided. Adapters that unescape entities produce owned Strings;
        // the owned buffer was dropped at the end of the scan function and
        // the transmuted reference dangled, so heap reuse corrupted accepted
        // rows after enough rejections. With `Str(Cow<'a, str>)` the owned
        // buffer moves into the row buffer.
        let mut plan = ExecutionPlan::new();
        plan.filter = Some(FilterPredicate::Equal {
            field: "B".to_string(),
            value: "ok".to_string(),
        });
        let mut tb = TableBuilder::with_plan(16, Arc::new(plan));
        for i in 0..50 {
            tb.begin_row();
            // Owned values, as produced by entity unescaping in an adapter.
            let a = Cow::Owned(format!("value-{i}-unescaped"));
            let b = Cow::Owned(if i % 2 == 0 { "ok".to_string() } else { "no".to_string() });
            tb.resolve_and_put("A", Value::Str(a));
            tb.resolve_and_put("B", Value::Str(b));
            tb.end_row();
        }
        assert_eq!(tb.num_rows(), 25, "even rows pass (B == ok)");
        let col = tb.get_column("A").unwrap();
        let vals = col.as_str_vec();
        assert_eq!(vals.len(), 25);
        assert_eq!(vals[0], Some("value-0-unescaped".to_string()));
        assert_eq!(vals[24], Some("value-48-unescaped".to_string()));
    }

    #[test]
    fn test_predicate_first_skip_via_resolve_and_put() {
        // Test the skip path: predicate on first field, all rows rejected.
        // This exercises the mark_predicate_slot + row_rejected + end_row path
        // that the scanner uses via resolve_and_put.
        let mut plan = ExecutionPlan::new();
        plan.filter = Some(FilterPredicate::Equal {
            field: "A".to_string(),
            value: "999".to_string(),
        });
        let mut tb = TableBuilder::with_plan(16, Arc::new(plan));
        for _i in 0..100 {
            tb.begin_row();
            tb.resolve_and_put("A", Value::Str(Cow::Borrowed("0")));
            tb.resolve_and_put("B", Value::Str(Cow::Borrowed("x")));
            tb.resolve_and_put("C", Value::Str(Cow::Borrowed("y")));
            tb.end_row();
        }
        let batch = tb.finish().unwrap();
        assert_eq!(
            batch.num_rows(),
            0,
            "all rows should be rejected (A=0 != 999)"
        );
    }

    #[test]
    fn test_sparse_column_predicate_skip() {
        // Regression test for ncols gate: sparse column appears only after
        // row 50, predicate on early field (A). The adaptive strategy must
        // not disable buffering because ncols is computed from the incomplete
        // column set before the sparse column appears.
        //
        // Data: 100 rows. Rows 1-50 have A, B, C. Rows 51-100 also have D.
        // Filter: A == "reject" → all rows rejected.
        // The skip must fire for all rows, not just the first few.
        let mut plan = ExecutionPlan::new();
        plan.filter = Some(FilterPredicate::Equal {
            field: "A".to_string(),
            value: "reject".to_string(),
        });
        let mut tb = TableBuilder::with_plan(128, Arc::new(plan));
        for i in 0..100 {
            tb.begin_row();
            tb.resolve_and_put("A", Value::Str(Cow::Borrowed("ok")));
            tb.resolve_and_put("B", Value::Str(Cow::Borrowed("x")));
            tb.resolve_and_put("C", Value::Str(Cow::Borrowed("y")));
            if i >= 50 {
                tb.resolve_and_put("D", Value::Str(Cow::Borrowed("z")));
            }
            tb.end_row();
        }
        let batch = tb.finish().unwrap();
        assert_eq!(batch.num_rows(), 0, "all rows rejected (A=ok != reject)");
    }

    #[test]
    fn test_sparse_column_predicate_pass() {
        // Like above, but predicate passes. The sparse column D should appear
        // only for rows 51-100; rows 1-50 should have D = null.
        let mut plan = ExecutionPlan::new();
        plan.filter = Some(FilterPredicate::Equal {
            field: "A".to_string(),
            value: "ok".to_string(),
        });
        let mut tb = TableBuilder::with_plan(128, Arc::new(plan));
        for _i in 0..100 {
            tb.begin_row();
            tb.resolve_and_put("A", Value::Str(Cow::Borrowed("ok")));
            tb.resolve_and_put("B", Value::Str(Cow::Borrowed("x")));
            tb.resolve_and_put("C", Value::Str(Cow::Borrowed("y")));
            if _i >= 50 {
                tb.resolve_and_put("D", Value::Str(Cow::Borrowed("z")));
            }
            tb.end_row();
        }
        let batch = tb.finish().unwrap();
        assert_eq!(batch.num_rows(), 100, "all rows pass (A=ok == ok)");
        // D column: null for first 50, "z" for last 50
        if let Some(col) = batch.column_by_name("D") {
            let arr = col.as_string::<i32>();
            for i in 0..50 {
                assert!(arr.is_null(i), "row {} should have D=null", i);
            }
            for i in 50..100 {
                assert!(
                    !arr.is_null(i),
                    "row {} should not be null (D len={}, total rows={})",
                    i,
                    arr.len(),
                    batch.num_rows()
                );
                assert_eq!(arr.value(i), "z", "rows 50+ should have D=z");
            }
        } else {
            panic!("D column should exist");
        }
    }

    #[test]
    fn test_sparse_late_predicate_ordinal() {
        // Regression: predicate on column E that only appears after row 30.
        // ncols gate must use schema/column count that includes E, not just
        // the columns seen so far.
        let mut plan = ExecutionPlan::new();
        plan.filter = Some(FilterPredicate::Equal {
            field: "E".to_string(),
            value: "999".to_string(),
        });
        let mut tb = TableBuilder::with_plan(128, Arc::new(plan));
        // First 30 rows: only A, B
        for _i in 0..30 {
            tb.begin_row();
            tb.resolve_and_put("A", Value::Str(Cow::Borrowed("x")));
            tb.resolve_and_put("B", Value::Str(Cow::Borrowed("y")));
            tb.end_row();
        }
        // Rows 31-100: A, B, C, D, E
        for _i in 30..100 {
            tb.begin_row();
            tb.resolve_and_put("A", Value::Str(Cow::Borrowed("x")));
            tb.resolve_and_put("B", Value::Str(Cow::Borrowed("y")));
            tb.resolve_and_put("C", Value::Str(Cow::Borrowed("c")));
            tb.resolve_and_put("D", Value::Str(Cow::Borrowed("d")));
            tb.resolve_and_put("E", Value::Str(Cow::Borrowed("e")));
            tb.end_row();
        }
        let batch = tb.finish().unwrap();
        // E="e" != "999" → all rows rejected
        assert_eq!(batch.num_rows(), 0, "all rows rejected (E=\"e\" != 999)");
    }

    #[test]
    fn raw_name_helpers_apply_rename_and_drop() {
        use crate::decoder::ColumnarSink;

        let plan = ExecutionPlan::new()
            .rename("raw", "renamed")
            .drop("ignored");
        let mut tb = TableBuilder::with_plan(2, Arc::new(plan));
        tb.begin_row();
        tb.resolve_and_put_raw(b"raw", Value::Str("value"));
        tb.resolve_and_put_raw(b"ignored", Value::Str("not stored"));
        tb.put_row(&[("other", Value::Str("also stored"))]);
        tb.end_row();

        assert_eq!(tb.column_names(), &["renamed", "other"]);
        assert_eq!(tb.resolve_raw(b"raw"), Some("renamed"));
    }
}
