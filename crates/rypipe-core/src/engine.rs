use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Diagnostic: counts resolve_and_put calls across all TableBuilder instances.
pub static RESOLVE_AND_PUT_COUNT: AtomicUsize = AtomicUsize::new(0);

use arrow::datatypes::{Field as ArrowField, Schema};
use arrow::record_batch::RecordBatch;
use rustc_hash::FxHashMap as HashMap;

use crate::columnar::ColumnBuilder;
use crate::decoder::ColumnarSink;
use crate::plan::ExecutionPlan;
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
    pub(crate) plan: ExecutionPlan,
    /// Dirty mask for the current row: bit `i` set iff column `i` received a
    /// value in this row. `Vec<u64>` word array so >64 columns work (e.g.,
    /// Crystal Reports exports with >64 fields). One compare `mask != full`
    /// replaces `for col in 0..ncols` loop when every field is present (dense).
    pub(crate) row_dirty: Vec<u64>,
}

impl TableBuilder {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            field_index: HashMap::default(),
            column_order: Vec::new(),
            row_count: 0,
            estimated_rows: 0,
            plan: ExecutionPlan::new(),
            row_dirty: Vec::new(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            columns: Vec::new(),
            field_index: HashMap::default(),
            column_order: Vec::new(),
            row_count: 0,
            estimated_rows: cap,
            plan: ExecutionPlan::new(),
            row_dirty: Vec::new(),
        }
    }

    pub fn with_plan(cap: usize, plan: ExecutionPlan) -> Self {
        Self {
            columns: Vec::new(),
            field_index: HashMap::default(),
            column_order: Vec::new(),
            row_count: 0,
            estimated_rows: cap,
            plan,
            row_dirty: Vec::new(),
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
            self.field_index.insert(name_str.to_string(), self.columns.len() - 1);
            self.column_order.push(name_str.to_string());
        }
        // Resize row_dirty bitmask to cover all columns.
        let words = (self.columns.len() + 63) / 64;
        self.row_dirty.resize(words, 0);
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
            plan: self.plan.clone(),
            row_dirty: vec![0; (self.columns.len() + 63) / 64],
        };
        for (idx, col) in self.columns.iter_mut().enumerate() {
            let drain = col.split_off(n);
            other.columns.push(drain);
            // Remainder stays in self.columns[idx]
            let _ = idx;
        }
        self.row_count -= n;
        // row_dirty for self should be all false (no dirty in remainder yet)
        self.row_dirty = vec![0; (self.columns.len() + 63) / 64];
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
        let new_len = (self.columns.len() + 63) / 64;
        self.row_dirty.resize(new_len, 0);
        // Clear all bits (no dirty in remainder for take_column case)
        for w in &mut self.row_dirty {
            *w = 0;
        }
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
            let moved_name = self
                .field_index
                .iter()
                .find_map(|(k, &v)| if v == old_last { Some(k.clone()) } else { None });
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
        self.column_order.iter().map(|name| {
            let idx = self.field_index[name];
            let col = &self.columns[idx];
            (name.clone(), col.bytes_used(), col.capacity_bytes())
        }).collect()
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
        for w in &mut self.row_dirty { *w = 0; }
        self.row_count = 0;
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
        for w in &mut self.row_dirty {
            *w = 0;
        }
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
        let est = self.estimated_rows.max(64);
        let col_type = self.plan.column_type(name);
        let mut b = ColumnBuilder::with_capacity(est, &col_type);
        for _ in 0..self.row_count {
            b.push(None);
        }
        let idx = self.columns.len();
        self.columns.push(b);
        self.field_index.insert(name.to_owned(), idx);
        // Ensure row_dirty has enough words for new column
        let needed = (self.columns.len() + 63) / 64;
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
        let idx = self.ensure_column_idx(resolved_name);
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
        // Fast path: no rename/drop configured.
        let owned;
        let resolved: &str = if self.plan.field_map.is_empty() && self.plan.drop_fields.is_empty() {
            name
        } else {
            match self.plan.resolve_field(name) {
                Some(n) => {
                    owned = n.to_owned();
                    &owned
                }
                None => return,
            }
        };

        self.push_field_resolved(resolved, value);
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
            && (rem_bits == 0 || self.row_dirty.get(full_words).copied().unwrap_or(0) == (1u64 << rem_bits) - 1);
        if is_full {
            for w in &mut self.row_dirty {
                *w = 0;
            }
        } else {
            for (i, b) in self.columns.iter_mut().enumerate() {
                let word = i / 64;
                let bit = i % 64;
                let is_set = (self.row_dirty[word] >> bit) & 1 == 1;
                if !is_set {
                    b.push(None);
                }
            }
            for w in &mut self.row_dirty {
                *w = 0;
            }
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
}

impl Default for TableBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnarSink for TableBuilder {
    #[inline]
    fn begin_row(&mut self) {
        // Row boundaries are tracked by `row_count`; no state to set up.
    }

    #[inline]
    fn put_field(&mut self, name: &str, value: Value<'_>) {
        self.push_field(name, value);
    }

    #[inline]
    fn end_row(&mut self) {
        self.finish_row();
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
        self.push_field_resolved(resolved_name, value);
    }

    #[inline]
    fn resolve_and_put(&mut self, name: &str, value: Value<'_>) {
        RESOLVE_AND_PUT_COUNT.fetch_add(1, Ordering::Relaxed);
        if self.plan.field_map.is_empty() && self.plan.drop_fields.is_empty() {
            // Common case: no renames/drops, push directly
            self.push_field_resolved(name, value);
        } else {
            if let Some(resolved) = self.plan.resolve_field(name) {
                let owned = resolved.to_owned();
                self.push_field_resolved(&owned, value);
            }
        }
    }

    #[inline]
    fn finish(&mut self) -> Result<RecordBatch> {
        self.normalize();

        if self.column_order.is_empty() {
            let schema = Arc::new(Schema::empty());
            return Ok(RecordBatch::new_empty(schema));
        }

        self.auto_dict_upgrade();
        self.sort_columns();

        let mut fields = Vec::with_capacity(self.column_order.len());
        let mut arrays = Vec::with_capacity(self.column_order.len());
        for name in &self.column_order {
            if let Some(b) = self.get_column(name) {
                fields.push(ArrowField::new(name.as_str(), b.arrow_datatype(), true));
                arrays.push(b.to_arrow_array()?);
            }
        }

        let schema = Arc::new(Schema::new(fields));
        Ok(RecordBatch::try_new(schema, arrays)?)
    }
}

// ---------------------------------------------------------------------------
// LocateOnly sink — walk rows, resolve field names, decode nothing.
// ---------------------------------------------------------------------------

/// A zero-allocation sink that counts rows and fields without decoding or
/// storing any values.  Use this to measure the cost of the scan + locate
/// phase independently from extract + store.

pub struct LocateOnly {
    pub row_count: usize,
    pub field_count: usize,
    pub distinct_fields: rustc_hash::FxHashSet<String>,
    plan: ExecutionPlan,
}

impl LocateOnly {
    pub fn new(plan: ExecutionPlan) -> Self {
        Self {
            row_count: 0,
            field_count: 0,
            distinct_fields: rustc_hash::FxHashSet::default(),
            plan,
        }
    }

    /// Total fields seen across all rows.
    pub fn total_fields(&self) -> usize {
        self.field_count
    }

    /// Number of distinct field names encountered.
    pub fn num_distinct_fields(&self) -> usize {
        self.distinct_fields.len()
    }
}

impl ColumnarSink for LocateOnly {
    #[inline]
    fn begin_row(&mut self) {}

    #[inline]
    fn put_field(&mut self, name: &str, _value: Value<'_>) {
        self.field_count += 1;
        if let Some(resolved) = self.plan.resolve_field(name) {
            self.distinct_fields.insert(resolved.to_owned());
        }
    }

    #[inline]
    fn end_row(&mut self) {
        self.row_count += 1;
    }

    #[inline]
    fn wants(&self, _name: &str) -> bool {
        true
    }

    #[inline]
    fn needs_value(&self) -> bool {
        false
    }

    #[inline]
    fn resolve<'a>(&'a self, name: &'a str) -> Option<&'a str> {
        self.plan.resolve_field(name)
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::empty());
        Ok(RecordBatch::new_empty(schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::ColumnBuilder;
    use crate::decoder::{ColumnarSink, RecordParser, Splitter};
    use crate::plan::{CompareOp, ExecutionPlan, FieldType, FilterPredicate};
    use crate::value::Value;
    use crate::Result;
    use arrow::array::AsArray;

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
                        sink.put_field(k, Value::Str(v));
                    }
                }
                sink.end_row();
            }
            Ok(())
        }
    }

    struct LineSplitter;

    impl Splitter for LineSplitter {
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
        let mut sink = TableBuilder::with_plan((bytes.len() / 16).max(4), plan);
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
            assert_eq!(v, &vec![Some(42), None, Some(100)]);
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
            assert!((v[0].unwrap() - 1.5).abs() < 1e-9);
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
            assert_eq!(codes, &vec![Some(0), Some(1), Some(0)]);
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
        let mut merged = TableBuilder::with_plan(64, plan);
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
            tb.row_dirty = vec![0; (ncols + 63) / 64];
            // Test is_full when all bits set
            for i in 0..ncols {
                let word = i / 64;
                let bit = i % 64;
                tb.row_dirty[word] |= 1u64 << bit;
            }
            let full_words = ncols / 64;
            let rem = ncols % 64;
            let is_full = (0..full_words).all(|w| tb.row_dirty[w] == u64::MAX)
                && (rem == 0 || tb.row_dirty.get(full_words).copied().unwrap_or(0) == (1u64 << rem) - 1);
            assert!(is_full, "ncols={ncols} should be full, row_dirty={:?}", tb.row_dirty);
            // Clear one bit and check not full
            if ncols > 0 {
                let word = (ncols - 1) / 64;
                let bit = (ncols - 1) % 64;
                tb.row_dirty[word] &= !(1u64 << bit);
                let is_full2 = (0..full_words).all(|w| tb.row_dirty[w] == u64::MAX)
                    && (rem == 0 || tb.row_dirty.get(full_words).copied().unwrap_or(0) == (1u64 << rem) - 1);
                assert!(!is_full2, "ncols={ncols} should not be full after clearing one bit");
            }
        }
    }
}
