use std::sync::Arc;

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
    /// Dirty mask for the current row: true iff column `i` received a value
    /// in this row. Used to null-fill only missing columns in `finish_row`
    /// instead of iterating all columns and checking `len < target`.
    pub(crate) row_dirty: Vec<bool>,
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

    /// Remove and return a column by name, fixing the Vec index map.
    /// Used by `merge::extend` to move builders out of the `other` table.
    pub(crate) fn take_column(&mut self, name: &str) -> Option<ColumnBuilder> {
        let idx = self.field_index.remove(name)?;
        // Keep row_dirty in sync with columns (order not important for `other`'s remaining dirty state
        // since `other` is consumed, but we must keep Vec lengths equal).
        let _ = if self.row_dirty.len() > idx {
            if idx == self.row_dirty.len() - 1 {
                self.row_dirty.pop().unwrap();
                false
            } else {
                self.row_dirty.swap_remove(idx);
                true
            }
        } else {
            false
        };
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
        self.row_dirty.clear();
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
        for v in &mut self.row_dirty {
            *v = false;
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
        self.row_dirty.push(false);
        let order_idx = self.schema_insert_index(name);
        self.column_order.insert(order_idx, name.to_owned());
        idx
    }

    /// Push a field value without resolving renames/drops.
    /// Caller must have already resolved `resolved_name` (or know it is kept).
    fn push_field_resolved(&mut self, resolved_name: &str, value: Value<'_>) {
        let idx = self.ensure_column_idx(resolved_name);
        self.row_dirty[idx] = true;
        let b = &mut self.columns[idx];
        let row_count = self.row_count;
        if b.len() > row_count {
            b.pop();
        }
        b.push_value(value);
    }

    /// Push a field value, resolving renames/drops and applying last-write-wins
    /// within the current uncommitted row.
    fn push_field(&mut self, name: &str, value: Value<'_>) {
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

    /// Null-fill any column missing this row, then apply the per-row filter.
    /// If the filter rejects the row, undo it by popping values.
    /// Uses the dirty bitmask so only missing columns are touched.
    fn finish_row(&mut self) {
        // Null-fill missing columns: dirty == false means not touched this row.
        for (i, b) in self.columns.iter_mut().enumerate() {
            if !self.row_dirty[i] {
                b.push(None);
            } else {
                // Clear for next row.
                self.row_dirty[i] = false;
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
    fn begin_row(&mut self) {
        // Row boundaries are tracked by `row_count`; no state to set up.
    }

    fn put_field(&mut self, name: &str, value: Value<'_>) {
        self.push_field(name, value);
    }

    fn end_row(&mut self) {
        self.finish_row();
    }

    fn wants(&self, name: &str) -> bool {
        self.resolve(name).is_some()
    }

    fn resolve<'a>(&'a self, name: &'a str) -> Option<&'a str> {
        self.plan.resolve_field(name)
    }

    fn put_field_resolved(&mut self, resolved_name: &str, value: Value<'_>) {
        self.push_field_resolved(resolved_name, value);
    }

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
}
