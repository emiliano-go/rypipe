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
    pub(crate) columns: HashMap<String, ColumnBuilder>,
    pub(crate) column_order: Vec<String>,
    pub(crate) row_count: usize,
    pub(crate) estimated_rows: usize,
    pub(crate) plan: ExecutionPlan,
}

impl TableBuilder {
    pub fn new() -> Self {
        Self {
            columns: HashMap::default(),
            column_order: Vec::new(),
            row_count: 0,
            estimated_rows: 0,
            plan: ExecutionPlan::new(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            columns: HashMap::default(),
            column_order: Vec::new(),
            row_count: 0,
            estimated_rows: cap,
            plan: ExecutionPlan::new(),
        }
    }

    pub fn with_plan(cap: usize, plan: ExecutionPlan) -> Self {
        Self {
            columns: HashMap::default(),
            column_order: Vec::new(),
            row_count: 0,
            estimated_rows: cap,
            plan,
        }
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
        self.column_order.clear();
        self.row_count = 0;
    }

    /// Truncate every column back to `row_count`, dropping any partial-row
    /// values from a mid-field EOF.  Idempotent.
    pub fn normalize(&mut self) {
        for b in self.columns.values_mut() {
            while b.len() > self.row_count {
                b.pop();
            }
        }
    }

    /// If `auto_dict` is set, upgrade low-cardinality string columns.
    pub fn auto_dict_upgrade(&mut self) {
        if self.plan.auto_dict {
            for b in self.columns.values_mut() {
                b.try_upgrade_to_dict(512);
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

    fn ensure_column(&mut self, name: &str) {
        if !self.columns.contains_key(name) {
            let est = self.estimated_rows.max(64);
            let col_type = self.plan.column_type(name);
            let mut b = ColumnBuilder::with_capacity(est, &col_type);
            for _ in 0..self.row_count {
                b.push(None);
            }
            self.columns.insert(name.to_owned(), b);
            let idx = self.schema_insert_index(name);
            self.column_order.insert(idx, name.to_owned());
        }
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

        self.ensure_column(resolved);
        let row_count = self.row_count;
        if let Some(b) = self.columns.get_mut(resolved) {
            if b.len() > row_count {
                b.pop();
            }
            b.push_value(value);
        }
    }

    /// Null-fill any column missing this row, then apply the per-row filter.
    /// If the filter rejects the row, undo it by popping values.
    fn finish_row(&mut self) {
        let target = self.row_count + 1;
        for b in self.columns.values_mut() {
            while b.len() < target {
                b.push(None);
            }
        }

        if let Some(ref filter) = self.plan.filter {
            if !filter.check(&self.columns, self.row_count, &self.plan) {
                for b in self.columns.values_mut() {
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
        self.plan.resolve_field(name).is_some()
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        self.normalize();

        if self.column_order.is_empty() {
            let schema = Arc::new(Schema::empty());
            return Ok(RecordBatch::try_new(schema, Vec::new())?);
        }

        self.auto_dict_upgrade();
        self.sort_columns();

        let mut fields = Vec::with_capacity(self.column_order.len());
        let mut arrays = Vec::with_capacity(self.column_order.len());
        for name in &self.column_order {
            if let Some(b) = self.columns.get(name) {
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
        for (name, col) in &merged.columns {
            assert_eq!(
                col.len(),
                merged.num_rows(),
                "column {} length mismatch",
                name
            );
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
        let col = engine.columns.get("X").unwrap();
        assert_eq!(col.as_str_vec(), vec![Some("20".into())]);
    }

    #[test]
    fn test_build_plan_rename() {
        let mut plan = ExecutionPlan::new();
        plan.field_map.insert("X".to_string(), "Y".to_string());
        let engine = parse_bytes(b"X=hello\n", plan);
        assert_eq!(engine.num_rows(), 1);
        assert!(engine.columns.contains_key("Y"));
        assert!(!engine.columns.contains_key("X"));
        assert_eq!(
            engine.columns.get("Y").unwrap().as_str_vec(),
            vec![Some("hello".into())]
        );
    }

    #[test]
    fn test_build_plan_drop() {
        let mut plan = ExecutionPlan::new();
        plan.drop_fields.insert("X".to_string());
        let engine = parse_bytes(b"X=hello Y=world\n", plan);
        assert_eq!(engine.num_rows(), 1);
        assert!(!engine.columns.contains_key("X"));
        assert!(engine.columns.contains_key("Y"));
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
        let col = engine.columns.get("X").unwrap();
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
        let col = engine.columns.get("X").unwrap();
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
        let col = engine.columns.get("Y").unwrap();
        assert_eq!(col.as_str_vec(), vec![Some("99".into())]);
    }

    #[test]
    fn test_typed_int64_column() {
        let mut plan = ExecutionPlan::new();
        plan.field_types.insert("X".to_string(), FieldType::Int64);
        let engine = parse_bytes(b"X=42\nX=bad\nX=100\n", plan);
        assert_eq!(engine.num_rows(), 3);
        if let ColumnBuilder::Int64(v) = &engine.columns["X"] {
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
        if let ColumnBuilder::Float64(v) = &engine.columns["X"] {
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
        if let ColumnBuilder::Dictionary { codes, dict, .. } = &engine.columns["P"] {
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
            merged.columns["A"].as_str_vec(),
            vec![Some("1".into()), Some("3".into()), None]
        );
        assert_eq!(
            merged.columns["B"].as_str_vec(),
            vec![Some("2".into()), None, Some("4".into())]
        );
        assert_eq!(
            merged.columns["C"].as_str_vec(),
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
    fn test_extend_variant_mismatch_errors_not_panics() {
        let mut e1 = parse_bytes(b"P=x\n", ExecutionPlan::new());
        let mut plan = ExecutionPlan::new();
        plan.dictionary_columns.insert("P".to_string());
        let e2 = parse_bytes(b"P=x\n", plan);
        let result = e1.extend(e2);
        assert!(
            result.is_err(),
            "String/Dictionary mismatch must return Err"
        );
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
