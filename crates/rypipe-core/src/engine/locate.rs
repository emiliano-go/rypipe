use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use rustc_hash::FxHashSet;

use crate::decoder::ColumnarSink;
use crate::plan::ExecutionPlan;
use crate::value::Value;
use crate::Result;

/// A zero-allocation sink that counts rows and fields without decoding or
/// storing any values.  Use this to measure the cost of the scan + locate
/// phase independently from extract + store.
pub struct LocateOnly {
    pub row_count: usize,
    pub field_count: usize,
    pub distinct_fields: FxHashSet<String>,
    plan: ExecutionPlan,
}

impl LocateOnly {
    pub fn new(plan: ExecutionPlan) -> Self {
        Self {
            row_count: 0,
            field_count: 0,
            distinct_fields: FxHashSet::default(),
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
