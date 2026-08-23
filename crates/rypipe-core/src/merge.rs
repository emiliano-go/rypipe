use std::sync::Arc;

use arrow::datatypes::{Field as ArrowField, Schema};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use rustc_hash::FxHashMap as HashMap;

use crate::arrow_export::{apply_compare_filter, null_array};
use crate::engine::TableBuilder;
use crate::plan::ExecutionPlan;
use crate::Result;

impl TableBuilder {
    /// Merge another builder's data into this one, consuming `other`.
    ///
    /// New columns discovered in `other` are null-padded for the existing rows
    /// in `self`.  Columns present in `self` but absent from `other` are
    /// null-padded for `other`'s rows.  Column order follows first-appearance
    /// order across both builders.
    pub fn extend(&mut self, mut other: TableBuilder) -> Result<()> {
        let self_rows = self.row_count;
        let other_rows = other.row_count;

        // 1. Create columns from other that self doesn't have yet.
        let have: std::collections::HashSet<String> = self.column_order.iter().cloned().collect();
        for name in &other.column_order {
            if !have.contains(name) {
                let est = self_rows + other.estimated_rows.max(64);
                let col_type = other.plan.column_type(name);
                let mut builder = crate::columnar::ColumnBuilder::with_capacity(est, &col_type);
                for _ in 0..self_rows {
                    builder.push(None);
                }
                self.columns.insert(name.clone(), builder);
                let idx = self.schema_insert_index(name);
                self.column_order.insert(idx, name.clone());
            }
        }

        // 2. Append other's values to all columns, null-pad missing ones.
        for name in &self.column_order {
            if let Some(self_b) = self.columns.get_mut(name) {
                if let Some(other_b) = other.columns.remove(name) {
                    self_b.extend_owned(other_b)?;
                } else {
                    for _ in 0..other_rows {
                        self_b.push(None);
                    }
                }
            }
        }

        self.row_count = self_rows + other_rows;
        Ok(())
    }
}

/// Export per-chunk builders as `RecordBatch`es without merging them in
/// columnar form.  Each builder becomes a batch; the resulting vector has
/// chunked columns.  This skips the serial merge-then-re-copy that `extend`
/// would do.
///
/// The `Compare` filter, if present, is applied to each batch.
pub fn engines_to_record_batches(
    mut engines: Vec<TableBuilder>,
    plan: &ExecutionPlan,
) -> Result<Vec<RecordBatch>> {
    for e in engines.iter_mut() {
        e.normalize();
    }
    engines.retain(|e| e.row_count > 0);

    // Unified column order + datatypes across chunks. Types are deterministic
    // from the plan, so first sighting wins.
    let mut order: Vec<String> = Vec::new();
    let mut types: HashMap<String, arrow::datatypes::DataType> = HashMap::default();
    for e in &engines {
        for name in &e.column_order {
            if !types.contains_key(name) {
                if let Some(b) = e.columns.get(name) {
                    types.insert(name.clone(), b.arrow_datatype());
                    order.push(name.clone());
                }
            }
        }
    }

    if order.is_empty() {
        return Ok(Vec::new());
    }

    let fields: Vec<ArrowField> = order
        .iter()
        .map(|n| ArrowField::new(n.as_str(), types[n].clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let batches: Result<Vec<RecordBatch>> = engines
        .par_iter()
        .map(|e| {
            let mut arrays = Vec::with_capacity(order.len());
            for name in &order {
                match e.columns.get(name) {
                    Some(b) => arrays.push(b.to_arrow_array()?),
                    None => arrays.push(null_array(&types[name], e.row_count)),
                }
            }
            RecordBatch::try_new(schema.clone(), arrays).map_err(crate::Error::Arrow)
        })
        .collect();
    let mut batches = batches?;

    if let Some(ref filter) = plan.filter {
        for batch in &mut batches {
            *batch = apply_compare_filter(
                std::mem::replace(batch, RecordBatch::new_empty(schema.clone())),
                filter,
            )?;
        }
    }

    Ok(batches)
}
