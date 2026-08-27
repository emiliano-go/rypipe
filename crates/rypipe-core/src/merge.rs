use std::sync::Arc;

use arrow::datatypes::{DataType, Field as ArrowField, Schema};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use rustc_hash::FxHashMap as HashMap;

use crate::arrow_export::{apply_compare_filter, null_array};
use crate::columnar::unify_variants;
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
    ///
    /// Columns that disagree on storage variant are reconciled with safe
    /// promotions (`int64`→`float64`, `string`→`dictionary`); irreconcilable
    /// conflicts return [`crate::Error::Merge`].
    pub fn extend(&mut self, mut other: TableBuilder) -> Result<()> {
        let self_rows = self.row_count;
        let other_rows = other.row_count;

        // 1. Create columns from other that self doesn't have yet.
        for name in &other.column_order.clone() {
            if !self.field_index.contains_key(name) {
                let est = self_rows + other.estimated_rows.max(64);
                let col_type = other.plan.column_type(name);
                let mut builder = crate::columnar::ColumnBuilder::with_capacity(est, &col_type);
                for _ in 0..self_rows {
                    builder.push(None);
                }
                let idx = self.columns.len();
                self.columns.push(builder);
                self.field_index.insert(name.clone(), idx);
                self.row_dirty.push(false);
                let order_idx = self.schema_insert_index(name);
                self.column_order.insert(order_idx, name.clone());
            }
        }

        // 2. Append other's values to all columns, null-pad missing ones.
        // Clone column_order to avoid borrowing self while mutably borrowing columns.
        let order_snapshot = self.column_order.clone();
        for name in &order_snapshot {
            if let Some(self_idx) = self.field_index.get(name).copied() {
                let self_b = &mut self.columns[self_idx];
                if let Some(mut other_b) = other.take_column(name) {
                    let skey = self_b.variant_key();
                    let okey = other_b.variant_key();
                    if skey != okey {
                        let target = unify_variants(skey, okey).ok_or_else(|| {
                            crate::Error::Merge(format!(
                                "column '{name}' has conflicting types across chunks \
                                 ({skey} vs {okey}); provide explicit field_types"
                            ))
                        })?;
                        self_b.promote_to_variant(target)?;
                        other_b.promote_to_variant(target)?;
                    }
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
/// All batches share a single unified schema: column order follows
/// first appearance, chunks missing a column are null-filled, and columns
/// whose storage variants differ across chunks are promoted to their common
/// type where safe (`int64`+`float64` → `float64`,
/// `string`+`dictionary` → `dictionary`). Irreconcilable conflicts raise
/// [`crate::Error::Merge`].
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

    // Unified column order + storage variant across chunks. Order follows
    // first appearance; conflicting variants are folded through
    // `unify_variants` instead of silently keeping the first sighting.
    let mut order: Vec<String> = Vec::new();
    let mut targets: HashMap<String, &'static str> = HashMap::default();
    for e in &engines {
        for name in &e.column_order {
            let Some(b) = e.get_column(name.as_str()) else {
                continue;
            };
            match targets.get_mut(name) {
                None => {
                    order.push(name.clone());
                    targets.insert(name.clone(), b.variant_key());
                }
                Some(t) => {
                    let key = b.variant_key();
                    *t = unify_variants(t, key).ok_or_else(|| {
                        crate::Error::Merge(format!(
                            "column '{name}' has conflicting types across chunks \
                             ({t} vs {key}); provide explicit field_types"
                        ))
                    })?;
                }
            }
        }
    }

    if order.is_empty() {
        return Ok(Vec::new());
    }

    // Promote every builder to the unified variant so all exported arrays
    // share one Arrow type per column.
    for e in engines.iter_mut() {
        for name in &e.column_order.clone() {
            if let Some(target) = targets.get(name).copied() {
                if let Some(b) = e.get_column_mut(name) {
                    b.promote_to_variant(target)?;
                }
            }
        }
    }

    // With promotion complete, each column's Arrow datatype is deterministic
    // from its unified variant; first sighting suffices.
    let mut types: HashMap<String, DataType> = HashMap::default();
    for e in &engines {
        for name in &e.column_order {
            types.entry(name.clone()).or_insert_with(|| {
                e.get_column(name.as_str())
                    .expect("column must exist after promotion")
                    .arrow_datatype()
            });
        }
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
                match e.get_column(name) {
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
