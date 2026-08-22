use arrow::array::{ArrayRef, AsArray, BooleanArray};
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::compute::kernels::cast::cast;
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Float64Type};
use arrow::record_batch::RecordBatch;

use crate::plan::{CompareOp, FilterPredicate};
use crate::Result;

/// Build a null array of `datatype` with `len` rows.
pub fn null_array(datatype: &DataType, len: usize) -> ArrayRef {
    arrow::array::new_null_array(datatype, len)
}

/// Apply a column-to-column `Compare` filter to an assembled `RecordBatch`.
///
/// This is evaluated entirely in Rust using `arrow::compute` comparison
/// kernels and `filter_record_batch`.  No Python callable is invoked.
/// Per-row `Equal`/`NotEqual` filters are already applied during row assembly
/// and are a no-op here.
pub fn apply_compare_filter(batch: RecordBatch, predicate: &FilterPredicate) -> Result<RecordBatch> {
    let FilterPredicate::Compare { field_a, op, field_b } = predicate else {
        return Ok(batch);
    };

    let col_a = batch
        .column_by_name(field_a)
        .ok_or_else(|| crate::Error::Plan(format!("compare filter references unknown column {field_a:?}")))?;
    let col_b = batch
        .column_by_name(field_b)
        .ok_or_else(|| crate::Error::Plan(format!("compare filter references unknown column {field_b:?}")))?;

    let mask = compare_columns(col_a, col_b, *op)?;
    Ok(filter_record_batch(&batch, &mask)?)
}

fn is_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Boolean
    )
}

fn compare_columns(col_a: &ArrayRef, col_b: &ArrayRef, op: CompareOp) -> Result<BooleanArray> {
    let dt_a = col_a.data_type();
    let dt_b = col_b.data_type();

    // Choose a common concrete type so the `Datum` trait is satisfied by a
    // concrete array reference.  Numeric columns are compared as f64;
    // everything else is compared as UTF-8.
    let (a, b): (ArrayRef, ArrayRef) = if is_numeric(dt_a) && is_numeric(dt_b) {
        (cast(col_a, &DataType::Float64)?, cast(col_b, &DataType::Float64)?)
    } else {
        (cast(col_a, &DataType::Utf8)?, cast(col_b, &DataType::Utf8)?)
    };

    match a.data_type() {
        DataType::Float64 => {
            let a = a.as_primitive::<Float64Type>();
            let b = b.as_primitive::<Float64Type>();
            Ok(match op {
                CompareOp::Gt => gt(a, b)?,
                CompareOp::Lt => lt(a, b)?,
                CompareOp::Ge => gt_eq(a, b)?,
                CompareOp::Le => lt_eq(a, b)?,
                CompareOp::Eq => eq(a, b)?,
                CompareOp::Ne => neq(a, b)?,
            })
        }
        DataType::Utf8 => {
            let a = a.as_string::<i32>();
            let b = b.as_string::<i32>();
            Ok(match op {
                CompareOp::Gt => gt(a, b)?,
                CompareOp::Lt => lt(a, b)?,
                CompareOp::Ge => gt_eq(a, b)?,
                CompareOp::Le => lt_eq(a, b)?,
                CompareOp::Eq => eq(a, b)?,
                CompareOp::Ne => neq(a, b)?,
            })
        }
        other => Err(crate::Error::Plan(format!(
            "unsupported common type for compare filter: {other}"
        ))),
    }
}
