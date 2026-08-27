//! Helpers for exporting Arrow batches to PyArrow objects.
//!
//! These functions are `pub` so that downstream wrapper crates (e.g. a future
//! `crxml` shim) can reuse the same export / concatenation logic.

use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

/// Export a single Arrow `RecordBatch` to a `pyarrow.RecordBatch`.
pub fn record_batch_to_pyarrow<'py>(
    py: Python<'py>,
    batch: &RecordBatch,
) -> PyResult<Bound<'py, PyAny>> {
    batch.to_pyarrow(py)
}

/// Export a slice of Arrow `RecordBatch`es as a Python list of
/// `pyarrow.RecordBatch` objects (no concatenation).
///
/// Useful for streaming-style APIs: callers can iterate the list and process
/// each batch incrementally instead of materializing one big table.
pub fn record_batches_to_pyarrow_batches<'py>(
    py: Python<'py>,
    batches: &[RecordBatch],
) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty(py);
    for batch in batches {
        list.append(record_batch_to_pyarrow(py, batch)?)?;
    }
    Ok(list)
}

/// Export a slice of Arrow `RecordBatch`es to a single `pyarrow.Table`.
///
/// Mirrors crxml's `concat_tables` logic:
/// * If all batch schemas match, concatenate without promotion.
/// * If schemas differ (e.g. auto-dict promotion), use
///   `promote_options="default"`.
pub fn record_batches_to_pyarrow_table<'py>(
    py: Python<'py>,
    batches: &[RecordBatch],
) -> PyResult<Bound<'py, PyAny>> {
    let pa = PyModule::import(py, "pyarrow")?;

    if batches.is_empty() {
        return pa.call_method1("table", (PyDict::new(py),));
    }

    // Convert each batch into a one-batch pyarrow.Table. We keep them as
    // individual tables so we can reuse crxml's concat_tables promotion path.
    let tables = PyList::empty(py);
    for batch in batches {
        let batch_obj = record_batch_to_pyarrow(py, batch)?;
        let table_list = PyList::new(py, vec![batch_obj])?;
        let table = pa
            .getattr("Table")?
            .call_method1("from_batches", (table_list,))?;
        tables.append(table)?;
    }

    if batches.len() == 1 {
        return tables.get_item(0);
    }

    let schemas_match = batches
        .iter()
        .skip(1)
        .all(|b| b.schema() == batches[0].schema());

    if schemas_match {
        Ok(pa.call_method1("concat_tables", (tables,))?)
    } else {
        let kwargs = PyDict::new(py);
        kwargs.set_item("promote_options", "default")?;
        Ok(pa.call_method("concat_tables", (tables,), Some(&kwargs))?)
    }
}
