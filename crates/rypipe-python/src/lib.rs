//! `rypipe-python`: PyO3 bindings over `rypipe-core` and `rypipe-xml`.
//!
//! Exposes the same columnar entry points as crxml (`read_to_columnar`,
//! `read_to_columnar_multi`, `read_to_columnar_par`, `read_to_columnar_bounded`)
//! and reusable Rust helpers in [`export`] for exporting Arrow batches to
//! Python.

use arrow::record_batch::RecordBatch;
use pyo3::exceptions::{PyFileNotFoundError, PyIOError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::wrap_pyfunction;
use std::collections::HashMap;
use std::path::Path;

use rypipe_core::{
    apply_compare_filter, bounded::MemoryBudget, decoder::RecordParser, decoder::Splitter,
    parallel::ParallelExecutor, InputBuffer, TableBuilder,
};
use rypipe_xml::{CrystalXmlDecoder, CrystalXmlSplitter};

mod export;
mod plan_kwargs;

pub use export::{record_batch_to_pyarrow, record_batches_to_pyarrow_table};
pub use plan_kwargs::execution_plan_from_kwargs;

// Fast allocator: replaces the system heap for Rust-side allocations.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// Typed exceptions so callers can distinguish failure classes:
// XmlError: malformed/unparseable XML input; PlanError: invalid pushdown
// plan kwargs (bad ops, unknown types); MergeError: chunk-merge conflicts.
pyo3::create_exception!(_rypipe, XmlError, pyo3::exceptions::PyException);
pyo3::create_exception!(_rypipe, PlanError, pyo3::exceptions::PyException);
pyo3::create_exception!(_rypipe, MergeError, pyo3::exceptions::PyException);

/// Convert a `rypipe_core::Error` into an appropriate Python exception.
fn py_err_from_rypipe(err: rypipe_core::Error) -> PyErr {
    match err {
        rypipe_core::Error::Utf8(e) => XmlError::new_err(format!("invalid UTF-8: {e}")),
        rypipe_core::Error::Plan(msg) => {
            // XML parse errors are currently funnelled through Error::Plan by
            // the rypipe-xml adapter; surface them as XmlError for callers.
            if msg.starts_with("XML parse error") || msg.contains("invalid UTF-8") {
                XmlError::new_err(msg)
            } else {
                PlanError::new_err(msg)
            }
        }
        rypipe_core::Error::Merge(msg) => MergeError::new_err(msg),
        rypipe_core::Error::Io(e) => PyIOError::new_err(e.to_string()),
        rypipe_core::Error::Arrow(e) => pyo3::exceptions::PyException::new_err(format!(
            "Arrow error: {e}"
        )),
    }
}

#[pyfunction]
#[pyo3(signature = (
    path,
    row_tag=None,
    field_mapping=None,
    drop_fields=None,
    filter=None,
    field_types=None,
    dictionary_columns=None,
    use_mmap=false,
    schema=None,
    auto_dict=false,
    prefault=false
))]
#[allow(clippy::too_many_arguments)]
fn read_to_columnar(
    py: Python<'_>,
    path: String,
    row_tag: Option<String>,
    field_mapping: Option<HashMap<String, String>>,
    drop_fields: Option<Vec<String>>,
    filter: Option<HashMap<String, String>>,
    field_types: Option<HashMap<String, String>>,
    dictionary_columns: Option<Vec<String>>,
    use_mmap: bool,
    schema: Option<Vec<String>>,
    auto_dict: bool,
    prefault: bool,
) -> PyResult<PyObject> {
    let plan = execution_plan_from_kwargs(
        field_mapping,
        drop_fields,
        filter,
        field_types,
        dictionary_columns,
        schema,
        auto_dict,
    )?;

    let p = Path::new(&path);
    if !p.is_file() {
        return Err(PyFileNotFoundError::new_err(format!(
            "No such file or directory: {path}"
        )));
    }
    let row_tag = row_tag.unwrap_or_else(|| "Row".to_string()).into_bytes();

    let batch = py.allow_threads(|| -> PyResult<RecordBatch> {
        let input = InputBuffer::open(p, use_mmap, prefault).map_err(|e| {
            PyIOError::new_err(format!("Cannot open {}: {}", path, e))
        })?;
        let bytes = input.as_slice();
        let mut engine = TableBuilder::with_plan((bytes.len() / 512).max(64), plan.clone());
        let decoder = CrystalXmlDecoder::with_row_tag(&row_tag);
        decoder
            .validate(bytes)
            .map_err(py_err_from_rypipe)?;
        decoder
            .parse_chunk(bytes, &mut engine)
            .map_err(py_err_from_rypipe)?;
        let mut batch = engine.finish().map_err(py_err_from_rypipe)?;
        if let Some(ref filter) = plan.filter {
            batch = apply_compare_filter(batch, filter).map_err(py_err_from_rypipe)?;
        }
        Ok(batch)
    })?;

    record_batches_to_pyarrow_table(py, &[batch])
}

#[pyfunction]
#[pyo3(signature = (
    path,
    row_tag=None,
    num_chunks=2,
    field_mapping=None,
    drop_fields=None,
    filter=None,
    field_types=None,
    dictionary_columns=None,
    use_mmap=false,
    schema=None,
    auto_dict=false,
    prefault=false
))]
#[allow(clippy::too_many_arguments)]
fn read_to_columnar_multi(
    py: Python<'_>,
    path: String,
    row_tag: Option<String>,
    num_chunks: usize,
    field_mapping: Option<HashMap<String, String>>,
    drop_fields: Option<Vec<String>>,
    filter: Option<HashMap<String, String>>,
    field_types: Option<HashMap<String, String>>,
    dictionary_columns: Option<Vec<String>>,
    use_mmap: bool,
    schema: Option<Vec<String>>,
    auto_dict: bool,
    prefault: bool,
) -> PyResult<PyObject> {
    let plan = execution_plan_from_kwargs(
        field_mapping,
        drop_fields,
        filter,
        field_types,
        dictionary_columns,
        schema,
        auto_dict,
    )?;

    let p = Path::new(&path);
    if !p.is_file() {
        return Err(PyFileNotFoundError::new_err(format!(
            "No such file or directory: {path}"
        )));
    }
    let row_tag = row_tag.unwrap_or_else(|| "Row".to_string()).into_bytes();

    let batch = py.allow_threads(|| -> PyResult<RecordBatch> {
        let input = InputBuffer::open(p, use_mmap, prefault).map_err(|e| {
            PyIOError::new_err(format!("Cannot open {}: {}", path, e))
        })?;
        let bytes = input.as_slice();
        let splitter = CrystalXmlSplitter::with_row_tag(&row_tag);
        let split_points = splitter.find_split_points(bytes, num_chunks);

        let mut ranges: Vec<std::ops::Range<usize>> = Vec::new();
        if split_points.len() < 2 {
            ranges.push(0..bytes.len());
        } else {
            for w in split_points.windows(2) {
                let (start, end) = (w[0], w[1]);
                if start < end {
                    ranges.push(start..end);
                }
            }
        }

        let mut engines: Vec<TableBuilder> = Vec::with_capacity(ranges.len());
        for range in &ranges {
            let est = if range.is_empty() {
                64
            } else {
                (range.len() / 512).max(64)
            };
            let mut engine = TableBuilder::with_plan(est, plan.clone());
            let decoder = CrystalXmlDecoder::with_row_tag(&row_tag);
            decoder
                .validate(&bytes[range.clone()])
                .map_err(py_err_from_rypipe)?;
            decoder
                .parse_chunk(&bytes[range.clone()], &mut engine)
                .map_err(py_err_from_rypipe)?;
            engines.push(engine);
        }

        let mut merged = TableBuilder::with_plan(engines.len().max(64) * 512, plan.clone());
        for engine in engines {
            merged.extend(engine).map_err(py_err_from_rypipe)?;
        }
        let mut batch = merged.finish().map_err(py_err_from_rypipe)?;
        if let Some(ref filter) = plan.filter {
            batch = apply_compare_filter(batch, filter).map_err(py_err_from_rypipe)?;
        }
        Ok(batch)
    })?;

    record_batches_to_pyarrow_table(py, &[batch])
}

#[pyfunction]
#[pyo3(signature = (
    path,
    row_tag=None,
    num_chunks=4,
    field_mapping=None,
    drop_fields=None,
    filter=None,
    field_types=None,
    dictionary_columns=None,
    use_mmap=false,
    schema=None,
    auto_dict=false,
    prefault=false
))]
#[allow(clippy::too_many_arguments)]
fn read_to_columnar_par(
    py: Python<'_>,
    path: String,
    row_tag: Option<String>,
    num_chunks: usize,
    field_mapping: Option<HashMap<String, String>>,
    drop_fields: Option<Vec<String>>,
    filter: Option<HashMap<String, String>>,
    field_types: Option<HashMap<String, String>>,
    dictionary_columns: Option<Vec<String>>,
    use_mmap: bool,
    schema: Option<Vec<String>>,
    auto_dict: bool,
    prefault: bool,
) -> PyResult<PyObject> {
    let plan = execution_plan_from_kwargs(
        field_mapping,
        drop_fields,
        filter,
        field_types,
        dictionary_columns,
        schema,
        auto_dict,
    )?;

    let p = Path::new(&path);
    if !p.is_file() {
        return Err(PyFileNotFoundError::new_err(format!(
            "No such file or directory: {path}"
        )));
    }
    let row_tag = row_tag.unwrap_or_else(|| "Row".to_string()).into_bytes();

    let batches = py.allow_threads(|| -> PyResult<Vec<RecordBatch>> {
        let input = InputBuffer::open(p, use_mmap, prefault).map_err(|e| {
            PyIOError::new_err(format!("Cannot open {}: {}", path, e))
        })?;
        let bytes = input.as_slice();
        let splitter = CrystalXmlSplitter::with_row_tag(&row_tag);
        let decoder = CrystalXmlDecoder::with_row_tag(&row_tag);
        ParallelExecutor::parse(bytes, &splitter, decoder, plan, num_chunks)
            .map_err(py_err_from_rypipe)
    })?;

    record_batches_to_pyarrow_table(py, &batches)
}

#[pyfunction]
#[pyo3(signature = (
    path,
    memory,
    row_tag=None,
    field_mapping=None,
    drop_fields=None,
    filter=None,
    field_types=None,
    dictionary_columns=None,
    schema=None,
    auto_dict=false,
    prefault=false
))]
#[allow(clippy::too_many_arguments)]
fn read_to_columnar_bounded(
    py: Python<'_>,
    path: String,
    memory: usize,
    row_tag: Option<String>,
    field_mapping: Option<HashMap<String, String>>,
    drop_fields: Option<Vec<String>>,
    filter: Option<HashMap<String, String>>,
    field_types: Option<HashMap<String, String>>,
    dictionary_columns: Option<Vec<String>>,
    schema: Option<Vec<String>>,
    auto_dict: bool,
    prefault: bool,
) -> PyResult<PyObject> {
    let plan = execution_plan_from_kwargs(
        field_mapping,
        drop_fields,
        filter,
        field_types,
        dictionary_columns,
        schema,
        auto_dict,
    )?;

    let p = Path::new(&path);
    if !p.is_file() {
        return Err(PyFileNotFoundError::new_err(format!(
            "No such file or directory: {path}"
        )));
    }
    let row_tag = row_tag.unwrap_or_else(|| "Row".to_string()).into_bytes();

    let batches = py.allow_threads(|| -> PyResult<Vec<RecordBatch>> {
        let splitter = CrystalXmlSplitter::with_row_tag(&row_tag);
        let decoder = CrystalXmlDecoder::with_row_tag(&row_tag);
        let budget = MemoryBudget::new(memory);
        rypipe_core::bounded::BoundedExecutor::new(budget)
            .run(p, &splitter, decoder, plan, prefault)
            .map_err(py_err_from_rypipe)
    })?;

    record_batches_to_pyarrow_table(py, &batches)
}

/// Generic multi-format/multi-mode read entry point.
///
/// `format` selects the parser (e.g. "xml"). `format_options` is a dict of
/// parser-specific options; for XML the only option is `row_tag`.
/// `mode` is one of "sync", "multi", "par", or "stream".
#[pyfunction]
#[pyo3(signature = (
    path,
    format,
    format_options=None,
    mode="par",
    num_chunks=4,
    memory=64_000_000,
    field_mapping=None,
    drop_fields=None,
    filter=None,
    field_types=None,
    dictionary_columns=None,
    use_mmap=false,
    schema=None,
    auto_dict=false,
    prefault=false
))]
#[allow(clippy::too_many_arguments)]
fn read(
    py: Python<'_>,
    path: String,
    format: String,
    format_options: Option<Bound<'_, PyDict>>,
    mode: &str,
    num_chunks: usize,
    memory: usize,
    field_mapping: Option<HashMap<String, String>>,
    drop_fields: Option<Vec<String>>,
    filter: Option<HashMap<String, String>>,
    field_types: Option<HashMap<String, String>>,
    dictionary_columns: Option<Vec<String>>,
    use_mmap: bool,
    schema: Option<Vec<String>>,
    auto_dict: bool,
    prefault: bool,
) -> PyResult<PyObject> {
    let plan = execution_plan_from_kwargs(
        field_mapping,
        drop_fields,
        filter,
        field_types,
        dictionary_columns,
        schema,
        auto_dict,
    )?;

    let p = Path::new(&path);
    if !p.is_file() {
        return Err(PyFileNotFoundError::new_err(format!(
            "No such file or directory: {path}"
        )));
    }

    match format.as_str() {
        "xml" => read_xml(
            py, p, format_options, mode, num_chunks, memory, plan, use_mmap, prefault,
        ),
        other => Err(PlanError::new_err(format!(
            "unsupported format {other:?}; supported formats: 'xml'"
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
fn read_xml(
    py: Python<'_>,
    path: &Path,
    format_options: Option<Bound<'_, PyDict>>,
    mode: &str,
    num_chunks: usize,
    memory: usize,
    plan: rypipe_core::ExecutionPlan,
    use_mmap: bool,
    prefault: bool,
) -> PyResult<PyObject> {
    let row_tag = parse_row_tag(format_options)?.unwrap_or_else(|| b"Row".to_vec());

    let batches = py.allow_threads(|| -> PyResult<Vec<RecordBatch>> {
        let splitter = CrystalXmlSplitter::with_row_tag(&row_tag);
        let decoder = CrystalXmlDecoder::with_row_tag(&row_tag);

        match mode {
            "sync" => {
                let input = InputBuffer::open(path, use_mmap, prefault).map_err(|e| {
                    PyIOError::new_err(format!("Cannot open {}: {}", path.display(), e))
                })?;
                let bytes = input.as_slice();
                let mut engine = TableBuilder::with_plan((bytes.len() / 512).max(64), plan.clone());
                decoder.validate(bytes).map_err(py_err_from_rypipe)?;
                decoder.parse_chunk(bytes, &mut engine).map_err(py_err_from_rypipe)?;
                let mut batch = engine.finish().map_err(py_err_from_rypipe)?;
                if let Some(ref filter) = plan.filter {
                    batch = apply_compare_filter(batch, filter).map_err(py_err_from_rypipe)?;
                }
                Ok(vec![batch])
            }
            "multi" => {
                let input = InputBuffer::open(path, use_mmap, prefault).map_err(|e| {
                    PyIOError::new_err(format!("Cannot open {}: {}", path.display(), e))
                })?;
                let bytes = input.as_slice();
                let split_points = splitter.find_split_points(bytes, num_chunks);
                let ranges = rypipe_core::decoder::split_points_to_ranges(&split_points, bytes.len());
                let mut engines: Vec<TableBuilder> = Vec::with_capacity(ranges.len());
                for range in &ranges {
                    let est = if range.is_empty() { 64 } else { (range.len() / 512).max(64) };
                    let mut engine = TableBuilder::with_plan(est, plan.clone());
                    decoder
                        .validate(&bytes[range.clone()])
                        .map_err(py_err_from_rypipe)?;
                    decoder
                        .parse_chunk(&bytes[range.clone()], &mut engine)
                        .map_err(py_err_from_rypipe)?;
                    engines.push(engine);
                }
                let mut merged = TableBuilder::with_plan(engines.len().max(64) * 512, plan.clone());
                for engine in engines {
                    merged.extend(engine).map_err(py_err_from_rypipe)?;
                }
                let mut batch = merged.finish().map_err(py_err_from_rypipe)?;
                if let Some(ref filter) = plan.filter {
                    batch = apply_compare_filter(batch, filter).map_err(py_err_from_rypipe)?;
                }
                Ok(vec![batch])
            }
            "par" | "parallel" => {
                let input = InputBuffer::open(path, use_mmap, prefault).map_err(|e| {
                    PyIOError::new_err(format!("Cannot open {}: {}", path.display(), e))
                })?;
                let bytes = input.as_slice();
                ParallelExecutor::parse(bytes, &splitter, decoder, plan, num_chunks)
                    .map_err(py_err_from_rypipe)
            }
            "stream" | "bounded" => {
                let budget = MemoryBudget::new(memory);
                rypipe_core::bounded::BoundedExecutor::new(budget)
                    .run(path, &splitter, decoder, plan, prefault)
                    .map_err(py_err_from_rypipe)
            }
            other => Err(PlanError::new_err(format!(
                "unsupported read mode {other:?}; use 'sync', 'multi', 'par', or 'stream'"
            ))),
        }
    })?;

    record_batches_to_pyarrow_table(py, &batches)
}

fn parse_row_tag(format_options: Option<Bound<'_, PyDict>>) -> PyResult<Option<Vec<u8>>> {
    let Some(opts) = format_options else {
        return Ok(None);
    };
    if let Some(tag) = opts.get_item("row_tag")? {
        let tag: String = tag.extract()?;
        return Ok(Some(tag.into_bytes()));
    }
    Ok(None)
}

#[pymodule]
fn _rypipe(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("XmlError", m.py().get_type::<XmlError>())?;
    m.add("PlanError", m.py().get_type::<PlanError>())?;
    m.add("MergeError", m.py().get_type::<MergeError>())?;

    m.add_function(wrap_pyfunction!(read_to_columnar, m)?)?;
    m.add_function(wrap_pyfunction!(read_to_columnar_multi, m)?)?;
    m.add_function(wrap_pyfunction!(read_to_columnar_par, m)?)?;
    m.add_function(wrap_pyfunction!(read_to_columnar_bounded, m)?)?;
    m.add_function(wrap_pyfunction!(read, m)?)?;

    Ok(())
}
