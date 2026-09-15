use std::borrow::Cow;
use std::collections::HashMap;

use pyo3::prelude::*;
use rypipe_core::{ColumnarSink, Pipeline, RecordParser, Splitter, Value};
use rypipe_python::{execution_plan_from_kwargs, py_err_from_rypipe, record_batches_to_pyarrow_table};

#[derive(Clone)]
pub struct AdapterParser;

impl RecordParser for AdapterParser {
    fn validate(&self, bytes: &[u8]) -> rypipe_core::Result<()> {
        simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> rypipe_core::Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Parser(e.to_string()))?;
        for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
            let Some((key, value)) = line.split_once('=') else { continue };
            sink.begin_row();
            if sink.wants("key") {
                sink.put_field("key", Value::Str(Cow::Borrowed(key.trim())));
            }
            if sink.wants("value") {
                sink.put_field("value", Value::Str(Cow::Borrowed(value.trim())));
            }
            sink.end_row();
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct LineSplitter;

impl Splitter for LineSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        let rest = bytes.get(from..)?;
        memchr::memchr(b'\n', rest).map(|i| from + i + 1)
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        (sample.len() / memchr::memchr_iter(b'\n', sample).count().max(1)).max(1)
    }
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (path, field_mapping=None, drop_fields=None, filter=None,
    field_types=None, dictionary_columns=None, schema=None, auto_dict=false,
    auto_dict_threshold=None, auto_dict_max_size=None, strict_types=false,
    max_split_chunks=None, observer=None, use_mmap=false, prefault=false))]
fn read_{{crate_name}}(
    py: Python<'_>,
    path: String,
    field_mapping: Option<HashMap<String, String>>,
    drop_fields: Option<Vec<String>>,
    filter: Option<Bound<'_, PyAny>>,
    field_types: Option<HashMap<String, String>>,
    dictionary_columns: Option<Vec<String>>,
    schema: Option<Vec<String>>,
    auto_dict: bool,
    auto_dict_threshold: Option<f64>,
    auto_dict_max_size: Option<usize>,
    strict_types: bool,
    max_split_chunks: Option<usize>,
    observer: Option<Bound<'_, PyAny>>,
    use_mmap: bool,
    prefault: bool,
) -> PyResult<Py<PyAny>> {
    let plan = execution_plan_from_kwargs(
        field_mapping, drop_fields, filter.as_ref(), field_types, dictionary_columns,
        schema, auto_dict, auto_dict_threshold, auto_dict_max_size, strict_types,
        max_split_chunks, observer.as_ref(),
    )?;
    let batch = py.detach(|| {
        Pipeline::new(LineSplitter, AdapterParser)
            .with_plan(plan)
            .read_path(&path, use_mmap, prefault)
    }).map_err(py_err_from_rypipe)?;
    record_batches_to_pyarrow_table(py, &[batch]).map(|v| v.unbind())
}

#[pymodule]
fn _{{crate_name}}(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_{{crate_name}}, m)?)?;
    Ok(())
}
