use std::borrow::Cow;

use arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use rypipe_core::{ExecutionPlan, FieldType, FilterPredicate, Pipeline, RecordParser, Splitter};
use std::collections::HashMap;

/// Your adapter struct
pub struct {{project-name}}Adapter;

impl RecordParser for {{project-name}}Adapter {
    fn validate(&self, bytes: &[u8]) -> rypipe_core::Result<()> {
        simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn rypipe_core::ColumnarSink) -> rypipe_core::Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            sink.begin_row();
            // Parse your format here
            // Example: sink.put_field("name", Value::Str(Cow::Borrowed("value")));
            sink.end_row();
        }
        Ok(())
    }
}

/// Your splitter struct
pub struct {{project-name}}Splitter;

impl Splitter for {{project-name}}Splitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        if from >= bytes.len() {
            return None;
        }
        // Implement your record boundary detection
        None
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        64
    }
}

// ---------------------------------------------------------------------------
// Python bindings — builds the ExecutionPlan manually from the9 kwargs.
// Fused pipeline stages (rename, drop, cast, filter) forward their options
// as keyword arguments. All9 must be present or stages silently fall back
// to Python (10-50x slower).
// ---------------------------------------------------------------------------

#[pyfunction]
#[pyo3(signature = (path, field_mapping=None, drop_fields=None, filter=None, field_types=None, schema=None, auto_dict=false, use_mmap=false, prefault=false))]
fn read_{{project-name}}(
    path: String,
    field_mapping: Option<HashMap<String, String>>,
    drop_fields: Option<Vec<String>>,
    filter: Option<HashMap<String, String>>,
    field_types: Option<HashMap<String, String>>,
    schema: Option<Vec<String>>,
    auto_dict: bool,
    use_mmap: bool,
    prefault: bool,
) -> PyResult<Py<PyAny>> {
    let mut plan = ExecutionPlan::new();
    if let Some(map) = field_mapping {
        plan.field_map = map.into_iter().collect();
    }
    if let Some(drop) = drop_fields {
        plan.drop_fields = drop.into_iter().collect();
    }
    if let Some(s) = schema {
        plan.schema_order = s;
    }
    plan.auto_dict = auto_dict;
    if let Some(ft) = field_types {
        for (name, type_str) in ft {
            let ft = type_str.parse::<FieldType>().map_err(|_| {
                PyValueError::new_err(format!("unknown field type '{type_str}' for '{name}'"))
            })?;
            plan.field_types.insert(name, ft);
        }
    }
    if let Some(f) = filter {
        let field = f
            .get("field")
            .ok_or_else(|| PyValueError::new_err("filter must include 'field' key"))?
            .to_owned();
        let op = f
            .get("op")
            .ok_or_else(|| PyValueError::new_err("filter must include 'op' key"))?
            .to_owned();
        let value = f
            .get("value")
            .ok_or_else(|| PyValueError::new_err("filter must include 'value' key"))?
            .to_owned();
        plan.filter = Some(match op.as_str() {
            "==" | "eq" => FilterPredicate::Equal { field, value },
            "!=" | "ne" => FilterPredicate::NotEqual { field, value },
            other => return Err(PyValueError::new_err(format!("unsupported filter op {other:?}"))),
        });
    }

    let batch = Pipeline::new({{project-name}}Splitter, {{project-name}}Adapter)
        .with_plan(plan)
        .read_path(&path, use_mmap, prefault)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    Python::with_gil(|py| {
        let pa = PyModule::import(py, "pyarrow")?;
        let rb = batch.to_pyarrow(py)?;
        let table = pa
            .getattr("Table")?
            .call_method1("from_batches", (vec![rb],))?;
        Ok(table.into())
    })
}

/// Python module entry point
#[pymodule]
fn _{{project-name}}(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_{{project-name}}, m)?)?;
    Ok(())
}
