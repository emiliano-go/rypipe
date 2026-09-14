use std::borrow::Cow;

use arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use rypipe_core::{ColumnarSink, ExecutionPlan, FieldType, FilterPredicate, Pipeline, RecordParser, Result, Splitter, Value};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Splitter: INI lines are newline-delimited; each line is a potential boundary.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct IniSplitter;

impl Splitter for IniSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        memchr::memchr(b'\n', &bytes[from..]).map(|r| from + r + 1)
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
        (sample.len() / n).max(1)
    }
}

// ---------------------------------------------------------------------------
// RecordParser: stateful parser that tracks [section] headers and emits
// section/key/value rows for each key=value or key:value line.
// Comments (; or #) and blank lines are skipped.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct IniParser;

impl RecordParser for IniParser {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes)
            .map_err(rypipe_core::Error::Utf8)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

        let mut current_section = String::new();

        for line in text.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            // [section] header
            if trimmed.starts_with('[') {
                if let Some(end) = trimmed.find(']') {
                    current_section = trimmed[1..end].to_string();
                }
                continue;
            }

            // Comment lines
            if trimmed.starts_with(';') || trimmed.starts_with('#') {
                continue;
            }

            // key = value or key: value
            if let Some((key, value)) = parse_kv(trimmed) {
                sink.begin_row();
                if sink.wants("section") {
                    sink.put_field("section", Value::Str(Cow::Borrowed(&current_section)));
                }
                if sink.wants(key) {
                    sink.put_field(key, Value::Str(Cow::Borrowed(value)));
                }
                sink.end_row();
            }
        }

        Ok(())
    }
}

/// Parse a trimmed INI line into (key, value), handling `=` and `:` separators.
fn parse_kv(line: &str) -> Option<(&str, &str)> {
    if let Some(pos) = line.find('=') {
        let key = line[..pos].trim_end();
        let value = line[pos + 1..].trim_start();
        return Some((key, value));
    }
    if let Some(pos) = line.find(':') {
        let key = line[..pos].trim_end();
        let value = line[pos + 1..].trim_start();
        return Some((key, value));
    }
    None
}

// ---------------------------------------------------------------------------
// PyO3 bindings
// ---------------------------------------------------------------------------

#[pyfunction]
#[pyo3(signature = (path, field_mapping=None, drop_fields=None, filter=None, field_types=None, schema=None, auto_dict=false, use_mmap=false, prefault=false))]
fn read_ini(
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

    let batch = Pipeline::new(IniSplitter, IniParser)
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

#[pymodule]
fn _rypipe_ini(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_ini, m)?)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rypipe_core::{ExecutionPlan, TableBuilder};
    use std::sync::Arc;

    const SAMPLE: &[u8] = b"[server]\nhost = localhost\nport = 8080\ndebug = true\n\n[database]\nhost = db.example.com\nport = 5432\nname = mydb\n";

    #[test]
    fn splitter_finds_line_starts() {
        let s = IniSplitter;
        let first = s.next_record_start(SAMPLE, 0).unwrap();
        assert!(first > 0);
        assert_eq!(s.next_record_start(SAMPLE, SAMPLE.len()), None);
    }

    #[test]
    fn parser_emits_all_rows() {
        let plan: Arc<ExecutionPlan> = Arc::new(ExecutionPlan::new());
        let mut builder = TableBuilder::with_plan(1024, plan);
        IniParser.validate(SAMPLE).unwrap();
        IniParser.parse_chunk(SAMPLE, &mut builder).unwrap();
        let batch = builder.finish().unwrap();
        // server: host, port, debug = 3 rows; database: host, port, name = 3 rows
        assert_eq!(batch.num_rows(), 6);
    }

    #[test]
    fn parser_extracts_sections() {
        let plan: Arc<ExecutionPlan> = Arc::new(ExecutionPlan::new());
        let mut builder = TableBuilder::with_plan(1024, plan);
        IniParser.validate(SAMPLE).unwrap();
        IniParser.parse_chunk(SAMPLE, &mut builder).unwrap();
        let batch = builder.finish().unwrap();
        assert!(batch.schema().field_with_name("section").is_ok());
        assert!(batch.schema().field_with_name("host").is_ok());
        assert!(batch.schema().field_with_name("port").is_ok());
    }

    #[test]
    fn parser_skips_comments() {
        let input = b"[sec]\n; comment\n# also comment\nkey = val\n";
        let plan: Arc<ExecutionPlan> = Arc::new(ExecutionPlan::new());
        let mut builder = TableBuilder::with_plan(1024, plan);
        IniParser.validate(input).unwrap();
        IniParser.parse_chunk(input, &mut builder).unwrap();
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn parser_handles_colon_separator() {
        let input = b"[sec]\nkey: value\n";
        let plan: Arc<ExecutionPlan> = Arc::new(ExecutionPlan::new());
        let mut builder = TableBuilder::with_plan(1024, plan);
        IniParser.validate(input).unwrap();
        IniParser.parse_chunk(input, &mut builder).unwrap();
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }
}
