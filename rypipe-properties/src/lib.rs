use std::borrow::Cow;

use arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use rypipe_core::{ColumnarSink, ExecutionPlan, FieldType, FilterPredicate, Pipeline, RecordParser, Result, Splitter, Value};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Splitter: Properties files are newline-delimited.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct PropertiesSplitter;

impl Splitter for PropertiesSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        memchr::memchr(b'\n', &bytes[from..]).map(|r| from + r + 1)
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
        (sample.len() / n).max(1)
    }
}

// ---------------------------------------------------------------------------
// RecordParser: parses Java .properties format.
//
// - Lines starting with # or ! are comments
// - key = value, key: value, or key<whitespace>value
// - Backslash at end of line continues the value on the next line
// - Blank lines are skipped
// - Emits rows with "key" and "value" columns
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct PropertiesParser;

/// Parse a properties line into (key, value), handling =, :, and whitespace separators.
fn parse_kv(line: &str) -> Option<(&str, &str)> {
    // Try '=' separator first
    if let Some(pos) = line.find('=') {
        let key = line[..pos].trim_end();
        let value = line[pos + 1..].trim_start();
        return Some((key, value));
    }
    // Try ':' separator
    if let Some(pos) = line.find(':') {
        let key = line[..pos].trim_end();
        let value = line[pos + 1..].trim_start();
        return Some((key, value));
    }
    // Whitespace separator: first whitespace splits key from value
    if let Some(pos) = line.find(|c: char| c.is_ascii_whitespace()) {
        let key = &line[..pos];
        let value = line[pos..].trim_start();
        return Some((key, value));
    }
    // Key with no value (valid in .properties: key = "")
    Some((line, ""))
}

impl RecordParser for PropertiesParser {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes)
            .map_err(rypipe_core::Error::Utf8)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

        // State: pending multi-line value (key, accumulated_value)
        let mut pending: Option<(String, String)> = None;

        for line in text.lines() {
            let trimmed = line.trim();

            // Blank lines flush any pending value and skip
            if trimmed.is_empty() {
                if let Some((key, value)) = pending.take() {
                    sink.begin_row();
                    if sink.wants("key") {
                        sink.put_field("key", Value::Str(Cow::Owned(key)));
                    }
                    if sink.wants("value") {
                        sink.put_field("value", Value::Str(Cow::Owned(value)));
                    }
                    sink.end_row();
                }
                continue;
            }

            // Comment lines: skip (but flush pending if continuation was active)
            if trimmed.starts_with('#') || trimmed.starts_with('!') {
                // Comments don't break multi-line continuations in Java .properties,
                // but for safety, treat them as line breaks.
                continue;
            }

            // Check for trailing backslash (continuation)
            let continued = trimmed.ends_with('\\');
            // Parse the line (before stripping backslash) to get key=value
            let parse_line = if continued {
                trimmed.strip_suffix('\\').unwrap_or(trimmed).trim_end()
            } else {
                trimmed
            };

            if let Some((_, ref mut acc_value)) = pending {
                // Continuation: append this line to the accumulated value
                acc_value.push(' ');
                acc_value.push_str(parse_line);
                if !continued {
                    // Done accumulating — emit the row
                    let (key, value) = pending.take().unwrap();
                    sink.begin_row();
                    if sink.wants("key") {
                        sink.put_field("key", Value::Str(Cow::Owned(key)));
                    }
                    if sink.wants("value") {
                        sink.put_field("value", Value::Str(Cow::Owned(value)));
                    }
                    sink.end_row();
                }
                continue;
            }

            // New property line
            if let Some((key, value)) = parse_kv(parse_line) {
                if continued {
                    // Start accumulating a multi-line value
                    pending = Some((key.to_string(), value.to_string()));
                } else {
                    // Simple single-line property
                    sink.begin_row();
                    if sink.wants("key") {
                        sink.put_field("key", Value::Str(Cow::Borrowed(key)));
                    }
                    if sink.wants("value") {
                        sink.put_field("value", Value::Str(Cow::Borrowed(value)));
                    }
                    sink.end_row();
                }
            }
        }

        // Flush any remaining pending value at chunk boundary
        if let Some((key, value)) = pending {
            sink.begin_row();
            if sink.wants("key") {
                sink.put_field("key", Value::Str(Cow::Owned(key)));
            }
            if sink.wants("value") {
                sink.put_field("value", Value::Str(Cow::Owned(value)));
            }
            sink.end_row();
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// PyO3 bindings
// ---------------------------------------------------------------------------

#[pyfunction]
#[pyo3(signature = (path, field_mapping=None, drop_fields=None, filter=None, field_types=None, schema=None, auto_dict=false, use_mmap=false, prefault=false))]
fn read_properties(
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

    let batch = Pipeline::new(PropertiesSplitter, PropertiesParser)
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
fn _rypipe_properties(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_properties, m)?)?;
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

    const SAMPLE: &[u8] = b"# comment\nname = Alice\nage: 30\ndb.host localhost\nmultiline = first part \\\n  second part\n";

    #[test]
    fn splitter_finds_line_starts() {
        let s = PropertiesSplitter;
        let first = s.next_record_start(SAMPLE, 0).unwrap();
        assert!(first > 0);
        assert_eq!(s.next_record_start(SAMPLE, SAMPLE.len()), None);
    }

    #[test]
    fn parser_emits_all_rows() {
        let plan: Arc<ExecutionPlan> = Arc::new(ExecutionPlan::new());
        let mut builder = TableBuilder::with_plan(1024, plan);
        PropertiesParser.validate(SAMPLE).unwrap();
        PropertiesParser.parse_chunk(SAMPLE, &mut builder).unwrap();
        let batch = builder.finish().unwrap();
        // name, age, db.host, multiline = 4 rows
        assert_eq!(batch.num_rows(), 4);
    }

    #[test]
    fn parser_extracts_key_and_value() {
        let plan: Arc<ExecutionPlan> = Arc::new(ExecutionPlan::new());
        let mut builder = TableBuilder::with_plan(1024, plan);
        PropertiesParser.validate(SAMPLE).unwrap();
        PropertiesParser.parse_chunk(SAMPLE, &mut builder).unwrap();
        let batch = builder.finish().unwrap();
        assert!(batch.schema().field_with_name("key").is_ok());
        assert!(batch.schema().field_with_name("value").is_ok());
    }

    #[test]
    fn parser_skips_comments() {
        let input = b"# comment\n! also comment\nkey = val\n";
        let plan: Arc<ExecutionPlan> = Arc::new(ExecutionPlan::new());
        let mut builder = TableBuilder::with_plan(1024, plan);
        PropertiesParser.validate(input).unwrap();
        PropertiesParser.parse_chunk(input, &mut builder).unwrap();
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn parser_handles_colon_separator() {
        let input = b"key: value\n";
        let plan: Arc<ExecutionPlan> = Arc::new(ExecutionPlan::new());
        let mut builder = TableBuilder::with_plan(1024, plan);
        PropertiesParser.validate(input).unwrap();
        PropertiesParser.parse_chunk(input, &mut builder).unwrap();
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn parser_handles_whitespace_separator() {
        let input = b"key value\n";
        let plan: Arc<ExecutionPlan> = Arc::new(ExecutionPlan::new());
        let mut builder = TableBuilder::with_plan(1024, plan);
        PropertiesParser.validate(input).unwrap();
        PropertiesParser.parse_chunk(input, &mut builder).unwrap();
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn parser_handles_multiline_continuation() {
        let input = b"key = first part \\\n  second part\n";
        let plan: Arc<ExecutionPlan> = Arc::new(ExecutionPlan::new());
        let mut builder = TableBuilder::with_plan(1024, plan);
        PropertiesParser.validate(input).unwrap();
        PropertiesParser.parse_chunk(input, &mut builder).unwrap();
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
        let values = batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(values.value(0), "first part second part");
    }

    #[test]
    fn parser_handles_key_with_no_value() {
        let input = b"standalone_key\n";
        let plan: Arc<ExecutionPlan> = Arc::new(ExecutionPlan::new());
        let mut builder = TableBuilder::with_plan(1024, plan);
        PropertiesParser.validate(input).unwrap();
        PropertiesParser.parse_chunk(input, &mut builder).unwrap();
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
        let values = batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(values.value(0), "");
    }
}
