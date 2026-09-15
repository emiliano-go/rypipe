use std::borrow::Cow;

#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::PyModule;
#[cfg(any(feature = "python", test))]
use rypipe_core::Pipeline;
use rypipe_core::{ColumnarSink, RecordParser, Result, Splitter, Value};
#[cfg(feature = "python")]
use rypipe_python::{execution_plan_from_kwargs, record_batches_to_pyarrow_table};
#[cfg(feature = "python")]
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Splitter: INI lines are newline-delimited; each line is a potential boundary.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct IniSplitter;

impl Splitter for IniSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        if from >= bytes.len() {
            return None;
        }
        let mut start = bytes[..from]
            .iter()
            .rposition(|&byte| byte == b'\n')
            .map_or(0, |position| position + 1);
        if start < from {
            start = memchr::memchr(b'\n', &bytes[from..])
                .map_or(bytes.len(), |position| from + position + 1);
        }
        while start < bytes.len() {
            let end = memchr::memchr(b'\n', &bytes[start..])
                .map_or(bytes.len(), |position| start + position);
            if start != from
                && bytes[start..end]
                    .iter()
                    .copied()
                    .skip_while(|byte| byte.is_ascii_whitespace())
                    .next()
                    == Some(b'[')
            {
                return Some(start);
            }
            start = end.saturating_add(1);
        }
        None
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
        simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text =
            std::str::from_utf8(bytes).map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

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

#[cfg(feature = "python")]
#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (path, field_mapping=None, drop_fields=None, filter=None, field_types=None, dictionary_columns=None, schema=None, auto_dict=false, auto_dict_threshold=None, auto_dict_max_size=None, strict_types=false, max_split_chunks=None, observer=None, use_mmap=false, prefault=false))]
fn read_ini(
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
        field_mapping,
        drop_fields,
        filter.as_ref(),
        field_types,
        dictionary_columns,
        schema,
        auto_dict,
        auto_dict_threshold,
        auto_dict_max_size,
        strict_types,
        max_split_chunks,
        observer.as_ref(),
    )?;
    let batch = py.detach(|| {
        Pipeline::new(IniSplitter, IniParser)
            .with_plan(plan)
            .read_path(&path, use_mmap, prefault)
            .map_err(rypipe_python::py_err_from_rypipe)
    })?;
    record_batches_to_pyarrow_table(py, &[batch]).map(|v| v.unbind())
}

#[cfg(feature = "python")]
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
    fn splitter_rejects_out_of_range_start() {
        assert_eq!(IniSplitter.next_record_start(SAMPLE, usize::MAX), None);
    }

    #[test]
    fn parallel_sections_keep_their_declared_section() {
        let input = b"; preface\r\n[one]\r\na = first\r\nb = second\r\n# gap\r\n[two]\r\na = third\r\nb: fourth\r\n";
        let single = Pipeline::new(IniSplitter, IniParser)
            .read_bytes(input)
            .unwrap();
        let parallel = Pipeline::new(IniSplitter, IniParser)
            .read_bytes_par(input, 4)
            .unwrap();
        assert_eq!(single.num_rows(), 4);
        assert_eq!(
            parallel.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            4
        );
        assert!(!format!("{parallel:?}").contains("\"\""));
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
