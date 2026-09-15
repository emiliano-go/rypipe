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
// Splitter: Properties files are newline-delimited.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct PropertiesSplitter;

impl Splitter for PropertiesSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        if from >= bytes.len() {
            return None;
        }
        memchr::memchr(b'\n', &bytes[from..]).map(|r| from + r + 1)
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
        (sample.len() / n).max(1)
    }
}

/// Plain properties records; Java escapes and continuations are unsupported.
#[derive(Clone, Default)]
pub struct PropertiesParser;

fn parse_kv(line: &str) -> (&str, &str) {
    let Some(pos) = line
        .char_indices()
        .find_map(|(i, c)| (c == '=' || c == ':' || c.is_ascii_whitespace()).then_some(i))
    else {
        return (line, "");
    };
    let key = &line[..pos];
    let mut value = line[pos..].trim_start_matches(|c: char| c.is_ascii_whitespace());
    if value.starts_with('=') || value.starts_with(':') {
        value = value[1..].trim_start_matches(|c: char| c.is_ascii_whitespace());
    }
    (key, value)
}

impl RecordParser for PropertiesParser {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text =
            std::str::from_utf8(bytes).map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

        for line in text.lines() {
            let trimmed = line.trim_start_matches(|c: char| c.is_ascii_whitespace());
            if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with('!') {
                continue;
            }
            if line.contains('\\') {
                return Err(rypipe_core::Error::Parser(
                    "properties escapes and continuations are unsupported".into(),
                ));
            }
            let (key, value) = parse_kv(trimmed);
            sink.begin_row();
            if sink.wants("key") {
                sink.put_field("key", Value::Str(Cow::Borrowed(key)));
            }
            if sink.wants("value") {
                sink.put_field("value", Value::Str(Cow::Borrowed(value)));
            }
            sink.end_row();
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// PyO3 bindings
// ---------------------------------------------------------------------------

#[cfg(feature = "python")]
#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (path, field_mapping=None, drop_fields=None, filter=None, field_types=None, dictionary_columns=None, schema=None, auto_dict=false, auto_dict_threshold=None, auto_dict_max_size=None, strict_types=false, max_split_chunks=None, observer=None, use_mmap=false, prefault=false))]
fn read_properties(
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
        Pipeline::new(PropertiesSplitter, PropertiesParser)
            .with_plan(plan)
            .read_path(&path, use_mmap, prefault)
            .map_err(rypipe_python::py_err_from_rypipe)
    })?;
    record_batches_to_pyarrow_table(py, &[batch]).map(|v| v.unbind())
}

#[cfg(feature = "python")]
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

    const SAMPLE: &[u8] =
        b"# comment\nname = Alice\nage: 30\ndb.host localhost\nmessage = first part second part\n";

    #[test]
    fn splitter_finds_line_starts() {
        let s = PropertiesSplitter;
        let first = s.next_record_start(SAMPLE, 0).unwrap();
        assert!(first > 0);
        assert_eq!(s.next_record_start(SAMPLE, SAMPLE.len()), None);
    }

    #[test]
    fn splitter_rejects_out_of_range_start() {
        assert_eq!(
            PropertiesSplitter.next_record_start(SAMPLE, usize::MAX),
            None
        );
    }

    #[test]
    fn parallel_matches_single_for_comments_and_crlf() {
        let input = b"# skip\r\nfirst = alpha\r\n! skip\r\nsecond: beta\r\nthird gamma\r\n";
        let single = Pipeline::new(PropertiesSplitter, PropertiesParser)
            .read_bytes(input)
            .unwrap();
        let parallel = Pipeline::new(PropertiesSplitter, PropertiesParser)
            .read_bytes_par(input, 4)
            .unwrap();
        assert_eq!(single.num_rows(), 3);
        assert_eq!(
            parallel.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            3
        );
        let rendered = format!("{parallel:?}");
        for value in ["alpha", "beta", "gamma"] {
            assert!(rendered.contains(value));
        }
    }

    #[test]
    fn parser_emits_all_rows() {
        let plan: Arc<ExecutionPlan> = Arc::new(ExecutionPlan::new());
        let mut builder = TableBuilder::with_plan(1024, plan);
        PropertiesParser.validate(SAMPLE).unwrap();
        PropertiesParser.parse_chunk(SAMPLE, &mut builder).unwrap();
        let batch = builder.finish().unwrap();
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
    fn parser_rejects_unsupported_continuation() {
        let input = b"key = first part \\\n  second part\n";
        let plan: Arc<ExecutionPlan> = Arc::new(ExecutionPlan::new());
        let mut builder = TableBuilder::with_plan(1024, plan);
        PropertiesParser.validate(input).unwrap();
        assert!(matches!(
            PropertiesParser.parse_chunk(input, &mut builder),
            Err(rypipe_core::Error::Parser(message)) if message.contains("continuations")
        ));
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
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "");
    }
}
