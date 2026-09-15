use std::borrow::Cow;

#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::PyModule;
#[cfg(any(feature = "python", test))]
use rypipe_core::Pipeline;
use rypipe_core::{find_next_record_boundary, ColumnarSink, RecordParser, Result, Splitter, Value};
#[cfg(feature = "python")]
use rypipe_python::{execution_plan_from_kwargs, record_batches_to_pyarrow_table};
#[cfg(feature = "python")]
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Splitter: LDIF records are separated by blank lines (\n\n).
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct LdifSplitter;

impl Splitter for LdifSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        find_next_record_boundary(bytes, from, None, &[], true)
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let n = sample
            .windows(2)
            .filter(|w| w[0] == b'\n' && w[1] == b'\n')
            .count()
            .max(1);
        (sample.len() / n).max(1)
    }
}

/// Plain LDIF values; folded, base64, and URL values are unsupported.
#[derive(Clone, Default)]
pub struct LdifParser;

impl RecordParser for LdifParser {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text =
            std::str::from_utf8(bytes).map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

        let mut in_record = false;

        for line in text.lines() {
            if line.is_empty() {
                if in_record {
                    sink.end_row();
                    in_record = false;
                }
                continue;
            }

            if line.starts_with(' ') {
                return Err(rypipe_core::Error::Parser(
                    "LDIF folded continuation lines are unsupported".into(),
                ));
            }

            if line.starts_with('#') {
                continue;
            }

            if !in_record {
                sink.begin_row();
                in_record = true;
            }

            if let Some(colon_pos) = line.find(':') {
                let key = &line[..colon_pos];
                let rest = &line[colon_pos + 1..];
                if rest.starts_with(':') {
                    return Err(rypipe_core::Error::Parser(
                        "LDIF base64 values are unsupported".into(),
                    ));
                }
                if rest.starts_with('<') {
                    return Err(rypipe_core::Error::Parser(
                        "LDIF URL values are unsupported".into(),
                    ));
                }
                let value = rest.trim_start_matches(' ');

                if sink.wants(key) {
                    sink.put_field(key, Value::Str(Cow::Borrowed(value)));
                }
            }
        }

        if in_record {
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
fn read_ldif(
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
        Pipeline::new(LdifSplitter, LdifParser)
            .with_plan(plan)
            .read_path(&path, use_mmap, prefault)
            .map_err(rypipe_python::py_err_from_rypipe)
    })?;
    record_batches_to_pyarrow_table(py, &[batch]).map(|v| v.unbind())
}

#[cfg(feature = "python")]
#[pymodule]
fn _rypipe_ldif(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_ldif, m)?)?;
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

    const SAMPLE: &[u8] = b"dN: uid=alice,ou=users\nobjectClass: inetOrgPerson\ncn: Alice Smith\n\ndn: uid=bob,ou=users\nobjectClass: inetOrgPerson\ncn: Bob Jones\n";

    #[test]
    fn splitter_finds_record_starts() {
        let s = LdifSplitter;
        // First record boundary: after "dN: ...\nobjectClass: ...\ncn: ...\n\n"
        let first_boundary = s.next_record_start(SAMPLE, 0).unwrap();
        assert!(first_boundary > 0);
        // No third record — the second is the last (no trailing blank line)
        assert_eq!(s.next_record_start(SAMPLE, first_boundary), None);
        // End of input
        assert_eq!(s.next_record_start(SAMPLE, SAMPLE.len()), None);
    }

    #[test]
    fn splitter_handles_crlf_and_out_of_range_start() {
        let input = b"dn: cn=one\r\ncn: one\r\n\r\ndn: cn=two\r\ncn: two\r\n";
        assert_eq!(LdifSplitter.next_record_start(input, 0), Some(23));
        assert_eq!(LdifSplitter.next_record_start(input, usize::MAX), None);
    }

    #[test]
    fn parallel_matches_single_for_crlf_entries() {
        let input = b"# header\r\ndn: cn=one\r\ncn: one\r\n\r\ndn: cn=two\r\ncn: two\r\n\r\ndn: cn=three\r\ncn: three\r\n";
        let single = Pipeline::new(LdifSplitter, LdifParser)
            .read_bytes(input)
            .unwrap();
        let parallel = Pipeline::new(LdifSplitter, LdifParser)
            .read_bytes_par(input, 4)
            .unwrap();
        assert_eq!(single.num_rows(), 3);
        assert_eq!(
            parallel.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            3
        );
        let rendered = format!("{parallel:?}");
        for value in ["cn=one", "cn=two", "cn=three"] {
            assert!(rendered.contains(value));
        }
    }

    #[test]
    fn parser_emits_all_rows() {
        let plan: Arc<ExecutionPlan> = Arc::new(ExecutionPlan::new());
        let mut builder = TableBuilder::with_plan(1024, plan);
        LdifParser.validate(SAMPLE).unwrap();
        LdifParser.parse_chunk(SAMPLE, &mut builder).unwrap();
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn parser_extracts_fields() {
        let plan: Arc<ExecutionPlan> = Arc::new(ExecutionPlan::new());
        let mut builder = TableBuilder::with_plan(1024, plan);
        LdifParser.validate(SAMPLE).unwrap();
        LdifParser.parse_chunk(SAMPLE, &mut builder).unwrap();
        let batch = builder.finish().unwrap();
        // Should have dn, objectClass, cn columns
        assert!(batch.schema().field_with_name("dn").is_ok());
        assert!(batch.schema().field_with_name("cn").is_ok());
    }
}
