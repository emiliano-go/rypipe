//! Sanity tests for `BoundedExecutor`.

use std::borrow::Cow;

use arrow::array::{Array, AsArray};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use std::path::PathBuf;
use std::sync::Arc;

use rypipe_core::{
    bounded::{BoundedExecutor, MemoryBudget},
    ColumnarSink, ExecutionPlan, RecordParser, Splitter, TableBuilder, Value,
};

#[derive(Clone)]
struct LineParser;

impl RecordParser for LineParser {
    fn validate(&self, bytes: &[u8]) -> rypipe_core::Result<()> {
        simdutf8::basic::from_utf8(bytes)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> rypipe_core::Result<()> {
        let text =
            std::str::from_utf8(bytes).map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            sink.begin_row();
            if let Some((k, v)) = line.split_once('=') {
                sink.put_field(k, Value::Str(Cow::Borrowed(v)));
            }
            sink.end_row();
        }
        Ok(())
    }
}

struct LineSplitter;

impl Splitter for LineSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        if from >= bytes.len() {
            return None;
        }
        let start = if bytes[from] == b'\n' { from + 1 } else { from };
        if start >= bytes.len() {
            return None;
        }
        memchr::memchr(b'\n', &bytes[start..]).map(|rel| start + rel + 1)
    }
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize> {
        if max_chunks <= 1 || bytes.is_empty() {
            return vec![0, bytes.len()];
        }
        let mut points = vec![0usize];
        let mut last = 0;
        for (i, &b) in bytes.iter().enumerate() {
            if b == b'\n' {
                let next = i + 1;
                if next > last && points.len() < max_chunks {
                    points.push(next);
                    last = next;
                }
            }
        }
        if *points.last().unwrap() != bytes.len() {
            points.push(bytes.len());
        }
        points
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let newline_count = sample.iter().filter(|&&b| b == b'\n').count().max(1);
        (sample.len() / newline_count).max(1)
    }
}

fn temp_file_path() -> PathBuf {
    std::env::temp_dir().join(format!("rypipe_bounded_test_{}.txt", std::process::id()))
}

fn build_file() -> (PathBuf, Vec<u8>) {
    let mut data = Vec::new();
    for i in 0..12 {
        data.extend_from_slice(format!("A={}\n", i).as_bytes());
    }
    let path = temp_file_path();
    std::fs::write(&path, &data).unwrap();
    (path, data)
}

fn parse_single(bytes: &[u8]) -> RecordBatch {
    let mut sink = TableBuilder::with_plan(bytes.len() / 4, Arc::new(ExecutionPlan::new()));
    LineParser.parse_chunk(bytes, &mut sink).unwrap();
    sink.finish().unwrap()
}

#[test]
fn test_bounded_executor_produces_matching_batches() {
    let (path, bytes) = build_file();
    let _cleanup = Cleanup(&path);

    let executor = BoundedExecutor::new(MemoryBudget::new(12));
    let batches = executor
        .run(
            &path,
            &LineSplitter,
            LineParser,
            Arc::new(ExecutionPlan::new()),
            false,
        )
        .unwrap();

    assert!(
        !batches.is_empty(),
        "bounded executor must return at least one batch for a non-empty file"
    );
    assert!(
        batches.iter().any(|b| b.num_rows() > 0),
        "at least one batch must be non-empty"
    );

    let schema = batches[0].schema();
    let concatenated = concat_batches(&schema, batches.iter().collect::<Vec<_>>()).unwrap();
    let expected = parse_single(&bytes);

    assert_eq!(concatenated.num_rows(), expected.num_rows());
    assert_eq!(concatenated.schema(), expected.schema());

    let actual = concatenated.column_by_name("A").unwrap().as_string::<i32>();
    let exp = expected.column_by_name("A").unwrap().as_string::<i32>();
    for i in 0..exp.len() {
        assert_eq!(actual.value(i), exp.value(i));
    }
}

struct Cleanup<'a>(&'a std::path::Path);

impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.0);
    }
}
