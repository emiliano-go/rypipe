//! Schema-unification tests for the parallel fast-path export
//! (`engines_to_record_batches`) and end-to-end heterogeneous chunks.

use std::borrow::Cow;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Float64Type};
use std::sync::Arc;

use rypipe_core::{
    engines_to_record_batches, ColumnarSink, ExecutionPlan, FieldType, RecordParser, TableBuilder,
    Value,
};

/// Build a one-column builder: `ty` selects the storage variant, `values` are
/// string-encoded rows.
fn one_col(name: &str, ty: Option<FieldType>, values: &[&str]) -> TableBuilder {
    let mut plan = ExecutionPlan::new();
    if let Some(ty) = ty {
        plan.field_types.insert(name.to_string(), ty);
    }
    let mut b = TableBuilder::with_plan(values.len().max(1), Arc::new(plan));
    for v in values {
        ColumnarSink::put_field(&mut b, name, Value::Str(Cow::Borrowed(v)));
        ColumnarSink::end_row(&mut b);
    }
    b
}

#[test]
fn test_export_int64_vs_float64_unifies_to_float64() {
    let e1 = one_col("N", Some(FieldType::Int64), &["1", "2"]);
    let e2 = one_col("N", Some(FieldType::Float64), &["3.5"]);
    let batches = engines_to_record_batches(vec![e1, e2], &ExecutionPlan::new())
        .expect("int64 + float64 must unify");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    for batch in &batches {
        let n = batch.column_by_name("N").unwrap();
        assert_eq!(
            n.data_type(),
            &DataType::Float64,
            "unified type must be Float64"
        );
    }
    let mut vals = Vec::new();
    for batch in &batches {
        let n = batch
            .column_by_name("N")
            .unwrap()
            .as_primitive::<Float64Type>();
        for i in 0..n.len() {
            vals.push(n.value(i));
        }
    }
    assert_eq!(vals, vec![1.0, 2.0, 3.5]);
}

#[test]
fn test_export_string_vs_dictionary_unifies_to_dictionary() {
    let e1 = one_col("P", None, &["a", "b"]);

    let mut plan = ExecutionPlan::new();
    plan.dictionary_columns.insert("P".to_string());
    let mut e2 = TableBuilder::with_plan(4, Arc::new(plan));
    for v in ["a", "c"] {
        ColumnarSink::put_field(&mut e2, "P", Value::Str(Cow::Borrowed(v)));
        ColumnarSink::end_row(&mut e2);
    }

    let batches =
        engines_to_record_batches(vec![e1, e2], &ExecutionPlan::new()).expect("must unify");
    for batch in &batches {
        let p = batch.column_by_name("P").unwrap();
        assert_eq!(
            p.data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            "string side must be promoted to dictionary"
        );
    }
}

#[test]
fn test_export_irreconcilable_types_error() {
    let e1 = one_col("S", None, &["x"]);
    let e2 = one_col("S", Some(FieldType::Int64), &["7"]);
    let err = engines_to_record_batches(vec![e1, e2], &ExecutionPlan::new())
        .expect_err("string vs int64 must fail");
    match err {
        rypipe_core::Error::Merge(msg) => {
            assert!(msg.contains("'S'"), "error should name the column: {msg}");
            assert!(msg.contains("field_types"), "error should hint: {msg}");
        }
        other => panic!("expected Merge error, got {other:?}"),
    }
}

#[test]
fn test_export_missing_columns_null_filled() {
    let e1 = one_col("A", Some(FieldType::Int64), &["1"]);
    // Chunk 2 sees A and B; both chunks must agree on A's storage type.
    let mut plan = ExecutionPlan::new();
    plan.field_types.insert("A".to_string(), FieldType::Int64);
    plan.field_types.insert("B".to_string(), FieldType::String);
    let mut e2 = TableBuilder::with_plan(4, Arc::new(plan));
    ColumnarSink::put_field(&mut e2, "A", Value::Str(Cow::Borrowed("2")));
    ColumnarSink::put_field(&mut e2, "B", Value::Str(Cow::Borrowed("hello")));
    ColumnarSink::end_row(&mut e2);

    let batches =
        engines_to_record_batches(vec![e1, e2], &ExecutionPlan::new()).expect("export ok");
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].num_columns(), 2, "batch 1 must gain column B");
    assert_eq!(batches[1].num_columns(), 2);
    let b0 = batches[0].column_by_name("B").unwrap();
    assert_eq!(b0.len(), batches[0].num_rows());
    assert!(b0.is_null(0), "missing column must be null-filled");
    let b1 = batches[1].column_by_name("B").unwrap().as_string::<i32>();
    assert_eq!(b1.value(0), "hello");
}

/// Parser for lines like `A=1 B=2`; fields are optional per line so chunks can
/// disagree on which columns exist.
#[derive(Clone)]
struct SparseLineParser;

impl RecordParser for SparseLineParser {
    fn validate(&self, bytes: &[u8]) -> rypipe_core::Result<()> {
        simdutf8::basic::from_utf8(bytes)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> rypipe_core::Result<()> {
        let text = simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            sink.begin_row();
            for token in line.split_whitespace() {
                if let Some((k, v)) = token.split_once('=') {
                    sink.put_field(k, Value::Str(Cow::Borrowed(v)));
                }
            }
            sink.end_row();
        }
        Ok(())
    }
}

/// Newline splitter: every newline outside the last line is a boundary.
struct NewlineSplitter;

impl rypipe_core::Splitter for NewlineSplitter {
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
        let mut points = vec![0];
        for (i, &b) in bytes.iter().enumerate().skip(1) {
            if b == b'\n' && points.len() < max_chunks {
                points.push(i + 1);
            }
        }
        if *points.last().unwrap() != bytes.len() {
            points.push(bytes.len());
        }
        points
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let newlines = sample.iter().filter(|&&b| b == b'\n').count().max(1);
        (sample.len() / newlines).max(1)
    }
}

#[test]
fn test_parallel_heterogeneous_columns_share_schema() {
    // Chunk 1 has only A; chunk 2 has A and B.
    let bytes = b"A=1\nA=2\nA=3 B=x\n";
    let batches = rypipe_core::parallel::ParallelExecutor::parse(
        bytes,
        &NewlineSplitter,
        SparseLineParser,
        Arc::new(ExecutionPlan::new()),
        2,
    )
    .expect("parallel parse ok");
    assert_eq!(batches.len(), 2);
    assert!(
        batches.iter().all(|b| b.num_columns() == 2),
        "all batches must share the unified schema"
    );
    let total_b_values = batches
        .iter()
        .map(|b| {
            let col = b.column_by_name("B").unwrap();
            (0..col.len()).filter(|i| !col.is_null(*i)).count()
        })
        .sum::<usize>();
    assert_eq!(total_b_values, 1, "exactly one row carries a B value");
}
