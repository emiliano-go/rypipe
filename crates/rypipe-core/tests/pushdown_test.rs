//! Integration tests for `ExecutionPlan` pushdown correctness.
//!
//! Uses a tiny newline-delimited parser so the tests exercise the core engine
//! without any XML-specific logic.

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use arrow::record_batch::RecordBatch;

use rypipe_core::{
    ColumnarSink, CompareOp, ExecutionPlan, FieldType, FilterPredicate, RecordParser, TableBuilder,
    Value,
};

/// Parser for lines like `A=1 B=2`.
struct LineParser;

impl RecordParser for LineParser {
    fn validate(&self, bytes: &[u8]) -> rypipe_core::Result<()> {
        simdutf8::basic::from_utf8(bytes)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> rypipe_core::Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            sink.begin_row();
            for token in line.split_whitespace() {
                if let Some((k, v)) = token.split_once('=') {
                    sink.put_field(k, Value::Str(v));
                }
            }
            sink.end_row();
        }
        Ok(())
    }
}

fn parse_bytes(bytes: &[u8], plan: ExecutionPlan) -> RecordBatch {
    let mut sink = TableBuilder::with_plan((bytes.len() / 16).max(4), plan);
    LineParser.parse_chunk(bytes, &mut sink).unwrap();
    sink.finish().unwrap()
}

#[test]
fn test_rename_pushdown() {
    let mut plan = ExecutionPlan::new();
    plan.field_map.insert("X".to_string(), "Alpha".to_string());

    let batch = parse_bytes(b"X=hello\n", plan);
    assert!(batch.column_by_name("Alpha").is_some());
    assert!(batch.column_by_name("X").is_none());

    let alpha = batch.column_by_name("Alpha").unwrap().as_string::<i32>();
    assert_eq!(alpha.value(0), "hello");
}

#[test]
fn test_drop_pushdown() {
    let mut plan = ExecutionPlan::new();
    plan.drop_fields.insert("X".to_string());

    let batch = parse_bytes(b"X=hello Y=world\n", plan);
    assert!(batch.column_by_name("X").is_none());
    assert!(batch.column_by_name("Y").is_some());

    let y = batch.column_by_name("Y").unwrap().as_string::<i32>();
    assert_eq!(y.value(0), "world");
}

#[test]
fn test_typed_int64_pushdown() {
    let mut plan = ExecutionPlan::new();
    plan.field_types.insert("N".to_string(), FieldType::Int64);

    let batch = parse_bytes(b"N=10\nN=bad\nN=30\n", plan);
    let n = batch
        .column_by_name("N")
        .unwrap()
        .as_primitive::<Int64Type>();
    assert_eq!(n.value(0), 10);
    assert!(n.is_null(1));
    assert_eq!(n.value(2), 30);
}

#[test]
fn test_typed_float64_pushdown() {
    let mut plan = ExecutionPlan::new();
    plan.field_types.insert("N".to_string(), FieldType::Float64);

    let batch = parse_bytes(b"N=1.5\nN=not_a_number\nN=2.5\n", plan);
    let n = batch
        .column_by_name("N")
        .unwrap()
        .as_primitive::<Float64Type>();
    assert!((n.value(0) - 1.5).abs() < 1e-9);
    assert!(n.is_null(1));
    assert!((n.value(2) - 2.5).abs() < 1e-9);
}

#[test]
fn test_typed_boolean_pushdown() {
    let mut plan = ExecutionPlan::new();
    plan.field_types.insert("F".to_string(), FieldType::Boolean);

    let batch = parse_bytes(b"F=true\nF=false\nF=maybe\n", plan);
    let f = batch.column_by_name("F").unwrap().as_boolean();
    assert!(f.value(0));
    assert!(!f.value(1));
    assert!(f.is_null(2));
}

#[test]
fn test_dictionary_pushdown() {
    let mut plan = ExecutionPlan::new();
    plan.dictionary_columns.insert("P".to_string());

    let batch = parse_bytes(b"P=Widget\nP=Gadget\nP=Widget\n", plan);
    let col = batch.column_by_name("P").unwrap();
    assert_eq!(
        col.data_type(),
        &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
    );

    let dict = col.as_dictionary::<arrow::datatypes::Int32Type>();
    let values = dict.values().as_string::<i32>();
    assert_eq!(values.value(0), "Widget");
    assert_eq!(values.value(1), "Gadget");
    assert_eq!(dict.keys().value(0), 0);
    assert_eq!(dict.keys().value(1), 1);
    assert_eq!(dict.keys().value(2), 0);
}

#[test]
fn test_equal_filter_pushdown() {
    let mut plan = ExecutionPlan::new();
    plan.filter = Some(FilterPredicate::Equal {
        field: "A".to_string(),
        value: "yes".to_string(),
    });

    let batch = parse_bytes(b"A=yes\nA=no\nA=yes\n", plan);
    assert_eq!(batch.num_rows(), 2);
    let a = batch.column_by_name("A").unwrap().as_string::<i32>();
    assert_eq!(a.value(0), "yes");
    assert_eq!(a.value(1), "yes");
}

#[test]
fn test_not_equal_filter_pushdown() {
    let mut plan = ExecutionPlan::new();
    plan.filter = Some(FilterPredicate::NotEqual {
        field: "A".to_string(),
        value: "skip".to_string(),
    });

    let batch = parse_bytes(b"A=keep\nA=skip\nA=keep\n", plan);
    assert_eq!(batch.num_rows(), 2);
    let a = batch.column_by_name("A").unwrap().as_string::<i32>();
    assert_eq!(a.value(0), "keep");
    assert_eq!(a.value(1), "keep");
}

#[test]
fn test_compare_filter_pushdown() {
    let mut plan = ExecutionPlan::new();
    plan.filter = Some(FilterPredicate::Compare {
        field_a: "A".to_string(),
        op: CompareOp::Gt,
        field_b: "B".to_string(),
    });

    let batch = parse_bytes(b"A=3 B=1\nA=2 B=2\nA=5 B=4\n", plan);
    // Compare filters are applied post-reduce by `apply_compare_filter`.
    let filtered = rypipe_core::apply_compare_filter(batch, &FilterPredicate::Compare {
        field_a: "A".to_string(),
        op: CompareOp::Gt,
        field_b: "B".to_string(),
    })
    .unwrap();

    assert_eq!(filtered.num_rows(), 2);
    let a = filtered.column_by_name("A").unwrap().as_string::<i32>();
    assert_eq!(a.value(0), "3");
    assert_eq!(a.value(1), "5");
}
