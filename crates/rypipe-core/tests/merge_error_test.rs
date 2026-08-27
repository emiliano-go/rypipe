//! Error-path tests for table/merge operations.

use arrow::array::AsArray;
use arrow::datatypes::{DataType, Int32Type};

use rypipe_core::{ColumnarSink, ExecutionPlan, FieldType, TableBuilder, Value};

#[test]
fn test_extend_string_vs_int64_returns_error() {
    let mut e1 = TableBuilder::with_plan(1, ExecutionPlan::new());
    ColumnarSink::put_field(&mut e1, "N", Value::Str("1"));
    ColumnarSink::end_row(&mut e1);

    let mut plan = ExecutionPlan::new();
    plan.field_types.insert("N".to_string(), FieldType::Int64);
    let mut e2 = TableBuilder::with_plan(1, plan);
    ColumnarSink::put_field(&mut e2, "N", Value::Str("2"));
    ColumnarSink::end_row(&mut e2);

    let result = e1.extend(e2);
    match result {
        Err(rypipe_core::Error::Merge(msg)) => {
            assert!(msg.contains("'N'"), "error should name the column: {msg}");
            assert!(
                msg.contains("field_types"),
                "error should suggest field_types: {msg}"
            );
        }
        other => panic!("expected Merge error, got {other:?}"),
    }
}

#[test]
fn test_extend_string_vs_dictionary_promotes() {
    let mut e1 = TableBuilder::with_plan(4, ExecutionPlan::new());
    for v in ["x", "y"] {
        ColumnarSink::put_field(&mut e1, "P", Value::Str(v));
        ColumnarSink::end_row(&mut e1);
    }

    let mut plan = ExecutionPlan::new();
    plan.dictionary_columns.insert("P".to_string());
    let mut e2 = TableBuilder::with_plan(4, plan);
    for v in ["y", "z"] {
        ColumnarSink::put_field(&mut e2, "P", Value::Str(v));
        ColumnarSink::end_row(&mut e2);
    }

    e1.extend(e2).expect("string + dictionary must reconcile");
    let batch = e1.finish().expect("finish after promotion");
    assert_eq!(batch.num_rows(), 4);
    let p = batch.column_by_name("P").unwrap();
    assert_eq!(
        p.data_type(),
        &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
    );
    let dict = p.as_dictionary::<Int32Type>();
    let values = dict.values().as_string::<i32>();
    let expected = ["x", "y", "y", "z"];
    for (i, exp) in expected.iter().enumerate() {
        assert_eq!(values.value(dict.key(i).unwrap()), *exp);
    }
}

#[test]
fn test_extend_int64_vs_float64_promotes() {
    let mut plan1 = ExecutionPlan::new();
    plan1.field_types.insert("N".to_string(), FieldType::Int64);
    let mut e1 = TableBuilder::with_plan(2, plan1);
    for v in ["1", "2"] {
        ColumnarSink::put_field(&mut e1, "N", Value::Str(v));
        ColumnarSink::end_row(&mut e1);
    }

    let mut plan = ExecutionPlan::new();
    plan.field_types.insert("N".to_string(), FieldType::Float64);
    let mut e2 = TableBuilder::with_plan(2, plan);
    ColumnarSink::put_field(&mut e2, "N", Value::Str("3.5"));
    ColumnarSink::end_row(&mut e2);

    e1.extend(e2).expect("int64 + float64 must reconcile");
    let batch = e1.finish().expect("finish after promotion");
    assert_eq!(batch.num_rows(), 3);
    let n = batch.column_by_name("N").unwrap().as_primitive::<arrow::datatypes::Float64Type>();
    assert_eq!(n.value(0), 1.0);
    assert_eq!(n.value(1), 2.0);
    assert!((n.value(2) - 3.5).abs() < 1e-9);
}
