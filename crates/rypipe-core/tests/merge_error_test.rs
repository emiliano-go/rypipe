//! Error-path tests for table/merge operations.

use rypipe_core::{ColumnarSink, ExecutionPlan, FieldType, TableBuilder, Value};

#[test]
fn test_merge_mismatched_column_types_returns_error() {
    let mut e1 = TableBuilder::with_plan(1, ExecutionPlan::new());
    ColumnarSink::put_field(&mut e1, "P", Value::Str("x"));
    ColumnarSink::end_row(&mut e1);

    let mut plan = ExecutionPlan::new();
    plan.dictionary_columns.insert("P".to_string());
    let mut e2 = TableBuilder::with_plan(1, plan);
    ColumnarSink::put_field(&mut e2, "P", Value::Str("x"));
    ColumnarSink::end_row(&mut e2);

    let result = e1.extend(e2);
    assert!(
        result.is_err(),
        "merging a String column with a Dictionary column must return an error"
    );
}

#[test]
fn test_extend_across_typed_column_mismatch_returns_error() {
    let mut e1 = TableBuilder::with_plan(1, ExecutionPlan::new());
    ColumnarSink::put_field(&mut e1, "N", Value::Str("1"));
    ColumnarSink::end_row(&mut e1);

    let mut plan = ExecutionPlan::new();
    plan.field_types.insert("N".to_string(), FieldType::Int64);
    let mut e2 = TableBuilder::with_plan(1, plan);
    ColumnarSink::put_field(&mut e2, "N", Value::Str("2"));
    ColumnarSink::end_row(&mut e2);

    let result = e1.extend(e2);
    assert!(
        result.is_err(),
        "merging a String column with an Int64 column must return an error"
    );
}
