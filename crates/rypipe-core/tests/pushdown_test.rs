//! Integration tests for `ExecutionPlan` pushdown correctness.
//!
//! Uses a tiny newline-delimited parser so the tests exercise the core engine
//! without any XML-specific logic.

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use rypipe_core::{
    ColumnarSink, CompareOp, ExecutionPlan, FieldType, FilterPredicate, RecordParser, TableBuilder,
    Value,
};

/// Parser for lines like `A=1 B=2`.
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
    let mut sink = TableBuilder::with_plan((bytes.len() / 16).max(4), Arc::new(plan));
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

    // Stringly columns: lexicographic comparison, so "3" < "4".
    let batch = parse_bytes(b"A=3 B=1\nA=2 B=2\nA=5 B=4\n", plan);
    assert_eq!(batch.num_rows(), 2);
    let a = batch.column_by_name("A").unwrap().as_string::<i32>();
    assert_eq!(a.value(0), "3");
    assert_eq!(a.value(1), "5");
}

/// Typed column-to-column comparison with numeric promotion.
#[test]
fn test_compare_typed_columns() {
    let mut plan = ExecutionPlan::new();
    plan.field_types.insert("A".to_string(), FieldType::Int64);
    plan.field_types.insert("B".to_string(), FieldType::Float64);
    plan.filter = Some(FilterPredicate::Compare {
        field_a: "A".to_string(),
        op: CompareOp::Gt,
        field_b: "B".to_string(),
    });

    // A > B numerically: 3 > 1.5 keep, 2 > 2.5 drop, 5 > 4.0 keep.
    let batch = parse_bytes(b"A=3 B=1.5\nA=2 B=2.5\nA=5 B=4.0\n", plan);
    assert_eq!(batch.num_rows(), 2);
    let a = batch
        .column_by_name("A")
        .unwrap()
        .as_primitive::<Int64Type>();
    assert_eq!(a.value(0), 3);
    assert_eq!(a.value(1), 5);
}

/// Mismatched non-numeric types (Str vs Int64) fail every row.
#[test]
fn test_compare_type_mismatch_fails_rows() {
    let mut plan = ExecutionPlan::new();
    plan.field_types.insert("A".to_string(), FieldType::Int64);
    plan.filter = Some(FilterPredicate::Compare {
        field_a: "A".to_string(),
        op: CompareOp::Lt,
        field_b: "S".to_string(), // S stays String
    });

    let batch = parse_bytes(b"A=1 S=x\nA=2 S=y\n", plan);
    assert_eq!(batch.num_rows(), 0);
}

/// Rows missing either compared field are rejected.
#[test]
fn test_compare_missing_field_rejects_row() {
    let mut plan = ExecutionPlan::new();
    plan.field_types.insert("A".to_string(), FieldType::Int64);
    plan.field_types.insert("B".to_string(), FieldType::Int64);
    plan.filter = Some(FilterPredicate::Compare {
        field_a: "A".to_string(),
        op: CompareOp::Le,
        field_b: "B".to_string(),
    });

    // Row 2 lacks B; row 3 lacks both.
    let batch = parse_bytes(b"A=1 B=9\nA=5\n\nA=7 B=8\n", plan);
    assert_eq!(batch.num_rows(), 2);
}

/// Compare through renames resolves to output column names.
#[test]
fn test_compare_resolves_rename() {
    let mut plan = ExecutionPlan::new();
    plan.field_map
        .insert("raw_a".to_string(), "alpha".to_string());
    plan.field_types
        .insert("alpha".to_string(), FieldType::Int64);
    plan.field_types
        .insert("beta".to_string(), FieldType::Int64);
    plan.filter = Some(FilterPredicate::Compare {
        field_a: "raw_a".to_string(),
        op: CompareOp::Ge,
        field_b: "beta".to_string(),
    });

    let batch = parse_bytes(b"raw_a=10 beta=3\nraw_a=1 beta=9\n", plan);
    assert_eq!(batch.num_rows(), 1);
}

/// Parallel fast path (no auto_dict) applies Compare filters per chunk and
/// matches the single-threaded result.
#[test]
fn test_compare_parallel_matches_single() {
    let splitter = LineSplitter;
    let mut plan = ExecutionPlan::new();
    plan.field_types.insert("A".to_string(), FieldType::Int64);
    plan.field_types.insert("B".to_string(), FieldType::Int64);
    plan.filter = Some(FilterPredicate::Compare {
        field_a: "A".to_string(),
        op: CompareOp::Gt,
        field_b: "B".to_string(),
    });

    let data: Vec<u8> = (0..1000)
        .flat_map(|i| format!("A={} B={}\n", i % 10, (i * 7) % 10).into_bytes())
        .collect();

    let single = parse_bytes(&data, plan.clone());

    let batches = rypipe_core::parallel::ParallelExecutor::parse(
        &data,
        &splitter,
        LineParser,
        Arc::new(plan),
        4,
    )
    .unwrap();

    let par_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(par_rows, single.num_rows());
    // Multiple chunks were produced, confirming the fast path was taken
    // rather than a serial merge into one batch.
    assert!(batches.len() > 1 || par_rows == 0);
}

/// Date32 columns parse ISO-8601 strings and export as Arrow Date32.
#[test]
fn test_date32_pushdown() {
    use arrow::array::Date32Array;

    let mut plan = ExecutionPlan::new();
    plan.field_types.insert("D".to_string(), FieldType::Date32);

    let batch = parse_bytes(b"D=2024-01-15\nD=nope\nD=1970-01-01\n", plan);
    assert_eq!(
        batch.column_by_name("D").unwrap().data_type(),
        &DataType::Date32
    );
    let d = batch
        .column_by_name("D")
        .unwrap()
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    // 2024-01-15 is 19737 days after the epoch; epoch itself is day 0.
    assert_eq!(d.value(0), 19737);
    assert!(d.is_null(1));
    assert_eq!(d.value(2), 0);
}

/// Timestamp columns parse ISO-8601 strings in the configured unit.
#[test]
fn test_timestamp_pushdown() {
    use arrow::datatypes::TimeUnit;

    for (type_str, expected_unit) in [
        ("timestamp[ms]", TimeUnit::Millisecond),
        ("timestamp", TimeUnit::Microsecond),
    ] {
        let mut plan = ExecutionPlan::new();
        plan.field_types.insert(
            "T".to_string(),
            FieldType::from_str(type_str).expect("valid type"),
        );

        let batch = parse_bytes(b"T=2024-01-15T10:30:00\nT=bad\n", plan);
        let t = batch.column_by_name("T").unwrap().clone();
        match t.data_type() {
            DataType::Timestamp(unit, None) => assert_eq!(*unit, expected_unit),
            other => panic!("unexpected type {other:?}"),
        }
    }
}

/// Equal filter on a Date32 column compares formatted dates.
#[test]
fn test_date32_equal_filter() {
    let mut plan = ExecutionPlan::new();
    plan.field_types.insert("D".to_string(), FieldType::Date32);
    plan.filter = Some(FilterPredicate::Equal {
        field: "D".to_string(),
        value: "2024-01-15".to_string(),
    });

    let batch = parse_bytes(b"D=2024-01-15\nD=2024-02-20\nD=2024-01-15\n", plan);
    assert_eq!(batch.num_rows(), 2);
}

/// Compare between two Timestamp columns is native-typed per row.
#[test]
fn test_timestamp_compare_filter() {
    use arrow::datatypes::TimeUnit;

    let mut plan = ExecutionPlan::new();
    plan.field_types.insert(
        "Start".to_string(),
        FieldType::Timestamp(TimeUnit::Microsecond),
    );
    plan.field_types.insert(
        "End".to_string(),
        FieldType::Timestamp(TimeUnit::Microsecond),
    );
    plan.filter = Some(FilterPredicate::Compare {
        field_a: "Start".to_string(),
        op: CompareOp::Lt,
        field_b: "End".to_string(),
    });

    // Earlier start keeps; equal start/end drops.
    let batch = parse_bytes(
        b"Start=2024-01-15T00:00:00 End=2024-01-16T00:00:00\n\
          Start=2024-02-01T12:00:00 End=2024-02-01T12:00:00\n",
        plan,
    );
    assert_eq!(batch.num_rows(), 1);
}

/// Auto-dict tuning: custom max_size and threshold are honored; defaults are
/// unchanged.
#[test]
fn test_dict_config_honored() {
    // 600 rows cycling through 10 distinct values.
    let mut data = Vec::new();
    for i in 0..600 {
        data.extend_from_slice(format!("S=v{}\n", i % 10).as_bytes());
    }

    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

    // Default (ratio 5% -> cap 30): 10 distinct <= 30, upgrades.
    let plan = ExecutionPlan::new().with_auto_dict(true);
    let batch = parse_bytes(&data, plan);
    assert_eq!(batch.column_by_name("S").unwrap().data_type(), &dict_type);

    // max_size=4 cannot hold 10 distinct values: stays string.
    let plan = ExecutionPlan::new()
        .with_auto_dict(true)
        .with_dict_max_size(4);
    let batch = parse_bytes(&data, plan);
    assert_eq!(
        batch.column_by_name("S").unwrap().data_type(),
        &DataType::Utf8
    );

    // 600 rows with 200 distinct values: default cap 30 -> no upgrade;
    // ratio 0.5 -> cap 300 -> upgrade.
    let mut data = Vec::new();
    for i in 0..600 {
        data.extend_from_slice(format!("S=w{}\n", i % 200).as_bytes());
    }
    let plan = ExecutionPlan::new().with_auto_dict(true);
    let batch = parse_bytes(&data, plan);
    assert_eq!(
        batch.column_by_name("S").unwrap().data_type(),
        &DataType::Utf8
    );

    let plan = ExecutionPlan::new()
        .with_auto_dict(true)
        .with_dict_threshold(0.5);
    let batch = parse_bytes(&data, plan);
    assert_eq!(batch.column_by_name("S").unwrap().data_type(), &dict_type);
}

/// A panicking adapter's panic message propagates into the returned error.
#[test]
fn test_parallel_panic_propagates_message() {
    #[derive(Clone)]
    struct PanickyParser;

    impl RecordParser for PanickyParser {
        fn validate(&self, _bytes: &[u8]) -> rypipe_core::Result<()> {
            Ok(())
        }

        fn parse_chunk(
            &self,
            _bytes: &[u8],
            _sink: &mut dyn ColumnarSink,
        ) -> rypipe_core::Result<()> {
            panic!("boom: bad row at 42");
        }
    }

    struct AnySplitter;

    impl rypipe_core::Splitter for AnySplitter {
        fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize> {
            vec![0, bytes.len().max(1) / max_chunks.max(1), bytes.len()]
        }

        fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
            sample.len().max(1)
        }
    }

    let err = rypipe_core::parallel::ParallelExecutor::parse(
        b"r1\nr2\n",
        &AnySplitter,
        PanickyParser,
        Arc::new(ExecutionPlan::new()),
        2,
    )
    .unwrap_err();

    let msg = err.to_string();
    assert!(msg.contains("boom: bad row at 42"), "got: {msg}");
}

/// Minimal newline splitter for the parallel test.
struct LineSplitter;

impl rypipe_core::Splitter for LineSplitter {
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize> {
        if max_chunks <= 1 || bytes.is_empty() {
            return vec![0, bytes.len()];
        }
        let mut points = vec![0usize];
        let stride = bytes.len() / max_chunks;
        let mut next = stride;
        for (i, &b) in bytes.iter().enumerate().skip(1) {
            if i >= next && b == b'\n' && points.len() < max_chunks {
                points.push(i + 1);
                next += stride;
            }
        }
        if *points.last().unwrap() != bytes.len() {
            points.push(bytes.len());
        }
        points
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let lines = sample.iter().filter(|&&b| b == b'\n').count().max(1);
        (sample.len() / lines).max(1)
    }
}

/// Compare between two Date32 columns: native-typed day comparison.
#[test]
fn test_date32_compare_filter() {
    use arrow::array::Date32Array;

    let mut plan = ExecutionPlan::new();
    plan.field_types.insert("A".to_string(), FieldType::Date32);
    plan.field_types.insert("B".to_string(), FieldType::Date32);
    plan.filter = Some(FilterPredicate::Compare {
        field_a: "A".to_string(),
        op: CompareOp::Gt,
        field_b: "B".to_string(),
    });

    // 2024-03-01 > 2024-01-15 → keep
    // 2024-01-10 > 2024-01-15 → drop
    // 2024-12-31 > 2024-06-15 → keep
    let batch = parse_bytes(
        b"A=2024-03-01 B=2024-01-15\nA=2024-01-10 B=2024-01-15\nA=2024-12-31 B=2024-06-15\n",
        plan,
    );
    assert_eq!(batch.num_rows(), 2);
    let a = batch
        .column_by_name("A")
        .unwrap()
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    // Both kept dates (2024-03-01, 2024-12-31) are after 2024-01-15.
    let day_2024_01_15 = 19737i32;
    assert!(a.value(0) > day_2024_01_15);
    assert!(a.value(1) > day_2024_01_15);
}

/// Cross-type Compare (Date32 vs String) rejects all rows.
#[test]
fn test_date32_vs_string_compare_fails() {
    let mut plan = ExecutionPlan::new();
    plan.field_types.insert("A".to_string(), FieldType::Date32);
    // B stays String
    plan.filter = Some(FilterPredicate::Compare {
        field_a: "A".to_string(),
        op: CompareOp::Gt,
        field_b: "B".to_string(),
    });

    let batch = parse_bytes(
        b"A=2024-01-15 B=2024-01-10\nA=2024-06-01 B=2024-01-01\n",
        plan,
    );
    assert_eq!(batch.num_rows(), 0);
}

/// Cross-type Compare (Int64 vs Float64) applies numeric promotion.
#[test]
fn test_int64_vs_float64_compare_promotion() {
    let mut plan = ExecutionPlan::new();
    plan.field_types.insert("A".to_string(), FieldType::Int64);
    plan.field_types.insert("B".to_string(), FieldType::Float64);
    plan.filter = Some(FilterPredicate::Compare {
        field_a: "A".to_string(),
        op: CompareOp::Le,
        field_b: "B".to_string(),
    });

    // 5 <= 5.0 → keep, 10 <= 3.0 → drop, 1 <= 2.5 → keep
    let batch = parse_bytes(b"A=5 B=5.0\nA=10 B=3.0\nA=1 B=2.5\n", plan);
    assert_eq!(batch.num_rows(), 2);
}

/// All Compare ops (Gt, Lt, Ge, Le, Eq, Ne) on typed columns.
#[test]
fn test_all_compare_ops_typed() {
    let data = b"A=1 B=2\nA=2 B=2\nA=3 B=2\n";

    for (op, expected) in [
        (CompareOp::Gt, 1), // 3 > 2
        (CompareOp::Lt, 1), // 1 < 2
        (CompareOp::Ge, 2), // 2 >= 2, 3 >= 2
        (CompareOp::Le, 2), // 1 <= 2, 2 <= 2
        (CompareOp::Eq, 1), // 2 == 2
        (CompareOp::Ne, 2), // 1 != 2, 3 != 2
    ] {
        let mut plan = ExecutionPlan::new();
        plan.field_types.insert("A".to_string(), FieldType::Int64);
        plan.field_types.insert("B".to_string(), FieldType::Int64);
        plan.filter = Some(FilterPredicate::Compare {
            field_a: "A".to_string(),
            op,
            field_b: "B".to_string(),
        });
        let batch = parse_bytes(data, plan);
        assert_eq!(
            batch.num_rows(),
            expected,
            "op={op:?} should keep {expected} rows"
        );
    }
}

/// Parser that uses `resolve_and_put` + `row_rejected` (like the crxml scanner),
/// instead of `put_field` (like LineParser).
#[derive(Clone)]
struct ResolveAndPutParser;

impl RecordParser for ResolveAndPutParser {
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
            for token in line.split_whitespace() {
                if let Some((k, v)) = token.split_once('=') {
                    sink.resolve_and_put(k, Value::Str(v));
                    if sink.row_rejected() {
                        break;
                    }
                }
            }
            sink.end_row();
        }
        Ok(())
    }
}

fn parse_bytes_resolve(bytes: &[u8], plan: ExecutionPlan) -> RecordBatch {
    let mut sink = TableBuilder::with_plan((bytes.len() / 16).max(4), Arc::new(plan));
    ResolveAndPutParser.parse_chunk(bytes, &mut sink).unwrap();
    sink.finish().unwrap()
}

#[test]
fn test_equal_filter_via_resolve_and_put() {
    let mut plan = ExecutionPlan::new();
    plan.filter = Some(FilterPredicate::Equal {
        field: "A".to_string(),
        value: "yes".to_string(),
    });

    let batch = parse_bytes_resolve(b"A=yes B=1\nA=no B=2\nA=yes B=3\n", plan);
    assert_eq!(batch.num_rows(), 2, "Equal filter via resolve_and_put should keep 2 rows");
    let a = batch.column_by_name("A").unwrap().as_string::<i32>();
    assert_eq!(a.value(0), "yes");
    assert_eq!(a.value(1), "yes");
}

#[test]
fn test_not_equal_filter_via_resolve_and_put() {
    let mut plan = ExecutionPlan::new();
    plan.filter = Some(FilterPredicate::NotEqual {
        field: "A".to_string(),
        value: "skip".to_string(),
    });

    let batch = parse_bytes_resolve(b"A=keep B=1\nA=skip B=2\nA=keep B=3\n", plan);
    assert_eq!(batch.num_rows(), 2, "NotEqual filter via resolve_and_put should keep 2 rows");
    let a = batch.column_by_name("A").unwrap().as_string::<i32>();
    assert_eq!(a.value(0), "keep");
    assert_eq!(a.value(1), "keep");
}
