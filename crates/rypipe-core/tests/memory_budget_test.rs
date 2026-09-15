use std::borrow::Cow;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::DataType;
use rypipe_core::{
    ColumnarSink, Error, ExecutionPlan, FieldType, MemoryBudget, Pipeline, RecordParser, Splitter,
    Value,
};

#[derive(Clone)]
struct Lines;

impl Splitter for Lines {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        memchr::memchr(b'\n', bytes.get(from..)?).map(|n| from + n + 1)
    }
    fn estimate_bytes_per_row(&self, _: &[u8]) -> usize {
        8
    }
}

#[derive(Clone)]
struct Parser;

impl RecordParser for Parser {
    fn validate(&self, bytes: &[u8]) -> rypipe_core::Result<()> {
        simdutf8::basic::from_utf8(bytes)?;
        Ok(())
    }
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> rypipe_core::Result<()> {
        for line in simdutf8::basic::from_utf8(bytes)?.lines() {
            let Some((id, text)) = line.split_once('=') else {
                continue;
            };
            sink.begin_row();
            sink.put_field("id", Value::Int64(id.parse().unwrap_or(0)));
            sink.put_field("text", Value::Str(Cow::Borrowed(text)));
            sink.end_row();
        }
        Ok(())
    }
}

#[derive(Clone)]
struct ExpandingParser;

impl RecordParser for ExpandingParser {
    fn validate(&self, _: &[u8]) -> rypipe_core::Result<()> {
        Ok(())
    }
    fn parse_chunk(&self, _: &[u8], sink: &mut dyn ColumnarSink) -> rypipe_core::Result<()> {
        sink.begin_row();
        sink.put_field("text", Value::Str(Cow::Owned("x".repeat(32 * 1024))));
        sink.end_row();
        Ok(())
    }
}

fn pipeline() -> Pipeline<Lines, Parser> {
    Pipeline::new(Lines, Parser)
}
fn memory(err: Error) -> bool {
    matches!(err, Error::Memory { .. })
}
#[test]
fn soft_budget_accepts_giant_row() {
    let data = format!("1={}\n", "x".repeat(32 * 1024));
    assert_eq!(
        pipeline()
            .read_bytes_stream(data.as_bytes(), MemoryBudget::new(8))
            .unwrap()[0]
            .num_rows(),
        1
    );
}

#[test]
fn wide_output_keeps_ordinary_batches_below_the_soft_target() {
    #[derive(Clone)]
    struct WideParser;
    impl RecordParser for WideParser {
        fn validate(&self, _: &[u8]) -> rypipe_core::Result<()> {
            Ok(())
        }
        fn parse_chunk(
            &self,
            bytes: &[u8],
            sink: &mut dyn ColumnarSink,
        ) -> rypipe_core::Result<()> {
            for line in std::str::from_utf8(bytes).unwrap().lines() {
                sink.begin_row();
                for name in ["a", "b", "c", "d", "e", "f", "g", "h"] {
                    sink.put_field(name, Value::Str(Cow::Borrowed(line)));
                }
                sink.end_row();
            }
            Ok(())
        }
    }
    let data = format!("{}\n", "x".repeat(32)).repeat(8_000);
    let budget = MemoryBudget::new(128 * 1024);
    let batches = Pipeline::new(Lines, WideParser)
        .read_bytes_stream(data.as_bytes(), budget)
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 8_000);
    assert!(batches
        .iter()
        .all(|b| b.num_columns() == 8 && b.get_array_memory_size() <= budget.bytes()));
}

#[test]
fn strict_budget_rejects_input_and_parser_expansion() {
    let data = b"1=x\n";
    assert!(matches!(
        pipeline().read_bytes_stream(data, MemoryBudget::new(64).with_strict(true)),
        Err(Error::Memory { .. })
    ));
    let result = rypipe_core::streaming::StreamingBatchIterator::new_bytes(
        b"x\n".to_vec(),
        Lines,
        ExpandingParser,
        Arc::new(ExecutionPlan::new()),
        MemoryBudget::new(16 * 1024).with_strict(true),
    );
    assert!(result
        .collect::<rypipe_core::Result<Vec<_>>>()
        .is_err_and(memory));
}

#[test]
fn ordinary_strict_typed_dictionary_read_passes() {
    let mut plan = ExecutionPlan::new().type_as("id", FieldType::Int64);
    plan.dictionary_columns.insert("text".into());
    let result = pipeline()
        .with_plan(plan)
        .read_bytes_stream(
            b"1=alpha\n2=beta\n",
            MemoryBudget::new(128 * 1024).with_strict(true),
        )
        .unwrap();
    assert_eq!(result.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    assert_eq!(
        result[0].column_by_name("id").unwrap().data_type(),
        &DataType::Int64
    );
    assert!(matches!(
        result[0].column_by_name("text").unwrap().data_type(),
        DataType::Dictionary(_, _)
    ));
}

#[test]
fn bounded_collection_reconciles_mixed_auto_dict_batches() {
    #[derive(Clone)]
    struct Late;
    impl RecordParser for Late {
        fn validate(&self, bytes: &[u8]) -> rypipe_core::Result<()> {
            simdutf8::basic::from_utf8(bytes)?;
            Ok(())
        }
        fn parse_chunk(
            &self,
            bytes: &[u8],
            sink: &mut dyn ColumnarSink,
        ) -> rypipe_core::Result<()> {
            for line in simdutf8::basic::from_utf8(bytes)?.lines() {
                let mut fields = line.split_whitespace();
                sink.begin_row();
                for field in fields.by_ref() {
                    let Some((name, value)) = field.split_once('=') else {
                        continue;
                    };
                    sink.put_field(name, Value::Str(Cow::Borrowed(value)));
                }
                sink.end_row();
            }
            Ok(())
        }
    }
    let mut data = String::new();
    for i in 0..12_000 {
        if i < 6_000 {
            data.push_str(&format!("key=v{}\n", i % 4));
        } else {
            data.push_str(&format!("key=unique-{i} late=y\n"));
        }
    }
    let plan = ExecutionPlan::new().with_auto_dict(true);
    let pipeline = Pipeline::new(Lines, Late).with_plan(plan);
    let budget = MemoryBudget::new(1024 * 1024);
    let mut raw = rypipe_core::consumer::CollectingConsumer(Vec::new());
    pipeline
        .read_bytes_stream_consumer(data.as_bytes(), budget, &mut raw)
        .unwrap();
    assert!(raw.0.iter().any(|b| matches!(
        b.column_by_name("key").unwrap().data_type(),
        DataType::Dictionary(_, _)
    )));
    assert!(raw
        .0
        .iter()
        .any(|b| b.column_by_name("key").unwrap().data_type() == &DataType::Utf8));
    let batches = pipeline.read_bytes_stream(data.as_bytes(), budget).unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 12_000);
    assert!(batches.iter().all(|b| b.schema() == batches[0].schema()));
    assert_eq!(
        batches
            .iter()
            .map(|b| b.column_by_name("late").unwrap().null_count())
            .sum::<usize>(),
        6_000
    );
    let key_values: Vec<_> = batches
        .iter()
        .flat_map(|b| {
            let col = b.column_by_name("key").unwrap();
            (0..b.num_rows()).map(move |i| col.as_string::<i32>().value(i).to_owned())
        })
        .collect();
    for (i, value) in key_values.iter().enumerate() {
        assert_eq!(
            *value,
            if i < 6_000 {
                format!("v{}", i % 4)
            } else {
                format!("unique-{i}")
            }
        );
    }
}

#[test]
fn strict_errors_propagate_through_iterator_then_recover() {
    let data = format!("{}1={}\n", "1=ok\n".repeat(10_000), "x".repeat(512 * 1024));
    let stream = rypipe_core::streaming::StreamingBatchIterator::new_bytes(
        data.into_bytes(),
        Lines,
        Parser,
        Arc::new(ExecutionPlan::new()),
        MemoryBudget::new(128 * 1024).with_strict(true),
    );
    let mut seen = 0;
    let mut failed = false;
    for batch in stream {
        match batch {
            Ok(batch) => {
                seen += batch.num_rows();
            }
            Err(error) => {
                assert!(memory(error));
                failed = true;
                break;
            }
        }
    }
    assert!(seen > 0 && failed);
    let clean = pipeline()
        .read_bytes_stream(b"1=ok\n", MemoryBudget::new(128 * 1024).with_strict(true))
        .unwrap();
    assert_eq!(clean.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[test]
fn strict_parallel_path_reports_memory_and_recovers() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), format!("1=ok\n2={}\n", "x".repeat(32 * 1024))).unwrap();
    let opts = rypipe_core::ParallelStreamOpts {
        threads: 2,
        ..Default::default()
    };
    let result = pipeline()
        .read_path_stream_par(
            file.path(),
            MemoryBudget::new(128 * 1024).with_strict(true),
            false,
            opts,
        )
        .unwrap()
        .collect::<rypipe_core::Result<Vec<_>>>();
    assert!(result.is_err_and(memory));
    let clean = pipeline().read_path(file.path(), false, false).unwrap();
    assert_eq!(clean.num_rows(), 2);
}

#[test]
fn strict_parallel_stream_preserves_order_across_batches() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let data: String = (0..20_000).map(|id| format!("{id}=value\n")).collect();
    std::fs::write(file.path(), data).unwrap();
    let stream = pipeline()
        .with_plan(ExecutionPlan::new().type_as("id", FieldType::Int64))
        .read_path_stream_par(
            file.path(),
            MemoryBudget::new(2 * 1024 * 1024).with_strict(true),
            false,
            rypipe_core::ParallelStreamOpts {
                threads: 2,
                ..Default::default()
            },
        )
        .unwrap();
    let mut rows = 0;
    let mut batches = 0;
    for batch in stream {
        let batch = batch.unwrap();
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        for id in ids.values() {
            assert_eq!(*id, rows);
            rows += 1;
        }
        batches += 1;
    }
    assert_eq!(rows, 20_000);
    assert!(batches > 1);
}

#[test]
fn ordered_stream_backpressures_workers_behind_a_slow_chunk() {
    #[derive(Clone)]
    struct SlowFirst(bool);
    impl RecordParser for SlowFirst {
        fn validate(&self, bytes: &[u8]) -> rypipe_core::Result<()> {
            Parser.validate(bytes)
        }
        fn parse_chunk(
            &self,
            bytes: &[u8],
            sink: &mut dyn ColumnarSink,
        ) -> rypipe_core::Result<()> {
            if sink.needs_value() && bytes.starts_with(b"0=") {
                assert!(!self.0, "first chunk failed");
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
            Parser.parse_chunk(bytes, sink)
        }
    }
    let file = tempfile::NamedTempFile::new().unwrap();
    let data: String = (0..100_000).map(|id| format!("{id}=value\n")).collect();
    std::fs::write(file.path(), data).unwrap();
    let stream = Pipeline::new(Lines, SlowFirst(false))
        .with_plan(ExecutionPlan::new().type_as("id", FieldType::Int64))
        .read_path_stream_par(
            file.path(),
            MemoryBudget::new(1024 * 1024),
            false,
            rypipe_core::ParallelStreamOpts {
                threads: 4,
                max_reorder: 1,
                ..Default::default()
            },
        )
        .unwrap();
    let mut expected = 0;
    for batch in stream {
        let batch = batch.unwrap();
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        for id in ids.values() {
            assert_eq!(*id, expected);
            expected += 1;
        }
    }
    assert_eq!(expected, 100_000);
    let failed = Pipeline::new(Lines, SlowFirst(true))
        .read_path_stream_par(
            file.path(),
            MemoryBudget::new(1024 * 1024),
            false,
            rypipe_core::ParallelStreamOpts {
                threads: 4,
                max_reorder: 1,
                ..Default::default()
            },
        )
        .unwrap()
        .collect::<rypipe_core::Result<Vec<_>>>();
    assert!(failed.unwrap_err().to_string().contains("worker panicked"));
}
