use std::borrow::Cow;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Float64Type};
use rypipe_core::{
    ColumnarSink, ExecutionPlan, FieldType, Pipeline, RecordParser, RowObserver, Splitter,
    TableBuilder, Value,
};

fn builder(plan: ExecutionPlan, rows: &[Option<&str>]) -> arrow::record_batch::RecordBatch {
    let mut b = TableBuilder::with_plan(rows.len().max(1), Arc::new(plan));
    for value in rows {
        b.begin_row();
        if let Some(value) = value {
            b.put_field("value", Value::Str(Cow::Borrowed(value)));
        }
        b.end_row();
    }
    b.finish().unwrap()
}

#[test]
fn explicit_schema_survives_empty_input() {
    let mut plan = ExecutionPlan::new();
    plan.schema_order = vec!["value".into()];
    let batch = builder(plan, &[]);
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.schema().field(0).name(), "value");
}

#[test]
fn typed_schema_and_numeric_promotion_are_preserved() {
    let mut left = ExecutionPlan::new();
    left.field_types.insert("value".into(), FieldType::Int64);
    let mut right = ExecutionPlan::new();
    right.field_types.insert("value".into(), FieldType::Float64);
    let a = builder(left, &[Some("1")]);
    let b = builder(right, &[Some("2.5")]);
    assert_eq!(a.column(0).data_type(), &DataType::Int64);
    assert_eq!(b.column(0).data_type(), &DataType::Float64);
    let batches = rypipe_core::engines_to_record_batches(
        vec![table_builder_from(a), table_builder_from(b)],
        &ExecutionPlan::new(),
    )
    .unwrap();
    assert!(batches
        .iter()
        .all(|batch| batch.column(0).data_type() == &DataType::Float64));
    let values: Vec<f64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Float64Type>()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert!(values.contains(&1.0) && values.contains(&2.5));
}

fn table_builder_from(batch: arrow::record_batch::RecordBatch) -> TableBuilder {
    let mut plan = ExecutionPlan::new();
    plan.field_types.insert(
        "value".into(),
        if batch.column(0).data_type() == &DataType::Int64 {
            FieldType::Int64
        } else {
            FieldType::Float64
        },
    );
    let mut b = TableBuilder::with_plan(batch.num_rows().max(1), Arc::new(plan));
    for i in 0..batch.num_rows() {
        b.begin_row();
        let value = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>();
        if let Some(value) = value {
            b.put_field("value", Value::Int64(value.value(i)));
        } else {
            let value = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            b.put_field("value", Value::Float64(value.value(i)));
        }
        b.end_row();
    }
    b
}

#[test]
fn dictionary_empty_single_and_unification_contract() {
    let mut plan = ExecutionPlan::new();
    plan.dictionary_columns.insert("value".into());
    let empty = builder(plan.clone(), &[]);
    assert_eq!(empty.num_rows(), 0);
    let single = builder(plan.clone(), &[Some("one"), Some("one")]);
    assert!(matches!(
        single.column(0).data_type(),
        DataType::Dictionary(_, _)
    ));
    let mut auto = ExecutionPlan::new();
    auto.auto_dict = true;
    auto.dict_threshold = Some(1.0);
    auto.dict_max_size = Some(1);
    let capped = builder(auto, &[Some("one"), Some("two")]);
    assert_eq!(capped.column(0).data_type(), &DataType::Utf8);
}

struct CountingObserver {
    accepted: Mutex<usize>,
    rejected: Mutex<usize>,
    chunks: Mutex<Vec<(usize, usize, usize)>>,
}

impl RowObserver for CountingObserver {
    fn on_row_accepted(&self, _: usize) {
        *self.accepted.lock().unwrap() += 1;
    }
    fn on_row_rejected(&self, _: usize) {
        *self.rejected.lock().unwrap() += 1;
    }
    fn on_chunk_finished(&self, total: usize, accepted: usize, rejected: usize) {
        self.chunks
            .lock()
            .unwrap()
            .push((total, accepted, rejected));
    }
}

#[derive(Clone)]
struct LineParser;

impl RecordParser for LineParser {
    fn validate(&self, _: &[u8]) -> rypipe_core::Result<()> {
        Ok(())
    }
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> rypipe_core::Result<()> {
        for line in bytes.split(|b| *b == b'\n').filter(|line| !line.is_empty()) {
            sink.begin_row();
            if let Some(pos) = line.iter().position(|b| *b == b'=') {
                let (name, value) = (&line[..pos], &line[pos + 1..]);
                sink.put_field(
                    std::str::from_utf8(name).unwrap(),
                    Value::Str(Cow::Borrowed(std::str::from_utf8(value).unwrap())),
                );
            }
            sink.end_row();
        }
        Ok(())
    }
}

#[derive(Clone)]
struct LineSplitter;

impl Splitter for LineSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        memchr::memchr(b'\n', bytes.get(from..)?).map(|n| from + n + 1)
    }
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize> {
        if max_chunks <= 1 {
            return vec![0, bytes.len()];
        }
        let mut p = vec![0];
        for (i, b) in bytes.iter().enumerate() {
            if *b == b'\n' && p.len() < max_chunks {
                p.push(i + 1);
            }
        }
        if *p.last().unwrap() != bytes.len() {
            p.push(bytes.len());
        }
        p
    }
    fn estimate_bytes_per_row(&self, _: &[u8]) -> usize {
        4
    }
}

struct PanicObserver;
impl RowObserver for PanicObserver {
    fn on_row_accepted(&self, _: usize) {
        panic!("observer panic");
    }
}

struct ChunkPanicObserver;
impl RowObserver for ChunkPanicObserver {
    fn on_chunk_finished(&self, _: usize, _: usize, _: usize) {
        panic!("chunk observer panic");
    }
}

#[test]
fn parallel_observer_panic_returns_error_and_next_read_succeeds() {
    let mut plan = ExecutionPlan::new();
    plan.observer = Some(Arc::new(PanicObserver));
    let pipeline = Pipeline::new(LineSplitter, LineParser).with_plan(plan);
    assert!(pipeline.read_bytes_par(b"a=1\nb=2\n", 2).is_err());
    let clean = Pipeline::new(LineSplitter, LineParser);
    assert_eq!(
        clean
            .read_bytes_par(b"a=1\nb=2\n", 2)
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        2
    );
}

#[test]
fn parallel_chunk_observer_panic_returns_error_and_next_read_succeeds() {
    let mut plan = ExecutionPlan::new();
    plan.observer = Some(Arc::new(ChunkPanicObserver));
    let pipeline = Pipeline::new(LineSplitter, LineParser).with_plan(plan);
    assert!(pipeline.read_bytes_par(b"a=1\nb=2\n", 2).is_err());
    let clean = Pipeline::new(LineSplitter, LineParser);
    assert_eq!(
        clean
            .read_bytes_par(b"a=1\nb=2\n", 2)
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        2
    );
}

#[test]
fn schema_cache_concurrent_insert_and_poison_recovery() {
    rypipe_core::schema::clear_schema_cache();
    let joins: Vec<_> = (0..8)
        .map(|i| {
            std::thread::spawn(move || {
                rypipe_core::schema::insert_schema_cache((i, i), Arc::new(vec![format!("c{i}")]));
            })
        })
        .collect();
    for join in joins {
        join.join().unwrap();
    }
    {
        let cache = rypipe_core::schema::SCHEMA_CACHE
            .read()
            .unwrap_or_else(|e| e.into_inner());
        for i in 0..8 {
            assert_eq!(cache.get(&(i, i)).unwrap().as_ref(), &[format!("c{i}")]);
        }
    }
    let poisoned = std::panic::catch_unwind(|| {
        let _guard = rypipe_core::schema::SCHEMA_CACHE.write().unwrap();
        panic!("poison cache lock");
    });
    assert!(poisoned.is_err());
    rypipe_core::schema::insert_schema_cache((999, 999), Arc::new(vec!["ok".into()]));
    let discovered = rypipe_core::parallel_stream::discover_schema_for_bytes(
        b"value=x\n",
        &LineSplitter,
        &LineParser,
        &ExecutionPlan::new(),
    );
    assert_eq!(discovered.column_names()[0].as_ref(), "value");
    rypipe_core::schema::SCHEMA_CACHE.clear_poison();
    rypipe_core::schema::clear_schema_cache();
}

#[test]
fn table_builder_lifecycle_and_null_rows_are_stable() {
    let mut end_without_begin = TableBuilder::new();
    end_without_begin.end_row();
    assert!(end_without_begin.finish().is_ok());

    let mut unfinished = TableBuilder::new();
    unfinished.begin_row();
    unfinished.put_field("value", Value::Str(Cow::Borrowed("committed")));
    unfinished.end_row();
    unfinished.begin_row();
    unfinished.put_field("value", Value::Str(Cow::Borrowed("partial")));
    let batch = unfinished.finish().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.column(0).as_string::<i32>().value(0), "committed");

    let mut duplicate_begin = TableBuilder::new();
    duplicate_begin.begin_row();
    duplicate_begin.begin_row();
    duplicate_begin.put_field("value", Value::Str(Cow::Borrowed("x")));
    duplicate_begin.end_row();
    assert_eq!(duplicate_begin.finish().unwrap().num_rows(), 1);

    let mut explicit = ExecutionPlan::new();
    explicit.schema_order = vec!["value".into()];
    explicit
        .field_types
        .insert("value".into(), FieldType::Int64);
    let batch = builder(explicit, &[None]);
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.column(0).data_type(), &DataType::Int64);
    assert!(batch.column(0).is_null(0));
}

#[test]
fn extend_drops_partial_rows_before_appending_committed_rows() {
    let mut left = TableBuilder::new();
    left.begin_row();
    left.put_field("value", Value::Str(Cow::Borrowed("one")));
    left.end_row();
    left.begin_row();
    left.put_field("value", Value::Str(Cow::Borrowed("ghost-left")));

    let mut right = TableBuilder::new();
    right.begin_row();
    right.put_field("value", Value::Str(Cow::Borrowed("two")));
    right.end_row();
    right.begin_row();
    right.put_field("value", Value::Str(Cow::Borrowed("ghost")));
    left.extend(right).unwrap();
    let batch = left.finish().unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.column(0).as_string::<i32>().value(0), "one");
    assert_eq!(batch.column(0).as_string::<i32>().value(1), "two");
}

#[test]
fn extend_preserves_unknown_field_errors() {
    let plan = ExecutionPlan::new();
    let schema = rypipe_core::schema::FrozenSchema::from_plan(&["value"], &plan);
    let mut unknown = TableBuilder::with_plan(1, Arc::new(plan.clone()));
    unknown.ensure_schema(&schema).unwrap();
    unknown.begin_row();
    unknown.put_field("unexpected", Value::Str(Cow::Borrowed("x")));
    unknown.end_row();
    let mut target = TableBuilder::new();
    target.extend(unknown).unwrap();
    assert!(target.finish().is_err());
}

#[test]
fn observer_hooks_are_safe_to_share() {
    let observer = Arc::new(CountingObserver {
        accepted: Mutex::new(0),
        rejected: Mutex::new(0),
        chunks: Mutex::new(Vec::new()),
    });
    let mut plan = ExecutionPlan::new();
    plan.observer = Some(observer.clone());
    let batch = builder(plan, &[Some("x"), None]);
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(*observer.accepted.lock().unwrap(), 2);
    assert_eq!(*observer.chunks.lock().unwrap(), vec![(2, 2, 0)]);
}

#[test]
fn serial_chunk_observer_panic_is_an_error() {
    let mut plan = ExecutionPlan::new();
    plan.observer = Some(Arc::new(ChunkPanicObserver));
    let mut table = TableBuilder::with_plan(1, Arc::new(plan));
    table.begin_row();
    table.put_field("value", Value::Str(Cow::Borrowed("x")));
    table.end_row();
    assert!(table.finish().is_err());
}
