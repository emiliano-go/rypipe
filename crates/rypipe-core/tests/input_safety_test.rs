#![cfg(feature = "mmap")]

use std::borrow::Cow;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::StringArray;
use rypipe_core::bounded::{BoundedExecutor, MemoryBudget};
use rypipe_core::consumer::CollectingConsumer;
use rypipe_core::input::InputBuffer;
use rypipe_core::{ColumnarSink, ExecutionPlan, RecordParser, Splitter, Value};

#[derive(Clone)]
struct Parser;

impl RecordParser for Parser {
    fn validate(&self, bytes: &[u8]) -> rypipe_core::Result<()> {
        std::str::from_utf8(bytes).unwrap();
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> rypipe_core::Result<()> {
        for line in std::str::from_utf8(bytes).unwrap().lines() {
            sink.begin_row();
            sink.put_field("value", Value::Str(Cow::Borrowed(line)));
            sink.end_row();
        }
        Ok(())
    }
}

struct SwappingSplitter {
    original: PathBuf,
    moved: PathBuf,
}

impl Splitter for SwappingSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        memchr::memchr(b'\n', bytes.get(from..)?).map(|i| from + i + 1)
    }

    fn estimate_bytes_per_row(&self, _: &[u8]) -> usize {
        4
    }

    fn find_split_points(&self, bytes: &[u8], _: usize) -> Vec<usize> {
        std::fs::rename(&self.original, &self.moved).unwrap();
        std::fs::write(&self.original, b"new\n").unwrap();
        vec![0, bytes.len()]
    }
}

#[test]
#[cfg(feature = "mmap")]
fn bounded_stream_keeps_the_file_used_for_planning() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input.txt");
    std::fs::write(&path, b"old\n").unwrap();
    let splitter = SwappingSplitter {
        original: path.clone(),
        moved: dir.path().join("moved.txt"),
    };
    let mut consumer = CollectingConsumer(Vec::new());
    BoundedExecutor::new(MemoryBudget::new(1024))
        .run_stream(
            &path,
            &splitter,
            Parser,
            Arc::new(ExecutionPlan::new()),
            false,
            &mut consumer,
        )
        .unwrap();
    let batches = consumer.0;
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), "old");
}

#[test]
fn empty_file_opens_in_both_input_modes() {
    let file = tempfile::NamedTempFile::new().unwrap();
    for mapped in [false, true] {
        assert!(InputBuffer::open(file.path(), mapped, false)
            .unwrap()
            .is_empty());
    }
}

#[derive(Clone)]
struct PanicParser(bool);

impl Splitter for PanicParser {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        memchr::memchr(b'\n', bytes.get(from..)?).map(|offset| from + offset + 1)
    }

    fn estimate_bytes_per_row(&self, _: &[u8]) -> usize {
        5
    }
}

impl RecordParser for PanicParser {
    fn validate(&self, _: &[u8]) -> rypipe_core::Result<()> {
        assert!(!self.0, "validation panic");
        Ok(())
    }

    fn parse_chunk(&self, _: &[u8], _: &mut dyn ColumnarSink) -> rypipe_core::Result<()> {
        panic!("parse panic");
    }
}

#[test]
fn bounded_validation_and_parse_panics_return_errors_in_both_input_modes() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), b"test\n").unwrap();
    let executor = BoundedExecutor::new(MemoryBudget::new(1024));
    for validate in [false, true] {
        let parser = PanicParser(validate);
        let mut consumer = CollectingConsumer(Vec::new());
        let plan = Arc::new(ExecutionPlan::new());
        let byte_error = executor
            .run_bytes_stream(
                b"test\n",
                &parser,
                parser.clone(),
                Arc::clone(&plan),
                &mut consumer,
            )
            .unwrap_err();
        let file_error = executor
            .run_stream(
                file.path(),
                &parser,
                parser.clone(),
                plan,
                false,
                &mut consumer,
            )
            .unwrap_err();
        assert_eq!(byte_error.to_string(), file_error.to_string());
        assert!(byte_error.to_string().contains(if validate {
            "validation panic"
        } else {
            "parse panic"
        }));
    }
}
