//! High-level, ergonomic API for running a `Splitter` + `RecordParser` pair.
//!
//! `Pipeline` removes the boilerplate of opening files, choosing an execution
//! mode, and applying the execution plan. It is the recommended entry point for
//! custom adapters.
//!
//! ```ignore
//! use rypipe_core::{ExecutionPlan, FieldType, Pipeline};
//!
//! let pipeline = Pipeline::new(MySplitter, MyParser)
//!     .with_plan(
//!         ExecutionPlan::new()
//!             .rename("raw_name", "name")
//!             .type_as("amount", FieldType::Float64),
//!     );
//!
//! let batch = pipeline.read_path("data.txt", false, false).unwrap();
//! ```

use std::path::Path;

use arrow::record_batch::RecordBatch;

use crate::bounded::{BoundedExecutor, MemoryBudget};
use crate::decoder::{RecordParser, Splitter};
use crate::engine::TableBuilder;
use crate::input::InputBuffer;
use crate::parallel::ParallelExecutor;
use crate::plan::ExecutionPlan;
use crate::Result;

/// A configured parser pipeline.
///
/// `S` is the splitter, `P` is the parser. Both must be `Clone` so the pipeline
/// can be reused across multiple files and execution modes.
#[derive(Clone, Debug)]
pub struct Pipeline<S, P> {
    splitter: S,
    parser: P,
    plan: ExecutionPlan,
}

impl<S, P> Pipeline<S, P>
where
    S: Splitter + Clone,
    P: RecordParser + Clone,
{
    /// Create a pipeline from a splitter/parser pair and a default plan.
    pub fn new(splitter: S, parser: P) -> Self {
        Self {
            splitter,
            parser,
            plan: ExecutionPlan::new(),
        }
    }

    /// Replace the execution plan.
    pub fn with_plan(mut self, plan: ExecutionPlan) -> Self {
        self.plan = plan;
        self
    }

    /// Parse an in-memory byte slice into a single `RecordBatch`.
    pub fn read_bytes(&self, bytes: &[u8]) -> Result<RecordBatch> {
        let mut engine = TableBuilder::with_plan((bytes.len() / 512).max(64), self.plan.clone());
        self.parser.validate(bytes)?;
        self.parser.parse_chunk(bytes, &mut engine)?;
        engine.finish()
    }

    /// Parse an in-memory byte slice in parallel, returning one or more
    /// `RecordBatch`es.
    ///
    /// Equivalent to [`Pipeline::read_path_par`] without file I/O: chunks are
    /// sliced directly from `bytes`.
    pub fn read_bytes_par(&self, bytes: &[u8], num_chunks: usize) -> Result<Vec<RecordBatch>> {
        ParallelExecutor::parse(
            bytes,
            &self.splitter,
            self.parser.clone(),
            self.plan.clone(),
            num_chunks,
        )
    }

    /// Parse an in-memory byte slice in bounded-memory batches.
    ///
    /// Equivalent to [`Pipeline::read_path_stream`] without file I/O: chunk
    /// ranges are computed over `bytes` and sliced directly.
    pub fn read_bytes_stream(
        &self,
        bytes: &[u8],
        budget: MemoryBudget,
    ) -> Result<Vec<RecordBatch>> {
        BoundedExecutor::new(budget).run_bytes(bytes, &self.splitter, self.parser.clone(), self.plan.clone())
    }

    /// Parse a file into a single `RecordBatch`.
    ///
    /// `use_mmap` requests a memory-mapped input when the `"mmap"` feature is
    /// enabled; otherwise the file is read into memory. `prefault` pre-faults
    /// mapped pages when true.
    pub fn read_path(
        &self,
        path: impl AsRef<Path>,
        use_mmap: bool,
        prefault: bool,
    ) -> Result<RecordBatch> {
        let input = InputBuffer::open(path.as_ref(), use_mmap, prefault)?;
        self.read_bytes(input.as_slice())
    }

    /// Parse a file in parallel, returning one or more `RecordBatch`es.
    ///
    /// The number of chunks is a hint; the splitter may return fewer chunks if
    /// the file is small.
    pub fn read_path_par(
        &self,
        path: impl AsRef<Path>,
        num_chunks: usize,
        use_mmap: bool,
        prefault: bool,
    ) -> Result<Vec<RecordBatch>> {
        let input = InputBuffer::open(path.as_ref(), use_mmap, prefault)?;
        ParallelExecutor::parse(
            input.as_slice(),
            &self.splitter,
            self.parser.clone(),
            self.plan.clone(),
            num_chunks,
        )
    }

    /// Parse a file in bounded-memory batches.
    pub fn read_path_stream(
        &self,
        path: impl AsRef<Path>,
        budget: MemoryBudget,
        prefault: bool,
    ) -> Result<Vec<RecordBatch>> {
        BoundedExecutor::new(budget).run(
            path.as_ref(),
            &self.splitter,
            self.parser.clone(),
            self.plan.clone(),
            prefault,
        )
    }

    /// Parse an in-memory byte slice in bounded-memory batches, calling
    /// `consumer` per batch (streaming, constant memory).
    pub fn read_bytes_stream_consumer<C>(
        &self,
        bytes: &[u8],
        budget: MemoryBudget,
        consumer: &mut C,
    ) -> Result<()>
    where
        C: crate::consumer::BatchConsumer,
    {
        BoundedExecutor::new(budget).run_bytes_stream(
            bytes,
            &self.splitter,
            self.parser.clone(),
            self.plan.clone(),
            consumer,
        )
    }

    /// Parse a file in bounded-memory batches, calling `consumer` per batch
    /// (streaming, constant memory).
    pub fn read_path_stream_consumer<C>(
        &self,
        path: impl AsRef<Path>,
        budget: MemoryBudget,
        prefault: bool,
        consumer: &mut C,
    ) -> Result<()>
    where
        C: crate::consumer::BatchConsumer,
    {
        BoundedExecutor::new(budget).run_stream(
            path.as_ref(),
            &self.splitter,
            self.parser.clone(),
            self.plan.clone(),
            prefault,
            consumer,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoder::{ColumnarSink, RecordParser, Splitter};
    use crate::value::Value;
    use crate::Result;

    #[derive(Clone, Debug, Default)]
    struct LineSplitter;

    impl Splitter for LineSplitter {
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

    #[derive(Clone, Debug, Default)]
    struct LineParser;

    impl RecordParser for LineParser {
        fn validate(&self, bytes: &[u8]) -> Result<()> {
            simdutf8::basic::from_utf8(bytes)?;
            Ok(())
        }

        fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
            let text = std::str::from_utf8(bytes).map_err(|e| crate::Error::Plan(e.to_string()))?;
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

    #[test]
    fn test_read_bytes() {
        let pipeline = Pipeline::new(LineSplitter, LineParser);
        let batch = pipeline.read_bytes(b"A=1 B=2\nA=3 B=4\n").unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_read_bytes_with_plan() {
        let pipeline = Pipeline::new(LineSplitter, LineParser).with_plan(
            ExecutionPlan::new()
                .rename("A", "Alpha")
                .drop("B")
                .type_as("Alpha", crate::plan::FieldType::Int64),
        );
        let batch = pipeline.read_bytes(b"A=1 B=2\nA=3 B=4\n").unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(batch.column_by_name("Alpha").is_some());
        assert!(batch.column_by_name("B").is_none());
    }
}
