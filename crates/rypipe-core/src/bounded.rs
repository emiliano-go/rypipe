#[cfg(feature = "mmap")]
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::arrow_export::apply_compare_filter;
use crate::consumer::CollectingConsumer;
use crate::decoder::{split_points_to_ranges, RecordParser, Splitter};
use crate::engine::TableBuilder;
use crate::input::InputBuffer;
use crate::plan::ExecutionPlan;
use crate::Result;

/// A memory budget expressed in bytes.
#[derive(Clone, Copy, Debug)]
pub struct MemoryBudget {
    bytes: usize,
}

impl MemoryBudget {
    pub fn new(bytes: usize) -> Self {
        Self { bytes }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

/// Internal safeguard: never request more than this many split points, so a
/// pathological row-size estimate cannot explode per-chunk overhead. The
/// batch count is otherwise derived from the budget, input size, and
/// estimated bytes per row; batches may still exceed the budget when the
/// required count exceeds this cap. Increased for 64KB streaming (50GB/64KB
/// ≈ 800k batches); still bounded by file scan cost.
pub const MAX_SPLIT_CHUNKS: usize = 100_000;

/// Parse an input in bounded batches to stay within a memory budget.
pub struct BoundedExecutor {
    budget: MemoryBudget,
    split_cap: usize,
}

impl BoundedExecutor {
    pub fn new(budget: MemoryBudget) -> Self {
        Self {
            budget,
            split_cap: MAX_SPLIT_CHUNKS,
        }
    }

    /// Override the split cap (default [`MAX_SPLIT_CHUNKS`]). Raising it may be
    /// needed for very large files with small budgets, where capped batches
    /// would otherwise overshoot the budget.
    pub fn with_split_cap(mut self, cap: usize) -> Self {
        self.split_cap = cap;
        self
    }

    /// Derive chunk ranges and batch sizing from an in-memory sample of the
    /// input. Returns `(chunk_ranges, rows_per_batch, bytes_per_row)`.
    fn plan_chunks(
        &self,
        bytes: &[u8],
        splitter: &dyn Splitter,
    ) -> (Vec<Range<usize>>, usize, usize) {
        let bytes_per_row = splitter.estimate_bytes_per_row(bytes).max(1);
        let total_rows_est = bytes.len() / bytes_per_row;
        let rows_per_batch = (self.budget.bytes() / bytes_per_row)
            .max(1)
            .min(total_rows_est.max(1));

        let num_batches = (total_rows_est / rows_per_batch).max(1);
        let capped = num_batches.min(self.split_cap);
        let split_points = splitter.find_split_points(bytes, capped);
        let chunks = split_points_to_ranges(&split_points, bytes.len());
        (chunks, rows_per_batch, bytes_per_row)
    }

    fn parse_chunk<P>(
        parser: &P,
        bytes: &[u8],
        plan: Arc<ExecutionPlan>,
        bytes_per_row: usize,
    ) -> Result<TableBuilder>
    where
        P: RecordParser,
    {
        let mut engine =
            TableBuilder::with_plan((bytes.len() / bytes_per_row.max(512)).max(64), plan);
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parser.validate(bytes)?;
            parser.parse_chunk_generic(bytes, &mut engine)
        }))
        .unwrap_or_else(|payload| {
            let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic".to_string()
            };
            Err(crate::Error::Parser(format!(
                "worker panicked during bounded parse: {msg}"
            )))
        })?;
        Ok(engine)
    }

    fn consume_chunks<I, C>(
        &self,
        chunks: I,
        rows_per_batch: usize,
        bytes_per_row: usize,
        plan: Arc<ExecutionPlan>,
        consumer: &mut C,
    ) -> Result<()>
    where
        I: IntoIterator<Item = Result<TableBuilder>>,
        C: crate::consumer::BatchConsumer,
    {
        let mut batch_engine = TableBuilder::with_plan(bytes_per_row.max(64), Arc::clone(&plan));
        let mut rows_in_batch = 0usize;
        for chunk_engine in chunks {
            let chunk_engine = chunk_engine?;
            let chunk_rows = chunk_engine.num_rows();
            batch_engine.extend(chunk_engine)?;
            rows_in_batch += chunk_rows;
            while rows_in_batch >= rows_per_batch
                || batch_engine.bytes_used() >= self.budget.bytes()
            {
                if batch_engine.num_rows() == 0 {
                    break;
                }
                let n = rows_per_batch.min(batch_engine.num_rows());
                let n = if batch_engine.bytes_used() >= self.budget.bytes()
                    && batch_engine.num_rows() > 1
                {
                    let est = (batch_engine.num_rows() as f64 * self.budget.bytes() as f64
                        / batch_engine.bytes_used() as f64) as usize;
                    est.clamp(1, n)
                } else {
                    n
                };
                let mut batch = batch_engine.split_off(n).finish()?;
                if let Some(ref filter) = plan.filter {
                    batch = apply_compare_filter(batch, filter)?;
                }
                consumer.consume(batch)?;
                rows_in_batch = rows_in_batch.saturating_sub(n);
            }
        }
        if let Some(err) = batch_engine.unknown_error.take() {
            return Err(crate::Error::Merge(err));
        }
        if let Some(err) = batch_engine.strict_error.take() {
            return Err(err);
        }
        if batch_engine.num_rows() > 0 {
            let mut batch = batch_engine.finish()?;
            if let Some(ref filter) = plan.filter {
                batch = apply_compare_filter(batch, filter)?;
            }
            consumer.consume(batch)?;
        }
        Ok(())
    }

    /// Parse an in-memory byte slice in bounded batches, calling `consumer` per batch.
    ///
    /// Chunks are sliced directly from `bytes`; no file I/O occurs. This is
    /// the streaming entry point for adapters holding decompressed data.
    /// The budget controls batch sizing; allocator, parser, input, and Arrow
    /// overhead can raise process RSS above this target.
    pub fn run_bytes_stream<P, C>(
        &self,
        bytes: &[u8],
        splitter: &dyn Splitter,
        parser: P,
        plan: Arc<ExecutionPlan>,
        consumer: &mut C,
    ) -> Result<()>
    where
        P: RecordParser + Clone + Send + Sync,
        C: crate::consumer::BatchConsumer,
    {
        if bytes.is_empty() {
            return Ok(());
        }

        let (mut chunks, rows_per_batch, bytes_per_row) = self.plan_chunks(bytes, splitter);

        // Guard against degenerate splitter output that produces no ranges.
        if chunks.is_empty() {
            chunks.push(0..bytes.len());
        }

        let parse_plan = Arc::clone(&plan);
        let chunks = chunks.into_iter().map(move |chunk| {
            Self::parse_chunk(
                &parser,
                &bytes[chunk.start..chunk.end],
                Arc::clone(&parse_plan),
                bytes_per_row,
            )
        });
        self.consume_chunks(chunks, rows_per_batch, bytes_per_row, plan, consumer)
    }

    /// Parse an in-memory byte slice in bounded batches, returning one
    /// `RecordBatch` per batch.
    ///
    /// Chunks are sliced directly from `bytes`; no file I/O occurs. This is
    /// the entry point for adapters holding decompressed or streamed-in data.
    pub fn run_bytes<P>(
        &self,
        bytes: &[u8],
        splitter: &dyn Splitter,
        parser: P,
        plan: Arc<ExecutionPlan>,
    ) -> Result<Vec<RecordBatch>>
    where
        P: RecordParser + Clone + Send + Sync,
    {
        let batches = Vec::new();
        let mut consumer = CollectingConsumer(batches);
        self.run_bytes_stream(bytes, splitter, parser, plan, &mut consumer)?;
        Ok(consumer.0)
    }

    /// Parse `path` in batches, calling `consumer` per batch (streaming).
    ///
    /// Compressed inputs are decompressed up front and served via
    /// `run_bytes_stream`. Uncompressed inputs with `mmap` use the seek-based
    /// path with a reusable chunk buffer to keep RSS constant.
    pub fn run_stream<P, C>(
        &self,
        path: &Path,
        splitter: &dyn Splitter,
        parser: P,
        plan: Arc<ExecutionPlan>,
        prefault: bool,
        consumer: &mut C,
    ) -> Result<()>
    where
        P: RecordParser + Clone + Send + Sync,
        C: crate::consumer::BatchConsumer,
    {
        let use_mmap = cfg!(feature = "mmap");
        let input = InputBuffer::open(path, use_mmap, prefault)?;

        #[cfg(feature = "mmap")]
        if matches!(input, InputBuffer::Mmap(_)) {
            return self.run_mapped_stream(input, splitter, parser, plan, consumer);
        }

        self.run_bytes_stream(input.as_slice(), splitter, parser, plan, consumer)
    }

    /// Parse `path` in batches, returning one `RecordBatch` per batch.
    ///
    /// The caller is responsible for concatenating the batches if a single
    /// table is desired.
    ///
    /// Compressed inputs (see [`InputBuffer::open`]) are decompressed up
    /// front and served from memory via [`BoundedExecutor::run_bytes`].
    /// Uncompressed inputs on platforms with the `mmap` feature retain the
    /// seek-based path: the mapping is dropped after planning and each chunk
    /// is read from disk on demand, keeping resident memory low.
    pub fn run<P>(
        &self,
        path: &Path,
        splitter: &dyn Splitter,
        parser: P,
        plan: Arc<ExecutionPlan>,
        prefault: bool,
    ) -> Result<Vec<RecordBatch>>
    where
        P: RecordParser + Clone + Send + Sync,
    {
        let batches = Vec::new();
        let mut consumer = CollectingConsumer(batches);
        self.run_stream(path, splitter, parser, plan, prefault, &mut consumer)?;
        Ok(consumer.0)
    }

    /// Streaming path for mapped inputs: plan against the mapping, drop it,
    /// then read each chunk with a reusable buffer.
    #[cfg(feature = "mmap")]
    fn run_mapped_stream<P, C>(
        &self,
        input: InputBuffer,
        splitter: &dyn Splitter,
        parser: P,
        plan: Arc<ExecutionPlan>,
        consumer: &mut C,
    ) -> Result<()>
    where
        P: RecordParser + Clone + Send + Sync,
        C: crate::consumer::BatchConsumer,
    {
        let bytes = input.as_slice();
        if bytes.is_empty() {
            return Ok(());
        }

        let (mut chunks, rows_per_batch, bytes_per_row) = self.plan_chunks(bytes, splitter);

        // Guard against degenerate splitter output that produces no ranges.
        if chunks.is_empty() {
            chunks.push(0..bytes.len());
        }

        let InputBuffer::Mmap(handle) = input else {
            unreachable!("mapped input required");
        };
        let mut file = handle.file;
        drop(handle.mmap);

        // Reusable buffer sized to the largest chunk to avoid per-chunk alloc.
        let max_chunk = chunks.iter().map(|r| r.len()).max().unwrap_or(0);
        let mut chunk_buf = Vec::with_capacity(max_chunk);

        let parse_plan = Arc::clone(&plan);
        let parsed_chunks = chunks.into_iter().map(move |chunk| {
            let chunk_len = chunk.len();
            chunk_buf.resize(chunk_len, 0);
            file.seek(SeekFrom::Start(chunk.start as u64))?;
            file.read_exact(&mut chunk_buf)?;
            Self::parse_chunk(&parser, &chunk_buf, Arc::clone(&parse_plan), bytes_per_row)
        });
        self.consume_chunks(parsed_chunks, rows_per_batch, bytes_per_row, plan, consumer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoder::Splitter;
    use crate::ExecutionPlan;

    struct NewlineSplitter;

    impl Splitter for NewlineSplitter {
        fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
            if from == 0 && !bytes.is_empty() {
                return Some(0);
            }
            bytes[from.saturating_sub(1)..]
                .iter()
                .position(|&b| b == b'\n')
                .map(|i| from + i + 1)
                .filter(|&p| p < bytes.len())
        }

        fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
            let newlines = sample.iter().filter(|&&b| b == b'\n').count().max(1);
            (sample.len() / newlines).max(1)
        }
    }

    fn rows(n: usize) -> Vec<u8> {
        "abcdefghij\n".repeat(n).into_bytes()
    }

    #[test]
    fn test_split_cap_limits_chunk_count() {
        let data = rows(10_000);
        let budget = MemoryBudget::new(1024);

        let default_chunks = BoundedExecutor::new(budget)
            .plan_chunks(&data, &NewlineSplitter)
            .0
            .len();
        assert!(
            default_chunks > 2,
            "uncapped run must split: {default_chunks}"
        );

        for cap in [1, 2, 3] {
            let chunks = BoundedExecutor::new(budget)
                .with_split_cap(cap)
                .plan_chunks(&data, &NewlineSplitter)
                .0;
            assert!(
                chunks.len() <= cap.max(1),
                "cap {cap} must bound chunk count, got {}",
                chunks.len()
            );
        }
    }

    #[test]
    fn test_default_split_cap_is_max_split_chunks() {
        assert_eq!(
            BoundedExecutor::new(MemoryBudget::new(1)).split_cap,
            MAX_SPLIT_CHUNKS
        );
        let plan = ExecutionPlan::new().with_max_split_chunks(7);
        assert_eq!(plan.max_split_chunks, Some(7));
        assert_eq!(ExecutionPlan::new().max_split_chunks, None);
    }
}
