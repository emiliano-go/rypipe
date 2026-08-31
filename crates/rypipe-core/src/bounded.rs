#[cfg(feature = "mmap")]
use std::fs::File;
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
/// ≈ 800k batches) — still bounded by file scan cost.
const MAX_SPLIT_CHUNKS: usize = 100_000;

/// Parse an input in bounded batches to stay within a memory budget.
pub struct BoundedExecutor {
    budget: MemoryBudget,
}

impl BoundedExecutor {
    pub fn new(budget: MemoryBudget) -> Self {
        Self { budget }
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
        let capped = num_batches.min(MAX_SPLIT_CHUNKS);
        let split_points = splitter.find_split_points(bytes, capped);
        let chunks = split_points_to_ranges(&split_points, bytes.len());
        (chunks, rows_per_batch, bytes_per_row)
    }

    /// Parse an in-memory byte slice in bounded batches, calling `consumer` per batch.
    ///
    /// Chunks are sliced directly from `bytes`; no file I/O occurs. This is
    /// the streaming entry point for adapters holding decompressed data.
    /// Peak memory is `budget + batch`.
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

        let (chunks, rows_per_batch, bytes_per_row) = self.plan_chunks(bytes, splitter);

        let mut batch_engine = TableBuilder::with_plan(bytes_per_row.max(64), Arc::clone(&plan));
        let mut rows_in_batch = 0usize;

        for chunk in &chunks {
            let chunk_bytes = &bytes[chunk.start..chunk.end];
            let mut chunk_engine = TableBuilder::with_plan(
                (chunk.len() / bytes_per_row.max(512)).max(64),
                Arc::clone(&plan),
            );
            parser.validate(chunk_bytes)?;
            parser.parse_chunk_generic(chunk_bytes, &mut chunk_engine)?;

            let chunk_rows = chunk_engine.num_rows();
            batch_engine.extend(chunk_engine)?;
            rows_in_batch += chunk_rows;

            // Dynamic sizing: flush when either row count or actual bytes_used exceeds budget.
            while rows_in_batch >= rows_per_batch
                || batch_engine.bytes_used() >= self.budget.bytes()
            {
                if batch_engine.num_rows() == 0 {
                    break;
                }
                let n = rows_per_batch.min(batch_engine.num_rows());
                // If bytes_used is the trigger, estimate a smaller n to stay under budget
                let n = if batch_engine.bytes_used() >= self.budget.bytes()
                    && batch_engine.num_rows() > 1
                {
                    let est = (batch_engine.num_rows() as f64 * self.budget.bytes() as f64
                        / batch_engine.bytes_used() as f64) as usize;
                    est.clamp(1, n)
                } else {
                    n
                };
                let mut to_consume = batch_engine.split_off(n);
                let mut batch = to_consume.finish()?;
                if let Some(ref filter) = plan.filter {
                    batch = apply_compare_filter(batch, filter)?;
                }
                consumer.consume(batch)?;
                rows_in_batch = rows_in_batch.saturating_sub(n);
            }
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
            return self.run_mapped_stream(path, input, splitter, parser, plan, consumer);
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
        path: &Path,
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

        let (chunks, rows_per_batch, bytes_per_row) = self.plan_chunks(bytes, splitter);
        drop(input);

        let mut batch_engine = TableBuilder::with_plan(bytes_per_row.max(64), Arc::clone(&plan));
        let mut rows_in_batch = 0usize;

        let mut file = File::open(path)?;
        // Reusable buffer sized to the largest chunk to avoid per-chunk alloc.
        let max_chunk = chunks.iter().map(|r| r.len()).max().unwrap_or(0);
        let mut chunk_buf = Vec::with_capacity(max_chunk);

        for chunk in &chunks {
            let chunk_len = chunk.len();
            chunk_buf.resize(chunk_len, 0);
            file.seek(SeekFrom::Start(chunk.start as u64))?;
            file.read_exact(&mut chunk_buf)?;
            let mut chunk_engine = TableBuilder::with_plan(
                (chunk.len() / bytes_per_row.max(512)).max(64),
                Arc::clone(&plan),
            );
            parser.validate(&chunk_buf)?;
            parser.parse_chunk_generic(&chunk_buf, &mut chunk_engine)?;

            let chunk_rows = chunk_engine.num_rows();
            batch_engine.extend(chunk_engine)?;
            rows_in_batch += chunk_rows;

            // Dynamic sizing: flush when either row count or actual bytes_used exceeds budget.
            while rows_in_batch >= rows_per_batch
                || batch_engine.bytes_used() >= self.budget.bytes()
            {
                if batch_engine.num_rows() == 0 {
                    break;
                }
                let n = rows_per_batch.min(batch_engine.num_rows());
                // If bytes_used is the trigger, estimate a smaller n to stay under budget
                let n = if batch_engine.bytes_used() >= self.budget.bytes()
                    && batch_engine.num_rows() > 1
                {
                    let est = (batch_engine.num_rows() as f64 * self.budget.bytes() as f64
                        / batch_engine.bytes_used() as f64) as usize;
                    est.clamp(1, n)
                } else {
                    n
                };
                let mut to_consume = batch_engine.split_off(n);
                let mut batch = to_consume.finish()?;
                if let Some(ref filter) = plan.filter {
                    batch = apply_compare_filter(batch, filter)?;
                }
                consumer.consume(batch)?;
                rows_in_batch = rows_in_batch.saturating_sub(n);
            }
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

    /// Legacy path for mapped inputs: plan against the mapping, drop it, then
    /// read each chunk from the file with `seek` + `read_exact`.
    #[cfg(feature = "mmap")]
    fn run_mapped<P>(
        &self,
        path: &Path,
        input: InputBuffer,
        splitter: &dyn Splitter,
        parser: P,
        plan: Arc<ExecutionPlan>,
    ) -> Result<Vec<RecordBatch>>
    where
        P: RecordParser + Clone + Send + Sync,
    {
        let mut batches = Vec::new();
        let mut consumer = CollectingConsumer(batches);
        self.run_mapped_stream(path, input, splitter, parser, plan, &mut consumer)?;
        Ok(consumer.0)
    }
}

/// Post-assembly safety net: re-apply pure column-comparison plans with
/// Arrow kernels. Per-row evaluation during parse is authoritative, so trees
/// involving `Or`, `Not`, `Equal`, or `NotEqual` pass through untouched.
fn apply_plan_filter(batches: &mut Vec<RecordBatch>, plan: &ExecutionPlan) -> Result<()> {
    if let Some(ref filter) = plan.filter {
        for batch in batches {
            *batch = apply_compare_filter(
                std::mem::replace(batch, RecordBatch::new_empty(batch.schema())),
                filter,
            )?;
        }
    }
    Ok(())
}
