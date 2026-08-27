#[cfg(feature = "mmap")]
use std::fs::File;
#[cfg(feature = "mmap")]
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::Path;

use arrow::record_batch::RecordBatch;

use crate::arrow_export::apply_compare_filter;
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
/// required count exceeds this cap.
const MAX_SPLIT_CHUNKS: usize = 256;

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
        let split_points =
            splitter.find_split_points(bytes, num_batches.min(MAX_SPLIT_CHUNKS));
        let chunks = split_points_to_ranges(&split_points, bytes.len());
        (chunks, rows_per_batch, bytes_per_row)
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
        plan: ExecutionPlan,
    ) -> Result<Vec<RecordBatch>>
    where
        P: RecordParser + Clone + Send + Sync,
    {
        if bytes.is_empty() {
            return Ok(Vec::new());
        }

        let (chunks, rows_per_batch, bytes_per_row) = self.plan_chunks(bytes, splitter);

        let mut batches = Vec::new();
        let mut batch_engine = TableBuilder::with_plan(bytes_per_row.max(64), plan.clone());
        let mut rows_in_batch = 0usize;

        for chunk in &chunks {
            let chunk_bytes = &bytes[chunk.start..chunk.end];
            let mut chunk_engine =
                TableBuilder::with_plan((chunk.len() / 512).max(64), plan.clone());
            parser.validate(chunk_bytes)?;
            parser.parse_chunk(chunk_bytes, &mut chunk_engine)?;

            let chunk_rows = chunk_engine.num_rows();
            batch_engine.extend(chunk_engine)?;
            rows_in_batch += chunk_rows;

            if rows_in_batch >= rows_per_batch {
                batches.push(batch_engine.finish()?);
                batch_engine.reset();
                rows_in_batch = 0;
            }
        }

        if batch_engine.num_rows() > 0 {
            batches.push(batch_engine.finish()?);
        }

        apply_plan_filter(&mut batches, &plan)?;
        Ok(batches)
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
        plan: ExecutionPlan,
        prefault: bool,
    ) -> Result<Vec<RecordBatch>>
    where
        P: RecordParser + Clone + Send + Sync,
    {
        let use_mmap = cfg!(feature = "mmap");
        let input = InputBuffer::open(path, use_mmap, prefault)?;

        #[cfg(feature = "mmap")]
        if matches!(input, InputBuffer::Mmap(_)) {
            return self.run_mapped(path, input, splitter, parser, plan);
        }

        self.run_bytes(input.as_slice(), splitter, parser, plan)
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
        plan: ExecutionPlan,
    ) -> Result<Vec<RecordBatch>>
    where
        P: RecordParser + Clone + Send + Sync,
    {
        let bytes = input.as_slice();
        if bytes.is_empty() {
            return Ok(Vec::new());
        }

        let (chunks, rows_per_batch, bytes_per_row) = self.plan_chunks(bytes, splitter);
        drop(input);

        let mut batches = Vec::new();
        let mut batch_engine = TableBuilder::with_plan(bytes_per_row.max(64), plan.clone());
        let mut rows_in_batch = 0usize;

        let mut file = File::open(path)?;
        for chunk in &chunks {
            let chunk_len = chunk.len();
            let mut chunk_buf = vec![0u8; chunk_len];
            file.seek(SeekFrom::Start(chunk.start as u64))?;
            file.read_exact(&mut chunk_buf)?;

            let mut chunk_engine = TableBuilder::with_plan((chunk_len / 512).max(64), plan.clone());
            parser.validate(&chunk_buf)?;
            parser.parse_chunk(&chunk_buf, &mut chunk_engine)?;

            let chunk_rows = chunk_engine.num_rows();
            batch_engine.extend(chunk_engine)?;
            rows_in_batch += chunk_rows;

            if rows_in_batch >= rows_per_batch {
                batches.push(batch_engine.finish()?);
                batch_engine.reset();
                rows_in_batch = 0;
            }
        }

        if batch_engine.num_rows() > 0 {
            batches.push(batch_engine.finish()?);
        }

        apply_plan_filter(&mut batches, &plan)?;
        Ok(batches)
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
