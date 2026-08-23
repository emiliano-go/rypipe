use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
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

/// Parse a file in bounded batches to stay within a memory budget.
pub struct BoundedExecutor {
    budget: MemoryBudget,
}

impl BoundedExecutor {
    pub fn new(budget: MemoryBudget) -> Self {
        Self { budget }
    }

    /// Parse `path` in batches, returning one `RecordBatch` per batch.
    ///
    /// The caller is responsible for concatenating the batches if a single
    /// table is desired.
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
        let bytes = input.as_slice();
        let file_len = bytes.len();

        let bytes_per_row = splitter.estimate_bytes_per_row(bytes).max(1);
        let total_rows_est = file_len / bytes_per_row;
        let rows_per_batch = (self.budget.bytes() / bytes_per_row)
            .max(1)
            .min(total_rows_est.max(1));

        let num_batches = (total_rows_est / rows_per_batch).max(1);
        let split_points = splitter.find_split_points(bytes, num_batches.min(64));
        let chunks = split_points_to_ranges(&split_points, file_len);
        drop(input);

        if file_len == 0 {
            return Ok(Vec::new());
        }

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
                let batch = batch_engine.finish()?;
                batches.push(batch);
                batch_engine.reset();
                rows_in_batch = 0;
            }
        }

        if batch_engine.num_rows() > 0 {
            let batch = batch_engine.finish()?;
            batches.push(batch);
        }

        if let Some(ref filter) = plan.filter {
            for batch in &mut batches {
                *batch = apply_compare_filter(
                    std::mem::replace(batch, RecordBatch::new_empty(batch.schema())),
                    filter,
                )?;
            }
        }

        Ok(batches)
    }
}
