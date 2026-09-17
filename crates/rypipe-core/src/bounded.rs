#[cfg(feature = "mmap")]
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::arrow_export::apply_compare_filter;
use crate::budget::{BudgetLedger, StreamStats};
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
    strict: bool,
}

impl MemoryBudget {
    pub fn new(bytes: usize) -> Self {
        Self {
            bytes,
            strict: false,
        }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Fail when an executor memory check exceeds its allocation allowance.
    /// This does not impose an OS limit on process RSS or adapter-owned memory.
    pub fn with_strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    pub fn is_strict(&self) -> bool {
        self.strict
    }

    pub(crate) fn share(self, count: usize) -> Self {
        Self {
            bytes: self.bytes / count.max(1),
            ..self
        }
    }

    pub(crate) fn check(self, used: usize) -> Result<()> {
        if self.strict && used > self.bytes {
            return Err(crate::Error::Memory {
                used,
                limit: self.bytes,
            });
        }
        Ok(())
    }
}

/// Internal safeguard: never request more than this many split points, so a
/// pathological row-size estimate cannot explode per-chunk overhead. The
/// batch count is otherwise derived from the budget, input size, and
/// estimated bytes per row; batches may still exceed the budget when the
/// required count exceeds this cap. Increased for 64KB streaming (50GB/64KB
/// ≈ 800k batches); still bounded by file scan cost.
pub const MAX_SPLIT_CHUNKS: usize = 100_000;

/// Minimum rows per emitted batch when the memory budget cannot hold a
/// single row. Batches are oversize in that regime regardless (ordinary
/// mode permits this), and per-row batches would let batch count — and
/// consumer cost — grow linearly with row count.
pub const MIN_ROWS_OVERSIZE_BATCH: usize = 64;

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

    /// Payload bytes the executor targets per emitted batch. Exported
    /// batches are exact-sized, so a single-record batch larger than this
    /// is a flagged oversize batch (see [`StreamStats::oversize_batches`]).
    fn batch_target_bytes(&self) -> usize {
        (self.budget.bytes() / 64).max(1)
    }

    /// Derive chunk ranges and batch sizing from an in-memory sample of the
    /// input. Returns `(chunk_ranges, rows_per_batch, bytes_per_row)`.
    fn plan_chunks(
        &self,
        bytes: &[u8],
        splitter: &dyn Splitter,
    ) -> (Vec<Range<usize>>, usize, usize, bool) {
        let bytes_per_row = splitter
            .estimate_bytes_per_row(&bytes[..bytes.len().min(65536)])
            .max(1);
        // Input, growing columns, merge buffers and Arrow output overlap.
        let batch_bytes = self.batch_target_bytes();
        let total_rows_est = bytes.len() / bytes_per_row;
        // When the budget cannot hold even one row, every batch is oversize
        // anyway (permitted in ordinary mode), so per-row batches only
        // explode the batch count — consumers concatenating thousands of
        // single-row batches degrade quadratically. Floor at a minimum
        // batch size in that regime, and report the regime so consume_chunks
        // does not adapt it back down (the capacity-based adaptive estimate
        // is inflated by builder preallocation and collapses to 1).
        let oversize = batch_bytes < bytes_per_row;
        let min_rows = if oversize { MIN_ROWS_OVERSIZE_BATCH } else { 1 };
        let rows_per_batch = (batch_bytes / bytes_per_row)
            .max(min_rows)
            .min(total_rows_est.max(1));

        let num_batches = bytes.len().div_ceil(batch_bytes).max(1);
        let capped = num_batches.min(self.split_cap);
        let split_points = splitter.find_split_points(bytes, capped);
        let chunks = split_points_to_ranges(&split_points, bytes.len());
        (chunks, rows_per_batch, bytes_per_row, oversize)
    }

    fn parse_chunk<P>(
        parser: &P,
        bytes: &[u8],
        plan: Arc<ExecutionPlan>,
        bytes_per_row: usize,
        budget: MemoryBudget,
    ) -> Result<TableBuilder>
    where
        P: RecordParser,
    {
        budget.check(bytes.len())?;
        let mut engine =
            TableBuilder::with_plan((bytes.len() / bytes_per_row.max(512)).max(64), plan);
        engine.set_memory_budget(budget)?;
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
        engine.check_memory_budget()?;
        Ok(engine)
    }

    fn consume_batch<C: crate::consumer::BatchConsumer>(
        &self,
        mut engine: TableBuilder,
        plan: &ExecutionPlan,
        consumer: &mut C,
        ledger: &mut BudgetLedger,
        stats: &mut StreamStats,
    ) -> Result<()> {
        let mut batch = engine.finish()?;
        if let Some(ref filter) = plan.filter {
            batch = apply_compare_filter(batch, filter)?;
        }
        let batch_bytes = batch.get_array_memory_size();
        self.budget.share(4).check(batch_bytes)?;
        if batch.num_rows() == 1 && batch_bytes > self.batch_target_bytes() {
            stats.oversize_batches += 1;
        }
        let rows = batch.num_rows();
        ledger.charge_in_flight(batch_bytes);
        let res = consumer.consume(batch);
        ledger.release_in_flight(batch_bytes);
        res?;
        stats.batches += 1;
        stats.rows += rows;
        Ok(())
    }

    fn consume_chunks<I, C>(
        &self,
        chunks: I,
        rows_per_batch: usize,
        oversize: bool,
        plan: Arc<ExecutionPlan>,
        consumer: &mut C,
        ledger: &mut BudgetLedger,
        stats: &mut StreamStats,
    ) -> Result<()>
    where
        I: IntoIterator<Item = Result<TableBuilder>>,
        C: crate::consumer::BatchConsumer,
    {
        let mut batch_engine = TableBuilder::with_plan(rows_per_batch.min(64), Arc::clone(&plan));
        batch_engine.set_memory_budget(self.budget.share(4))?;
        let mut rows_in_batch = 0usize;
        for chunk_engine in chunks {
            let chunk_engine = chunk_engine?;
            let chunk_rows = chunk_engine.num_rows();
            ledger.set_builders(
                batch_engine
                    .capacity_bytes()
                    .saturating_add(chunk_engine.capacity_bytes()),
            );
            let adaptive = (self.budget.bytes() / 8)
                .saturating_mul(chunk_rows)
                .checked_div(chunk_engine.capacity_bytes().max(1))
                .unwrap_or(1)
                .max(1);
            // Oversize regime (budget cannot hold one input row): every
            // batch is oversize regardless (permitted in ordinary mode), so
            // keep the planned minimum batch size instead of adapting to
            // per-row splits, and do not flush early — accumulation across
            // chunks is what keeps the batch count (and consumer cost)
            // sub-linear.
            let rows_per_batch = if oversize {
                rows_per_batch
            } else {
                rows_per_batch.min(adaptive)
            };
            if !oversize
                && rows_in_batch > 0
                && batch_engine
                    .capacity_bytes()
                    .saturating_add(chunk_engine.capacity_bytes())
                    > self.budget.bytes() / 4
            {
                let batch = batch_engine.split_off(batch_engine.num_rows());
                self.consume_batch(batch, &plan, consumer, ledger, stats)?;
                ledger.set_builders(batch_engine.capacity_bytes());
                rows_in_batch = 0;
            }
            batch_engine.extend(chunk_engine)?;
            batch_engine.check_memory_budget()?;
            ledger.set_builders(batch_engine.capacity_bytes());
            rows_in_batch += chunk_rows;
            while rows_in_batch >= rows_per_batch
                || (!oversize && batch_engine.bytes_used() >= self.budget.bytes())
            {
                if batch_engine.num_rows() == 0 {
                    break;
                }
                let n = rows_per_batch.min(batch_engine.num_rows());
                let n = if !oversize
                    && batch_engine.bytes_used() >= self.budget.bytes()
                    && batch_engine.num_rows() > 1
                {
                    let est = (batch_engine.num_rows() as f64 * self.budget.bytes() as f64
                        / batch_engine.bytes_used() as f64) as usize;
                    est.clamp(1, n)
                } else {
                    n
                };
                self.consume_batch(batch_engine.split_off(n), &plan, consumer, ledger, stats)?;
                ledger.set_builders(batch_engine.capacity_bytes());
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
            self.consume_batch(batch_engine, &plan, consumer, ledger, stats)?;
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
        self.run_bytes_stream_with_stats(bytes, splitter, parser, plan, consumer)
            .map(|_| ())
    }

    /// Like [`BoundedExecutor::run_bytes_stream`], but reports a
    /// [`StreamStats`] summary including the peak tracked engine memory.
    ///
    /// The input slice is caller-owned, so it is not charged to the ledger;
    /// `peak_tracked_bytes` covers builders and in-flight batches only.
    pub fn run_bytes_stream_with_stats<P, C>(
        &self,
        bytes: &[u8],
        splitter: &dyn Splitter,
        parser: P,
        plan: Arc<ExecutionPlan>,
        consumer: &mut C,
    ) -> Result<StreamStats>
    where
        P: RecordParser + Clone + Send + Sync,
        C: crate::consumer::BatchConsumer,
    {
        let mut ledger = BudgetLedger::new(self.budget);
        let mut stats = StreamStats::default();
        self.run_bytes_stream_inner(
            bytes,
            splitter,
            parser,
            plan,
            consumer,
            &mut ledger,
            &mut stats,
        )?;
        stats.peak_tracked_bytes = ledger.peak();
        Ok(stats)
    }

    #[allow(clippy::too_many_arguments)]
    fn run_bytes_stream_inner<P, C>(
        &self,
        bytes: &[u8],
        splitter: &dyn Splitter,
        parser: P,
        plan: Arc<ExecutionPlan>,
        consumer: &mut C,
        ledger: &mut BudgetLedger,
        stats: &mut StreamStats,
    ) -> Result<()>
    where
        P: RecordParser + Clone + Send + Sync,
        C: crate::consumer::BatchConsumer,
    {
        if bytes.is_empty() {
            return Ok(());
        }

        let (mut chunks, rows_per_batch, bytes_per_row, oversize) =
            self.plan_chunks(bytes, splitter);

        // Guard against degenerate splitter output that produces no ranges.
        if chunks.is_empty() {
            chunks.push(0..bytes.len());
        }

        let parse_plan = Arc::clone(&plan);
        let budget = self.budget.share(4);
        budget.check(chunks.capacity() * std::mem::size_of::<Range<usize>>())?;
        let chunks = chunks.into_iter().map(move |chunk| {
            Self::parse_chunk(
                &parser,
                &bytes[chunk.start..chunk.end],
                Arc::clone(&parse_plan),
                bytes_per_row,
                budget,
            )
        });
        self.consume_chunks(chunks, rows_per_batch, oversize, plan, consumer, ledger, stats)
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
        consumer.finish()
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
        self.run_stream_with_stats(path, splitter, parser, plan, prefault, consumer)
            .map(|_| ())
    }

    /// Like [`BoundedExecutor::run_stream`], but reports a [`StreamStats`]
    /// summary including the peak tracked engine memory.
    pub fn run_stream_with_stats<P, C>(
        &self,
        path: &Path,
        splitter: &dyn Splitter,
        parser: P,
        plan: Arc<ExecutionPlan>,
        prefault: bool,
        consumer: &mut C,
    ) -> Result<StreamStats>
    where
        P: RecordParser + Clone + Send + Sync,
        C: crate::consumer::BatchConsumer,
    {
        let mut ledger = BudgetLedger::new(self.budget);
        let mut stats = StreamStats::default();
        let use_mmap = cfg!(feature = "mmap");
        let input = InputBuffer::open(path, use_mmap, prefault)?;

        #[cfg(feature = "mmap")]
        if matches!(input, InputBuffer::Mmap(_)) {
            self.run_mapped_stream(
                input,
                splitter,
                parser,
                plan,
                consumer,
                &mut ledger,
                &mut stats,
            )?;
            stats.peak_tracked_bytes = ledger.peak();
            return Ok(stats);
        }

        self.budget.share(4).check(input.len())?;
        ledger.set_input(input.len());
        self.run_bytes_stream_inner(
            input.as_slice(),
            splitter,
            parser,
            plan,
            consumer,
            &mut ledger,
            &mut stats,
        )?;
        stats.peak_tracked_bytes = ledger.peak();
        Ok(stats)
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
        consumer.finish()
    }

    /// Streaming path for mapped inputs: plan against the mapping, drop it,
    /// then read each chunk with a reusable buffer.
    #[cfg(feature = "mmap")]
    #[allow(clippy::too_many_arguments)]
    fn run_mapped_stream<P, C>(
        &self,
        input: InputBuffer,
        splitter: &dyn Splitter,
        parser: P,
        plan: Arc<ExecutionPlan>,
        consumer: &mut C,
        ledger: &mut BudgetLedger,
        stats: &mut StreamStats,
    ) -> Result<()>
    where
        P: RecordParser + Clone + Send + Sync,
        C: crate::consumer::BatchConsumer,
    {
        let bytes = input.as_slice();
        if bytes.is_empty() {
            return Ok(());
        }

        let (mut chunks, rows_per_batch, bytes_per_row, oversize) =
            self.plan_chunks(bytes, splitter);

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
        let budget = self.budget.share(4);
        budget.check(
            max_chunk.saturating_add(chunks.capacity() * std::mem::size_of::<Range<usize>>()),
        )?;
        let mut chunk_buf = Vec::with_capacity(max_chunk);
        ledger.set_input(chunk_buf.capacity());

        let parse_plan = Arc::clone(&plan);
        let parsed_chunks = chunks.into_iter().map(move |chunk| {
            let chunk_len = chunk.len();
            chunk_buf.resize(chunk_len, 0);
            file.seek(SeekFrom::Start(chunk.start as u64))?;
            file.read_exact(&mut chunk_buf)?;
            Self::parse_chunk(
                &parser,
                &chunk_buf,
                Arc::clone(&parse_plan),
                bytes_per_row,
                budget,
            )
        });
        self.consume_chunks(parsed_chunks, rows_per_batch, oversize, plan, consumer, ledger, stats)
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

    /// Newline-delimited `key=value` rows, for bounded-path tests.
    #[derive(Clone)]
    struct KeyValueParser;

    impl crate::decoder::RecordParser for KeyValueParser {
        fn validate(&self, bytes: &[u8]) -> crate::Result<()> {
            simdutf8::basic::from_utf8(bytes)?;
            Ok(())
        }

        fn parse_chunk(
            &self,
            bytes: &[u8],
            sink: &mut dyn crate::decoder::ColumnarSink,
        ) -> crate::Result<()> {
            let text =
                std::str::from_utf8(bytes).map_err(|e| crate::Error::Plan(e.to_string()))?;
            for line in text.lines() {
                if line.is_empty() {
                    continue;
                }
                sink.begin_row();
                for token in line.split_whitespace() {
                    if let Some((k, v)) = token.split_once('=') {
                        sink.put_field(k, crate::value::Value::Str(std::borrow::Cow::Borrowed(v)));
                    }
                }
                sink.end_row();
            }
            Ok(())
        }
    }

    /// Records start exactly at every multiple of 12, so chunk boundaries
    /// never land mid-record (unlike NewlineSplitter, which is off-by-one
    /// for this purpose).
    struct AlignedSplitter;

    impl Splitter for AlignedSplitter {
        fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
            let start = if from == 0 { 0 } else { ((from + 11) / 12) * 12 };
            (start < bytes.len()).then_some(start)
        }

        fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
            12
        }
    }

    #[test]
    fn tiny_budget_below_row_size_keeps_batch_count_bounded() {
        // Budget (8 bytes) smaller than a single row (~11 bytes): without the
        // oversize floor this emits one batch per row (10_000 batches), which
        // degrades quadratically for consumers concatenating batches.
        // ~12 bytes/row; budget (8 bytes) cannot hold a single row. Without
        // the oversize floor this emits one batch per row (100_000 batches),
        // which degrades quadratically for consumers concatenating batches.
        let data = "a=1 b=2 c=3\n".repeat(100_000).into_bytes();
        let mut consumer = crate::consumer::CollectingConsumer(Vec::new());
        let stats = BoundedExecutor::new(MemoryBudget::new(8))
            .run_bytes_stream_with_stats(
                &data,
                &AlignedSplitter,
                KeyValueParser,
                std::sync::Arc::new(ExecutionPlan::new()),
                &mut consumer,
            )
            .unwrap();
        assert_eq!(stats.rows, 100_000, "all rows must be delivered");
        assert!(
            stats.batches <= 2048,
            "batch count must stay far below row count for pathological              budgets, got {}",
            stats.batches
        );
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
