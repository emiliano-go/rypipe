//! Parallel streaming executor: multi-core parsing with bounded memory.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use arrow::record_batch::RecordBatch;

use std::sync::atomic::{AtomicU64, Ordering};

use crate::bounded::MemoryBudget;
use crate::consumer::BatchConsumer;
use crate::decoder::{RecordParser, Splitter};
use crate::engine::TableBuilder;
use crate::input::InputBuffer;
use crate::plan::ExecutionPlan;
use crate::schema::{DiscoveryOpts, FrozenSchema};
use crate::Result;

static DISCOVERY_NS: AtomicU64 = AtomicU64::new(0);

/// Return the elapsed time (in nanoseconds) spent in schema discovery.
pub fn discovery_profile() -> u64 {
    DISCOVERY_NS.load(Ordering::Relaxed)
}
/// Reset the discovery timer to zero.
pub fn reset_discovery_profile() {
    DISCOVERY_NS.store(0, Ordering::Relaxed);
}

/// Options for parallel streaming.
pub struct ParallelStreamOpts {
    /// Number of worker threads.
    pub threads: usize,
    /// Whether to preserve row order (default: true).
    pub ordered: bool,
    /// Maximum reorder buffer size (default: = threads).
    pub max_reorder: usize,
    /// Explicit schema.  If `Some`, no discovery pass is needed.
    /// Workers pre-size all columns from construction.
    pub schema: Option<FrozenSchema>,
}

impl Default for ParallelStreamOpts {
    fn default() -> Self {
        Self {
            threads: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
            ordered: true,
            max_reorder: 0, // 0 = use threads value
            schema: None,
        }
    }
}

/// Discovery sink that collects raw field names in encounter order.
/// `needs_value=false` keeps the scanner in locate-only mode (no value
/// extraction), but we still need `resolve()` calls, so `needs_resolve`
/// stays true. Capture happens in `resolve()` via interior mutability.
struct DiscoverySink {
    seen: std::cell::RefCell<rustc_hash::FxHashSet<Box<str>>>,
    order: std::cell::RefCell<Vec<String>>,
}

impl DiscoverySink {
    fn new() -> Self {
        Self {
            seen: std::cell::RefCell::new(rustc_hash::FxHashSet::default()),
            order: std::cell::RefCell::new(Vec::new()),
        }
    }
    fn into_order(self) -> Vec<String> {
        self.order.into_inner()
    }
}

impl crate::decoder::ColumnarSink for DiscoverySink {
    #[inline]
    fn begin_row(&mut self) {}
    #[inline]
    fn put_field(&mut self, name: &str, _value: crate::value::Value<'_>) {
        // Full-value fallback (if needs_value were true); keep for completeness.
        let mut seen = self.seen.borrow_mut();
        if seen.insert(Box::from(name)) {
            self.order.borrow_mut().push(name.to_string());
        }
    }
    #[inline]
    fn end_row(&mut self) {}
    #[inline]
    fn wants(&self, _name: &str) -> bool {
        true
    }
    #[inline]
    fn resolve<'a>(&'a self, name: &'a str) -> Option<&'a str> {
        // Locate-only path calls `resolve` without `put_field`; capture here.
        let mut seen = self.seen.borrow_mut();
        if seen.insert(Box::from(name)) {
            self.order.borrow_mut().push(name.to_string());
        }
        Some(name)
    }
    #[inline]
    fn needs_value(&self) -> bool {
        false
    }
    // needs_resolve defaults to true → scanner calls resolve() for each field.
    fn finish(&mut self) -> crate::Result<arrow::record_batch::RecordBatch> {
        Ok(arrow::record_batch::RecordBatch::new_empty(
            std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        ))
    }
}

fn discover_schema<P: crate::decoder::RecordParser>(
    bytes: &[u8],
    parser: &P,
    plan: &crate::plan::ExecutionPlan,
    splitter: &dyn crate::decoder::Splitter,
) -> (FrozenSchema, Vec<String>) {
    let t0 = std::time::Instant::now();
    // Explicit schema already handled by caller; this is auto-discovery.
    let opts = DiscoveryOpts::default();

    let file_size = bytes.len() as u64;

    // Dynamic sizing: scale window count and size by file size
    let n = crate::schema::dynamic_window_count(file_size);
    let wbytes = crate::schema::dynamic_window_size(file_size);

    let order: Vec<String> = if file_size < opts.full_scan_threshold || n == 0 {
        // Small file or dynamic sizing says full scan
        let mut sink = DiscoverySink::new();
        let _ = parser.parse_chunk_generic(bytes, &mut sink);
        sink.into_order()
    } else {
        use rayon::prelude::*;
        // Parallelise windows: n×wbytes independent parses
        let per_window: Vec<Vec<String>> = (0..n)
            .into_par_iter()
            .map(|i| {
                let start = (file_size * i as u64 / n as u64) as usize;
                let end = (start + wbytes).min(bytes.len());
                if start >= end {
                    return Vec::new();
                }
                let mut sink = DiscoverySink::new();
                let slice = &bytes[start..end];
                let _ = parser.parse_chunk_generic(slice, &mut sink);
                sink.into_order()
            })
            .collect();
        // Merge in file order, deduplicating, so global order approximates file order.
        let mut seen = rustc_hash::FxHashSet::<String>::default();
        let mut merged = Vec::new();
        for mut v in per_window {
            for name in v.drain(..) {
                if seen.insert(name.clone()) {
                    merged.push(name);
                }
            }
        }
        // Always scan the tail to catch late-appearing columns
        if opts.always_scan_tail {
            let tail_start = bytes.len().saturating_sub(wbytes);
            let mut tail_sink = DiscoverySink::new();
            let _ = parser.parse_chunk_generic(&bytes[tail_start..], &mut tail_sink);
            for name in tail_sink.into_order() {
                if seen.insert(name.clone()) {
                    merged.push(name);
                }
            }
        }
        if merged.is_empty() && !bytes.is_empty() {
            let mut sink = DiscoverySink::new();
            let _ = parser.parse_chunk_generic(bytes, &mut sink);
            sink.into_order()
        } else {
            merged
        }
    };
    let elapsed = t0.elapsed().as_nanos() as u64;
    DISCOVERY_NS.store(elapsed, Ordering::Relaxed);
    let _ = splitter; // keep splitter in signature for future alignment
    let schema = if order.is_empty() {
        FrozenSchema::from_plan(&[], plan)
    } else {
        FrozenSchema::from_discovered(&order, plan)
    };
    (schema, order)
}

/// Public helper for batch workloads: discover the schema once and reuse.
/// Example:
/// ```ignore
/// let schema = discover_schema_for_path(path, &splitter, &parser, &plan);
/// for f in files { ParallelStreamingBatchIterator::new(..., schema.clone(), ...) }
/// ```
pub fn discover_schema_for_bytes<P: crate::decoder::RecordParser>(
    bytes: &[u8],
    splitter: &dyn crate::decoder::Splitter,
    parser: &P,
    plan: &crate::plan::ExecutionPlan,
) -> FrozenSchema {
    if !plan.schema_order.is_empty() {
        let names: Vec<&str> = plan.schema_order.iter().map(|s| s.as_str()).collect();
        return FrozenSchema::from_partial_plan(&names, plan);
    }
    let opts = DiscoveryOpts::default();
    let sig = crate::schema::layout_signature(bytes, &opts);
    {
        let cache = crate::schema::SCHEMA_CACHE.read().unwrap();
        if let Some(order) = cache.get(&sig) {
            crate::schema::SCHEMA_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
            return FrozenSchema::from_discovered(order, plan);
        }
    }
    crate::schema::SCHEMA_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
    let (schema, order) = discover_schema(bytes, parser, plan, splitter);
    if !order.is_empty() {
        crate::schema::insert_schema_cache(sig, Arc::new(order));
    }
    schema
}

/// Parallel streaming executor: parses chunks concurrently with bounded memory.
pub struct ParallelStreamingExecutor {
    budget: MemoryBudget,
    max_in_flight: usize,
}

impl ParallelStreamingExecutor {
    pub fn new(budget: MemoryBudget, max_in_flight: usize) -> Self {
        Self {
            budget,
            max_in_flight: max_in_flight.max(1),
        }
    }

    /// Stream a file in parallel, calling `consumer` per batch in order.
    #[allow(clippy::too_many_arguments)]
    pub fn run_stream<P, C>(
        &self,
        path: &Path,
        splitter: &dyn Splitter,
        parser: P,
        plan: Arc<ExecutionPlan>,
        prefault: bool,
        opts: ParallelStreamOpts,
        consumer: &mut C,
    ) -> Result<()>
    where
        P: RecordParser + Clone + Send + Sync + 'static,
        C: BatchConsumer,
    {
        let input = InputBuffer::open(path, cfg!(feature = "mmap"), prefault)?;
        if input.is_empty() {
            return Ok(());
        }
        // Share the InputBuffer across workers via Arc to avoid
        // the O(file_size × threads) to_vec() clone.
        let shared = std::sync::Arc::new(input);
        self.run_bytes_stream_shared(shared, splitter, parser, plan, opts, consumer)
    }

    /// Stream from a shared InputBuffer (zero-copy on mmap).
    fn run_bytes_stream_shared<P, C>(
        &self,
        input: std::sync::Arc<InputBuffer>,
        splitter: &dyn Splitter,
        parser: P,
        plan: Arc<ExecutionPlan>,
        opts: ParallelStreamOpts,
        consumer: &mut C,
    ) -> Result<()>
    where
        P: RecordParser + Clone + Send + Sync + 'static,
        C: BatchConsumer,
    {
        // Safety: the Arc keeps the InputBuffer alive for the duration.
        // We pass a dummy &[u8] to satisfy the signature; run_bytes_stream_core
        // ignores it when input is Some and uses input.as_slice() instead.
        let dummy: &[u8] = &[];
        self.run_bytes_stream_core(dummy, Some(input), splitter, parser, plan, opts, consumer)
    }

    /// Stream bytes in parallel.
    pub fn run_bytes_stream<P, C>(
        &self,
        bytes: &[u8],
        splitter: &dyn Splitter,
        parser: P,
        plan: Arc<ExecutionPlan>,
        opts: ParallelStreamOpts,
        consumer: &mut C,
    ) -> Result<()>
    where
        P: RecordParser + Clone + Send + Sync + 'static,
        C: BatchConsumer,
    {
        self.run_bytes_stream_core(bytes, None, splitter, parser, plan, opts, consumer)
    }

    /// Core implementation shared by `run_bytes_stream` and `run_bytes_stream_shared`.
    #[allow(clippy::too_many_arguments)]
    fn run_bytes_stream_core<P, C>(
        &self,
        bytes: &[u8],
        input: Option<std::sync::Arc<InputBuffer>>,
        splitter: &dyn Splitter,
        parser: P,
        plan: Arc<ExecutionPlan>,
        opts: ParallelStreamOpts,
        consumer: &mut C,
    ) -> Result<()>
    where
        P: RecordParser + Clone + Send + Sync + 'static,
        C: BatchConsumer,
    {
        // Prefer the shared InputBuffer's slice over the passed-in bytes.
        let actual_bytes = match input {
            Some(ref inp) => inp.as_slice(),
            None => bytes,
        };
        let n = opts.threads.max(1);
        let max_reorder = if opts.max_reorder > 0 {
            opts.max_reorder
        } else {
            n
        };
        // Frozen schema: gates correct ParquetWriter / StreamWriter usage.
        // Without it, batch 2 can have different column order (FieldG vs Text20
        // last) even when the set is identical, breaking `write_batch`.
        // If opts.schema is None, auto-discover:
        //  - explicit plan.schema_order → from_partial_plan (allows unknown columns)
        //  - else sampled discovery (16×2 MiB windows for >128 MiB, else full)
        let schema: Option<FrozenSchema> = match opts.schema {
            Some(s) => Some(s),
            None => {
                if !plan.schema_order.is_empty() {
                    let names: Vec<&str> = plan.schema_order.iter().map(|s| s.as_str()).collect();
                    Some(FrozenSchema::from_partial_plan(&names, &plan))
                } else {
                    let opts = DiscoveryOpts::default();
                    let sig = crate::schema::layout_signature(actual_bytes, &opts);
                    let cached = crate::schema::SCHEMA_CACHE
                        .read()
                        .unwrap()
                        .get(&sig)
                        .cloned();
                    let (schema, order) = if let Some(order) = cached {
                        crate::schema::SCHEMA_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
                        (FrozenSchema::from_discovered(&order, &plan), order)
                    } else {
                        crate::schema::SCHEMA_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
                        let (schema, order) =
                            discover_schema(actual_bytes, &parser, &plan, splitter);
                        (schema, Arc::new(order))
                    };
                    if !order.is_empty() {
                        crate::schema::insert_schema_cache(sig, order);
                    }
                    Some(schema)
                }
            }
        };

        let bytes_per_row = splitter
            .estimate_bytes_per_row(&actual_bytes[..actual_bytes.len().min(65536)])
            .max(1);
        let chunk_size = (self.budget.bytes() / (n * 2))
            .max(bytes_per_row * 10)
            .max(64 * 1024);
        let num_chunks = (actual_bytes.len() / chunk_size).max(n).min(10000);
        let split_points = splitter.find_split_points(actual_bytes, num_chunks);
        let mut ranges = crate::decoder::split_points_to_ranges(&split_points, actual_bytes.len());
        if ranges.is_empty() {
            ranges.push(0..bytes.len());
        }

        let chunks_with_seq: Vec<(usize, std::ops::Range<usize>)> =
            ranges.into_iter().enumerate().collect();

        let (sender, receiver) = sync_channel(self.max_in_flight);
        let mut handles: Vec<JoinHandle<Result<()>>> = Vec::with_capacity(n);
        let chunk_queue = std::sync::Arc::new(std::sync::Mutex::new(chunks_with_seq));
        let plan_arc = plan;
        let schema_arc = schema.map(std::sync::Arc::new);
        let est_row = splitter
            .estimate_bytes_per_row(&bytes[..bytes.len().min(65536)])
            .max(512);

        // Pre-clone bytes for the fallback path (when input is None).
        let bytes_fallback = if input.is_none() {
            Some(actual_bytes.to_vec())
        } else {
            None
        };
        for _ in 0..n {
            let queue = std::sync::Arc::clone(&chunk_queue);
            let sender_clone: SyncSender<(usize, Result<RecordBatch>)> = sender.clone();
            let parser_clone = parser.clone();
            let plan_clone = std::sync::Arc::clone(&plan_arc);
            let schema_clone = schema_arc.clone();
            let input_clone = input.clone();
            let fallback_clone = bytes_fallback.clone();
            let handle = thread::spawn(move || -> Result<()> {
                // Use shared InputBuffer when available (zero-copy on mmap);
                // fall back to pre-cloned bytes for external callers.
                let bytes_ref: &[u8] = match input_clone {
                    Some(ref inp) => inp.as_slice(),
                    None => fallback_clone.as_deref().unwrap_or(&[]),
                };
                loop {
                    let next = {
                        let mut q = queue.lock().unwrap();
                        q.pop()
                    };
                    let Some((seq, range)) = next else { break };
                    let chunk_bytes = &bytes_ref[range.start..range.end];
                    let mut builder = TableBuilder::with_plan(
                        (chunk_bytes.len() / est_row).max(64),
                        plan_clone.clone(),
                    );
                    // If schema is provided, pre-size columns from it.
                    if let Some(ref schema) = schema_clone {
                        if let Err(e) = builder.ensure_schema(schema) {
                            let _ = sender_clone.send((seq, Err(e)));
                            break;
                        }
                    }
                    if let Err(e) = parser_clone.validate(chunk_bytes) {
                        let _ = sender_clone.send((seq, Err(e)));
                        break;
                    }
                    if let Err(e) = parser_clone.parse_chunk_generic(chunk_bytes, &mut builder) {
                        let _ = sender_clone.send((seq, Err(e)));
                        break;
                    }
                    let res = builder.finish();
                    if sender_clone.send((seq, res)).is_err() {
                        break;
                    }
                }
                Ok(())
            });
            handles.push(handle);
        }
        drop(sender);

        // Coordinator: order by seq and deliver.
        // If `ordered`, buffer out-of-order batches until the next-in-sequence arrives.
        // If unordered, deliver immediately.
        // When the reorder buffer overflows, fall back to unordered delivery.
        let mut pending: BTreeMap<usize, RecordBatch> = BTreeMap::new();
        let mut next_seq = 0usize;
        let mut reorder_bytes: usize = 0;
        let mut fallback_unordered = false;
        let reorder_limit = max_reorder * self.budget.bytes();
        for (seq, res) in receiver {
            match res {
                Ok(batch) => {
                    if opts.ordered && !fallback_unordered {
                        if seq == next_seq {
                            consumer.consume(batch)?;
                            next_seq += 1;
                            while let Some(b) = pending.remove(&next_seq) {
                                consumer.consume(b)?;
                                next_seq += 1;
                            }
                        } else {
                            reorder_bytes += batch.get_array_memory_size();
                            if reorder_bytes > reorder_limit {
                                // Buffer overflow: drain pending in arrival order
                                // (closest to correct), then deliver all remaining
                                // batches unordered.  This avoids a hard error when
                                // the memory budget is too small for the chunk count.
                                for (_, b) in std::mem::take(&mut pending) {
                                    consumer.consume(b)?;
                                }
                                consumer.consume(batch)?;
                                reorder_bytes = 0;
                                fallback_unordered = true;
                            } else {
                                pending.insert(seq, batch);
                            }
                        }
                    } else {
                        // Unordered (either opted out or fell back): deliver immediately.
                        consumer.consume(batch)?;
                    }
                }
                Err(_) => {
                    // Skip failed chunks (logged upstream).
                }
            }
        }
        // Drain remaining.
        if opts.ordered && !fallback_unordered {
            while let Some(b) = pending.remove(&next_seq) {
                consumer.consume(b)?;
                next_seq += 1;
            }
        }
        for h in handles {
            h.join()
                .map_err(|_| crate::Error::Parser(format!(
                    "worker panicked during parallel parse. \
                     This usually indicates a bug in the parser (e.g., returning \
                     Cow::Borrowed that outlives the input chunk, or an unwrap() \
                     on None/Err during parsing). Check your RecordParser::parse_chunk \
                     implementation for incorrect lifetime handling or missing error checks."
                )))??;
        }
        Ok(())
    }
}

/// Iterator wrapper for parallel streaming (pull-based).
pub struct ParallelStreamingBatchIterator {
    receiver: Receiver<Result<RecordBatch>>,
    handle: Option<JoinHandle<Result<()>>>,
    done: bool,
}

impl ParallelStreamingBatchIterator {
    pub fn new<P, S>(
        path: PathBuf,
        splitter: S,
        parser: P,
        plan: Arc<ExecutionPlan>,
        budget: MemoryBudget,
        prefault: bool,
        opts: ParallelStreamOpts,
    ) -> Self
    where
        P: RecordParser + Clone + Send + Sync + 'static,
        S: Splitter + Clone + Send + Sync + 'static,
    {
        let num_threads = opts.threads;
        let max_in_flight = 2 * num_threads;
        let (sender, receiver) = sync_channel(max_in_flight);
        let handle = thread::spawn(move || {
            let exec = ParallelStreamingExecutor::new(budget, max_in_flight);
            let mut consumer = ChannelConsumer {
                sender: sender.clone(),
            };
            let res = exec.run_stream(
                &path,
                &splitter,
                parser,
                plan,
                prefault,
                opts,
                &mut consumer,
            );
            if let Err(e) = res {
                let _ = sender.send(Err(e));
            }
            Ok(())
        });
        Self {
            receiver,
            handle: Some(handle),
            done: false,
        }
    }
}

struct ChannelConsumer {
    sender: SyncSender<Result<RecordBatch>>,
}
impl BatchConsumer for ChannelConsumer {
    fn consume(&mut self, batch: RecordBatch) -> Result<()> {
        let _ = self.sender.send(Ok(batch));
        Ok(())
    }
}

impl Iterator for ParallelStreamingBatchIterator {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        // This is a simplified version that just receives in order as sent,
        // not handling seq ordering for now. For true ordered, need BTreeMap logic as in run_stream.
        // For first version, we will just receive and yield.
        match self.receiver.recv() {
            Ok(Ok(batch)) => Some(Ok(batch)),
            Ok(Err(e)) => {
                self.done = true;
                Some(Err(e))
            }
            Err(_) => {
                self.done = true;
                if let Some(h) = self.handle.take() {
                    if let Err(p) = h.join() {
                        let msg = if let Some(s) = p.downcast_ref::<&str>() {
                            (*s).to_string()
                        } else if let Some(s) = p.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "worker panicked".to_string()
                        };
                        return Some(Err(crate::Error::Parser(format!(
                            "parallel streaming worker panicked: {msg}. \
                             This usually indicates a bug in the parser (e.g., returning \
                             Cow::Borrowed that outlives the input chunk, or an unwrap() \
                             on None/Err during parsing). Check your RecordParser::parse_chunk \
                             implementation for incorrect lifetime handling or missing error checks."
                        ))));
                    }
                }
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;
    use crate::decoder::{ColumnarSink, RecordParser, Splitter};
    use crate::plan::ExecutionPlan;
    use crate::schema::{clear_schema_cache, schema_cache_stats};
    use crate::value::Value;
    use arrow::array::Array;

    /// Minimal parser: newline-separated rows of `key=value\t...` tokens.
    #[derive(Clone, Debug, Default)]
    struct NameValueParser;

    impl RecordParser for NameValueParser {
        fn validate(&self, _bytes: &[u8]) -> Result<()> {
            Ok(())
        }

        fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
            let text = std::str::from_utf8(bytes).map_err(|e| crate::Error::Plan(e.to_string()))?;
            for line in text.lines() {
                if line.is_empty() {
                    continue;
                }
                sink.begin_row();
                for token in line.split('\t') {
                    if let Some((k, v)) = token.split_once('=') {
                        sink.resolve_and_put(k, Value::Str(Cow::Borrowed(v)));
                    }
                }
                sink.end_row();
            }
            Ok(())
        }
    }

    /// Trivial splitter: one chunk covering the whole input.
    struct NullSplitter;

    impl Splitter for NullSplitter {
        fn next_record_start(&self, _bytes: &[u8], _from: usize) -> Option<usize> {
            None
        }

        fn find_split_points(&self, bytes: &[u8], _max_chunks: usize) -> Vec<usize> {
            vec![0, bytes.len()]
        }

        fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
            let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
            sample.len() / n
        }
    }

    fn names(schema: &FrozenSchema) -> Vec<&str> {
        schema.column_names().iter().map(|s| s.as_ref()).collect()
    }

    #[test]
    fn schema_cache_hits_misses_and_plan_changes() {
        clear_schema_cache();
        let splitter = NullSplitter;
        let parser = NameValueParser;

        // First layout: miss.
        let bytes = b"a=1\tb=2\na=3\tb=4\n";
        let plan = ExecutionPlan::new();
        let s1 = discover_schema_for_bytes(bytes, &splitter, &parser, &plan);
        assert_eq!(names(&s1), vec!["a", "b"]);
        assert_eq!(schema_cache_stats(), (0, 1));

        // Same layout: hit.
        let s2 = discover_schema_for_bytes(bytes, &splitter, &parser, &plan);
        assert_eq!(names(&s2), vec!["a", "b"]);
        assert_eq!(schema_cache_stats(), (1, 1));

        // Different layout: new miss.
        let bytes2 = b"c=1\td=2\n";
        let s3 = discover_schema_for_bytes(bytes2, &splitter, &parser, &plan);
        assert_eq!(names(&s3), vec!["c", "d"]);
        assert_eq!(schema_cache_stats(), (1, 2));

        // Same layout as the first parse but with a different plan:
        // cache hit, but the applied schema differs.
        let mut renamed = ExecutionPlan::new();
        renamed.field_map.insert("a".to_string(), "x".to_string());
        let s4 = discover_schema_for_bytes(bytes, &splitter, &parser, &renamed);
        assert_eq!(names(&s4), vec!["x", "b"]);
        assert_eq!(schema_cache_stats(), (2, 2));
    }

    #[test]
    fn tail_scan_catches_late_columns() {
        // Create data where column "late" only appears in the last few rows.
        // With 16 windows of 2 MiB each, the last window starts at ~94%.
        // Column "late" appears only in the last ~5% of the data.
        let mut data = Vec::new();
        // First 90%: only columns "a" and "b"
        for i in 0..900 {
            data.extend_from_slice(format!("a={i}\tb=val{i}\n").as_bytes());
        }
        // Last 10%: introduce column "late"
        for i in 900..1000 {
            data.extend_from_slice(format!("a={i}\tb=val{i}\tlate=new{i}\n").as_bytes());
        }

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();
        let schema = discover_schema_for_bytes(&data, &splitter, &parser, &plan);

        // Column "late" must be discovered
        let cols = names(&schema);
        assert!(cols.contains(&"a"), "column 'a' missing");
        assert!(cols.contains(&"b"), "column 'b' missing");
        assert!(cols.contains(&"late"), "column 'late' missing from tail scan");

        // Verify all values are preserved by parsing the full data
        let mut builder = crate::engine::TableBuilder::with_plan(1024, Arc::new(plan.clone()));
        builder.ensure_schema(&schema).unwrap();
        parser.parse_chunk(&data, &mut builder).unwrap();
        let batch = builder.finish().unwrap();

        assert_eq!(batch.num_rows(), 1000);
        assert_eq!(batch.num_columns(), 3);

        // Check that the "late" column has values in the last 100 rows
        let late_col = batch.column_by_name("late").expect("late column missing");
        let late_arr = late_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("late column not string array");
        // First 900 rows should be null for "late"
        for i in 0..900 {
            assert!(
                late_arr.is_null(i),
                "row {i}: expected null for 'late', got {:?}",
                late_arr.value(i)
            );
        }
        // Last 100 rows should have values
        for i in 900..1000 {
            assert!(
                !late_arr.is_null(i),
                "row {i}: expected value for 'late', got null"
            );
            assert!(
                late_arr.value(i).starts_with("new"),
                "row {i}: expected 'new{i}', got {:?}",
                late_arr.value(i)
            );
        }
    }

    #[test]
    fn all_columns_discovered_no_data_loss() {
        // Test that all columns from a file are discovered, even with sparse data
        let mut data = Vec::new();
        // Mix of columns across rows
        let rows: Vec<&[u8]> = vec![
            b"a=1\tb=2\tc=3\n",
            b"b=5\td=6\n",
            b"a=7\tc=8\te=9\n",
            b"f=10\n",
            b"a=11\tb=12\tc=13\td=14\te=15\tf=16\n",
        ];
        for row in &rows {
            data.extend_from_slice(row);
        }

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();
        let schema = discover_schema_for_bytes(&data, &splitter, &parser, &plan);

        // All 6 columns must be discovered
        let cols = names(&schema);
        assert_eq!(cols.len(), 6, "expected 6 columns, got {:?}", cols);
        assert!(cols.contains(&"a"));
        assert!(cols.contains(&"b"));
        assert!(cols.contains(&"c"));
        assert!(cols.contains(&"d"));
        assert!(cols.contains(&"e"));
        assert!(cols.contains(&"f"));

        // Parse all data and verify values
        let mut builder = crate::engine::TableBuilder::with_plan(1024, Arc::new(plan.clone()));
        builder.ensure_schema(&schema).unwrap();
        parser.parse_chunk(&data, &mut builder).unwrap();
        let batch = builder.finish().unwrap();

        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 6);

        // Verify specific values
        let a_col = batch.column_by_name("a").unwrap();
        let a_arr = a_col.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(a_arr.value(0), "1");
        assert_eq!(a_arr.value(2), "7");
        assert_eq!(a_arr.value(4), "11");
    }

    #[test]
    fn dynamic_sizing_discovers_same_columns() {
        // Verify that dynamic window sizing discovers the same columns
        // as the fixed 16-window approach
        let mut data = Vec::new();
        // Create data with columns spread across the file
        for i in 0..1000 {
            if i < 100 {
                // First 10%: columns a, b
                data.extend_from_slice(format!("a={i}\tb={i}\n").as_bytes());
            } else if i < 500 {
                // Middle 40%: columns a, b, c
                data.extend_from_slice(format!("a={i}\tb={i}\tc={i}\n").as_bytes());
            } else if i < 900 {
                // Next 40%: columns a, b, c, d
                data.extend_from_slice(format!("a={i}\tb={i}\tc={i}\td={i}\n").as_bytes());
            } else {
                // Last 10%: columns a, b, c, d, e (late column)
                data.extend_from_slice(format!("a={i}\tb={i}\tc={i}\td={i}\te={i}\n").as_bytes());
            }
        }

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();
        let schema = discover_schema_for_bytes(&data, &splitter, &parser, &plan);

        // All 5 columns must be discovered
        let cols = names(&schema);
        assert_eq!(cols.len(), 5, "expected 5 columns, got {:?}", cols);
        assert!(cols.contains(&"a"));
        assert!(cols.contains(&"b"));
        assert!(cols.contains(&"c"));
        assert!(cols.contains(&"d"));
        assert!(cols.contains(&"e"));

        // Parse and verify
        let mut builder = crate::engine::TableBuilder::with_plan(1024, Arc::new(plan.clone()));
        builder.ensure_schema(&schema).unwrap();
        parser.parse_chunk(&data, &mut builder).unwrap();
        let batch = builder.finish().unwrap();

        assert_eq!(batch.num_rows(), 1000);
        assert_eq!(batch.num_columns(), 5);
    }

    #[test]
    fn column_order_consistent_across_file_sizes() {
        // Verify that column order is consistent regardless of file size
        // This ensures no data reordering issues
        let make_data = |n: usize| -> Vec<u8> {
            let mut data = Vec::new();
            for i in 0..n {
                data.extend_from_slice(format!("x={i}\ty={i}\tz={i}\n").as_bytes());
            }
            data
        };

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();

        // Test with different file sizes
        let sizes = vec![10, 100, 1000];
        let mut first_order: Option<Vec<String>> = None;

        for size in sizes {
            let data = make_data(size);
            let schema = discover_schema_for_bytes(&data, &splitter, &parser, &plan);
            let cols: Vec<String> = schema.column_names().iter().map(|s| s.to_string()).collect();

            if let Some(ref order) = first_order {
                assert_eq!(
                    &cols, order,
                    "column order changed for size {size}: {cols:?} vs {order:?}"
                );
            } else {
                first_order = Some(cols);
            }
        }
    }

    #[test]
    fn esoteric_sparse_schema_one_column_per_row() {
        // Each row has a unique column name - extremely sparse
        let mut data = Vec::new();
        for i in 0..100 {
            data.extend_from_slice(format!("col_{i}={i}\n").as_bytes());
        }

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();
        let schema = discover_schema_for_bytes(&data, &splitter, &parser, &plan);

        // All 100 columns should be discovered
        let cols = names(&schema);
        assert_eq!(cols.len(), 100, "expected 100 columns, got {:?}", cols.len());
    }

    #[test]
    fn esoteric_empty_rows() {
        // File with many empty rows interspersed
        let mut data = Vec::new();
        for i in 0..50 {
            data.extend_from_slice(format!("a={i}\n").as_bytes());
            data.extend_from_slice(b"\n"); // empty row
            data.extend_from_slice(b"\n"); // another empty row
            data.extend_from_slice(format!("b={i}\n").as_bytes());
        }

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();
        let schema = discover_schema_for_bytes(&data, &splitter, &parser, &plan);

        let cols = names(&schema);
        assert!(cols.contains(&"a"), "column 'a' missing");
        assert!(cols.contains(&"b"), "column 'b' missing");
    }

    #[test]
    fn esoteric_single_character_columns() {
        // Very short column names
        let data = b"a=1\tb=2\tc=3\nd=4\te=5\n";

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();
        let schema = discover_schema_for_bytes(data, &splitter, &parser, &plan);

        let cols = names(&schema);
        assert_eq!(cols.len(), 5);
        assert!(cols.contains(&"a"));
        assert!(cols.contains(&"e"));
    }

    #[test]
    fn esoteric_unicode_column_names() {
        // Unicode characters in column names
        let data = "名前=Alice\t年齢=30\n名前=Bob\t年齢=25\n".as_bytes();

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();
        let schema = discover_schema_for_bytes(data, &splitter, &parser, &plan);

        let cols = names(&schema);
        assert_eq!(cols.len(), 2);
        assert!(cols.contains(&"名前"));
        assert!(cols.contains(&"年齢"));
    }

    #[test]
    fn esoteric_very_long_column_names() {
        // Very long column names (100+ chars)
        let long_name = "a".repeat(200);
        let data = format!("{long_name}=1\tb=2\n{long_name}=3\tc=4\n");

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();
        let schema = discover_schema_for_bytes(data.as_bytes(), &splitter, &parser, &plan);

        let cols = names(&schema);
        assert_eq!(cols.len(), 3);
        assert!(cols.contains(&long_name.as_str()));
    }

    #[test]
    fn esoteric_duplicate_columns_in_row() {
        // Same column appears multiple times in a row
        let data = b"a=1\ta=2\ta=3\tb=4\n";

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();
        let schema = discover_schema_for_bytes(data, &splitter, &parser, &plan);

        let cols = names(&schema);
        assert_eq!(cols.len(), 2, "expected 2 unique columns, got {:?}", cols);
        assert!(cols.contains(&"a"));
        assert!(cols.contains(&"b"));
    }
}
