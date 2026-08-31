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

pub fn discovery_profile() -> u64 {
    DISCOVERY_NS.load(Ordering::Relaxed)
}
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
) -> FrozenSchema {
    let t0 = std::time::Instant::now();
    // Explicit schema already handled by caller; this is auto-discovery.
    let opts = DiscoveryOpts::default();
    let order: Vec<String> = if (bytes.len() as u64) < opts.full_scan_threshold {
        let mut sink = DiscoverySink::new();
        let _ = parser.parse_chunk_generic(bytes, &mut sink);
        sink.into_order()
    } else {
        use rayon::prelude::*;
        let n = opts.windows;
        let wbytes = opts.window_bytes;
        // Parallelise windows: 16×2 MiB independent parses (~19 ms serial → ~2 ms on 16t)
        let per_window: Vec<Vec<String>> = (0..n)
            .into_par_iter()
            .map(|i| {
                let start = (bytes.len() as u64 * i as u64 / n as u64) as usize;
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
    if order.is_empty() {
        FrozenSchema::from_plan(&[], plan)
    } else {
        FrozenSchema::from_discovered(&order, plan)
    }
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
        return FrozenSchema::from_plan(&names, plan);
    }
    discover_schema(bytes, parser, plan, splitter)
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
        //  - explicit plan.schema_order → from_plan (exact)
        //  - else sampled discovery (16×2 MiB windows for >128 MiB, else full)
        let schema: Option<FrozenSchema> = match opts.schema {
            Some(s) => Some(s),
            None => {
                if !plan.schema_order.is_empty() {
                    let names: Vec<&str> = plan.schema_order.iter().map(|s| s.as_str()).collect();
                    Some(FrozenSchema::from_plan(&names, &plan))
                } else {
                    Some(discover_schema(actual_bytes, &parser, &plan, splitter))
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
                    None => fallback_clone.as_ref().map(|v| v.as_slice()).unwrap_or(&[]),
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
        let mut pending: BTreeMap<usize, RecordBatch> = BTreeMap::new();
        let mut next_seq = 0usize;
        let mut reorder_bytes: usize = 0;
        for (seq, res) in receiver {
            match res {
                Ok(batch) => {
                    if opts.ordered {
                        if seq == next_seq {
                            consumer.consume(batch)?;
                            next_seq += 1;
                            while let Some(b) = pending.remove(&next_seq) {
                                consumer.consume(b)?;
                                next_seq += 1;
                            }
                        } else {
                            reorder_bytes += batch.get_array_memory_size();
                            if reorder_bytes > max_reorder * self.budget.bytes() {
                                return Err(crate::Error::Merge(format!(
                                    "reorder buffer exceeded {} MiB limit (max_reorder={max_reorder})",
                                    self.budget.bytes() / 1024 / 1024
                                )));
                            }
                            pending.insert(seq, batch);
                        }
                    } else {
                        // Unordered: deliver immediately regardless of sequence.
                        consumer.consume(batch)?;
                    }
                }
                Err(e) => return Err(e),
            }
        }
        // Drain remaining.
        if opts.ordered {
            while let Some(b) = pending.remove(&next_seq) {
                consumer.consume(b)?;
                next_seq += 1;
            }
        }
        for h in handles {
            h.join()
                .map_err(|_| crate::Error::Merge("worker panicked".into()))??;
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
                        return Some(Err(crate::Error::Merge(format!(
                            "parallel streaming worker panicked: {msg}"
                        ))));
                    }
                }
                None
            }
        }
    }
}
