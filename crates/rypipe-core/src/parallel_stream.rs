//! Parallel streaming executor: multi-core parsing with bounded memory.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread::{self, JoinHandle};

use arrow::record_batch::RecordBatch;

use crate::bounded::MemoryBudget;
use crate::consumer::BatchConsumer;
use crate::decoder::{RecordParser, Splitter};
use crate::engine::TableBuilder;
use crate::input::InputBuffer;
use crate::plan::ExecutionPlan;
use crate::schema::FrozenSchema;
use crate::Result;

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
        plan: ExecutionPlan,
        prefault: bool,
        opts: ParallelStreamOpts,
        consumer: &mut C,
    ) -> Result<()>
    where
        P: RecordParser + Clone + Send + Sync + 'static,
        C: BatchConsumer,
    {
        let input = InputBuffer::open(path, cfg!(feature = "mmap"), prefault)?;
        let bytes = input.as_slice();
        if bytes.is_empty() {
            return Ok(());
        }
        // Use in-memory path for now (mmap already in bytes); file-based
        // seek path would need per-worker file handles.
        self.run_bytes_stream(bytes, splitter, parser, plan, opts, consumer)
    }

    /// Stream bytes in parallel.
    pub fn run_bytes_stream<P, C>(
        &self,
        bytes: &[u8],
        splitter: &dyn Splitter,
        parser: P,
        plan: ExecutionPlan,
        opts: ParallelStreamOpts,
        consumer: &mut C,
    ) -> Result<()>
    where
        P: RecordParser + Clone + Send + Sync + 'static,
        C: BatchConsumer,
    {
        let n = opts.threads.max(1);
        let max_reorder = if opts.max_reorder > 0 {
            opts.max_reorder
        } else {
            n
        };
        let schema = opts.schema;

        // Estimate chunking.
        let bytes_per_row = splitter.estimate_bytes_per_row(&bytes[..bytes.len().min(65536)]).max(1);
        let chunk_size = (self.budget.bytes() / (n * 2)).max(bytes_per_row * 10).max(64 * 1024);
        let num_chunks = (bytes.len() / chunk_size).max(n).min(10000);
        let split_points = splitter.find_split_points(bytes, num_chunks);
        let mut ranges = crate::decoder::split_points_to_ranges(&split_points, bytes.len());
        if ranges.is_empty() {
            ranges.push(0..bytes.len());
        }

        // Assign sequence numbers.
        let chunks_with_seq: Vec<(usize, std::ops::Range<usize>)> =
            ranges.into_iter().enumerate().collect();

        let (sender, receiver) = sync_channel(self.max_in_flight);
        let mut handles: Vec<JoinHandle<Result<()>>> = Vec::with_capacity(n);
        let chunk_queue = std::sync::Arc::new(std::sync::Mutex::new(chunks_with_seq));
        let plan_arc = std::sync::Arc::new(plan);
        let schema_arc = schema.map(std::sync::Arc::new);

        // Pre-compute bytes_per_row for TableBuilder capacity (avoids per-chunk splitter call)
        let est_row = splitter.estimate_bytes_per_row(&bytes[..bytes.len().min(65536)]).max(512);
        for _ in 0..n {
            let queue = std::sync::Arc::clone(&chunk_queue);
            let sender_clone: SyncSender<(usize, Result<RecordBatch>)> = sender.clone();
            let parser_clone = parser.clone();
            let plan_clone = (*plan_arc).clone();
            let schema_clone = schema_arc.clone();
            let bytes_owned = bytes.to_vec(); // TODO: avoid clone for large files, use Arc<[u8]>
            let handle = thread::spawn(move || -> Result<()> {
                loop {
                    let next = {
                        let mut q = queue.lock().unwrap();
                        q.pop()
                    };
                    let Some((seq, range)) = next else { break };
                    let chunk_bytes = &bytes_owned[range.start..range.end];
                    let mut builder =
                        TableBuilder::with_plan((chunk_bytes.len() / est_row).max(64), plan_clone.clone());
                    // If schema is provided, pre-size columns from it.
                    if let Some(ref schema) = schema_clone {
                        builder.ensure_schema(schema)?;
                    }
                    parser_clone.validate(chunk_bytes)?;
                    parser_clone.parse_chunk_generic(chunk_bytes, &mut builder)?;
                    let batch = builder.finish()?;
                    if sender_clone.send((seq, Ok(batch))).is_err() {
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
            h.join().map_err(|_| crate::Error::Merge("worker panicked".into()))??;
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
        plan: ExecutionPlan,
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
            let mut consumer = ChannelConsumer { sender };
            exec.run_stream(&path, &splitter, parser, plan, prefault, opts, &mut consumer)
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
                        return Some(Err(crate::Error::Merge(format!("parallel streaming worker panicked: {msg}"))));
                    }
                }
                None
            }
        }
    }
}
