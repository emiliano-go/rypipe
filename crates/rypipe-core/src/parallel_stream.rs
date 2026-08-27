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
use crate::Result;

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
        num_threads: usize,
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
        self.run_bytes_stream(bytes, splitter, parser, plan, num_threads, consumer)
    }

    /// Stream bytes in parallel.
    pub fn run_bytes_stream<P, C>(
        &self,
        bytes: &[u8],
        splitter: &dyn Splitter,
        parser: P,
        plan: ExecutionPlan,
        num_threads: usize,
        consumer: &mut C,
    ) -> Result<()>
    where
        P: RecordParser + Clone + Send + Sync + 'static,
        C: BatchConsumer,
    {
        let n = num_threads.max(1);
        // Estimate chunking: use budget to limit in-flight, but create enough chunks for parallelism.
        // For 64KB budget, bytes_per_row ~1100, rows_per_batch ~59, total_rows ~ bytes/1100.
        // We create chunks sized to budget/(in_flight*2) to keep memory bounded.
        let bytes_per_row = splitter.estimate_bytes_per_row(&bytes[..bytes.len().min(65536)]).max(1);
        let total_rows = bytes.len() / bytes_per_row;
        let chunk_size = (self.budget.bytes() / (n * 2)).max(bytes_per_row * 10).max(64 * 1024);
        let num_chunks = (bytes.len() / chunk_size).max(n).min(10000);
        let split_points = splitter.find_split_points(bytes, num_chunks);
        let mut ranges = crate::decoder::split_points_to_ranges(&split_points, bytes.len());
        if ranges.is_empty() {
            ranges.push(0..bytes.len());
        }

        // Assign sequence numbers
        let chunks_with_seq: Vec<(usize, std::ops::Range<usize>)> =
            ranges.into_iter().enumerate().collect();

        let (sender, receiver) = sync_channel(self.max_in_flight);
        let mut handles: Vec<JoinHandle<Result<()>>> = Vec::with_capacity(n);
        let chunk_queue = std::sync::Arc::new(std::sync::Mutex::new(chunks_with_seq));
        let plan_arc = std::sync::Arc::new(plan);

        for _ in 0..n {
            let queue = std::sync::Arc::clone(&chunk_queue);
            let sender_clone: SyncSender<(usize, Result<RecordBatch>)> = sender.clone();
            let splitter_clone = {
                // Splitter is not Clone in trait, but we can box it? For now, require Clone.
                // Workaround: use a dummy splitter that splits on fixed size if not cloneable.
                // Instead, we clone via a closure: we need S: Clone, but &dyn Splitter is not Clone.
                // For this initial version, we will split chunks upfront and workers just get Range + bytes slice.
                // So no need for splitter in worker.
                ()
            };
            let parser_clone = parser.clone();
            let plan_clone = (*plan_arc).clone();
            let bytes_owned = bytes.to_vec(); // TODO: avoid clone for large files, use Arc<[u8]>
            let handle = thread::spawn(move || -> Result<()> {
                loop {
                    let next = {
                        let mut q = queue.lock().unwrap();
                        q.pop()
                    };
                    let Some((seq, range)) = next else { break };
                    let chunk_bytes = &bytes_owned[range.start..range.end];
                    let mut builder = TableBuilder::with_plan((chunk_bytes.len() / 512).max(64), plan_clone.clone());
                    parser_clone.validate(chunk_bytes)?;
                    parser_clone.parse_chunk(chunk_bytes, &mut builder)?;
                    let batch = builder.finish()?;
                    // Send with backpressure; if receiver gone, exit
                    if sender_clone.send((seq, Ok(batch))).is_err() {
                        break;
                    }
                }
                Ok(())
            });
            handles.push(handle);
        }
        drop(sender);

        // Coordinator: order by seq and deliver
        let mut pending: BTreeMap<usize, RecordBatch> = BTreeMap::new();
        let mut next_seq = 0usize;
        // We don't know total chunks count here without recomputing, but we can receive until channel closed
        for (seq, res) in receiver {
            match res {
                Ok(batch) => {
                    if seq == next_seq {
                        consumer.consume(batch)?;
                        next_seq += 1;
                        while let Some(b) = pending.remove(&next_seq) {
                            consumer.consume(b)?;
                            next_seq += 1;
                        }
                    } else {
                        pending.insert(seq, batch);
                    }
                }
                Err(e) => return Err(e),
            }
        }
        // Drain any remaining pending (should be none if all chunks processed in order, but handle)
        while let Some(b) = pending.remove(&next_seq) {
            consumer.consume(b)?;
            next_seq += 1;
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
    pending: BTreeMap<usize, RecordBatch>,
    next_seq: usize,
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
        num_threads: usize,
    ) -> Self
    where
        P: RecordParser + Clone + Send + Sync + 'static,
        S: Splitter + Clone + Send + Sync + 'static,
    {
        let (sender, receiver) = sync_channel(2 * num_threads);
        let handle = thread::spawn(move || {
            let exec = ParallelStreamingExecutor::new(budget, 2 * num_threads);
            let mut consumer = ChannelConsumer { sender };
            exec.run_stream(&path, &splitter, parser, plan, prefault, num_threads, &mut consumer)
        });
        Self {
            receiver,
            handle: Some(handle),
            pending: BTreeMap::new(),
            next_seq: 0,
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
