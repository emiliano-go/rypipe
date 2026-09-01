#[cfg(test)]
use std::borrow::Cow;
use std::path::PathBuf;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use arrow::record_batch::RecordBatch;

use crate::bounded::{BoundedExecutor, MemoryBudget};
use crate::consumer::BatchConsumer;
use crate::decoder::{RecordParser, Splitter};
use crate::plan::ExecutionPlan;
use crate::Result;

/// Channel-based consumer for the streaming iterator.
///
/// Sends `Result<RecordBatch>` via a bounded channel (capacity 1) to provide
/// backpressure — the producer blocks until the consumer calls `next()`.
struct ChannelConsumer {
    sender: SyncSender<Result<RecordBatch>>,
}

impl BatchConsumer for ChannelConsumer {
    fn consume(&mut self, batch: RecordBatch) -> Result<()> {
        // If the receiver is dropped (consumer gone), we stop.
        let _ = self.sender.send(Ok(batch));
        Ok(())
    }
}

/// Streaming iterator that yields `RecordBatch`es one at a time, with
/// constant memory bounded by `budget + batch`.
///
/// Created by `BoundedExecutor::stream` / `Pipeline::stream_batches`.
/// Backed by a worker thread running `BoundedExecutor::run_stream` and a
/// `sync_channel(1)` for backpressure — no accumulation into `Vec`.
pub struct StreamingBatchIterator {
    receiver: Receiver<Result<RecordBatch>>,
    handle: Option<JoinHandle<Result<()>>>,
    done: bool,
}

impl StreamingBatchIterator {
    /// Create a streaming iterator for a file path.
    ///
    /// Spawns a worker thread that runs `BoundedExecutor::run_stream` with a
    /// `ChannelConsumer`. The iterator yields batches as they are produced.
    pub fn new<P, S>(
        path: PathBuf,
        splitter: S,
        parser: P,
        plan: Arc<ExecutionPlan>,
        budget: MemoryBudget,
        prefault: bool,
    ) -> Self
    where
        P: RecordParser + Clone + Send + Sync + 'static,
        S: Splitter + Clone + Send + Sync + 'static,
    {
        let (sender, receiver) = sync_channel(1);
        let handle = thread::spawn(move || {
            let executor = BoundedExecutor::new(budget);
            let mut consumer = ChannelConsumer {
                sender: sender.clone(),
            };
            let res = executor.run_stream(&path, &splitter, parser, plan, prefault, &mut consumer);
            // If run_stream fails, send the error.
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

    /// Create a streaming iterator for in-memory bytes.
    pub fn new_bytes<P, S>(
        bytes: Vec<u8>,
        splitter: S,
        parser: P,
        plan: Arc<ExecutionPlan>,
        budget: MemoryBudget,
    ) -> Self
    where
        P: RecordParser + Clone + Send + Sync + 'static,
        S: Splitter + Clone + Send + Sync + 'static,
    {
        let (sender, receiver) = sync_channel(1);
        let handle = thread::spawn(move || {
            let executor = BoundedExecutor::new(budget);
            let mut consumer = ChannelConsumer {
                sender: sender.clone(),
            };
            let res = executor.run_bytes_stream(&bytes, &splitter, parser, plan, &mut consumer);
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

impl Iterator for StreamingBatchIterator {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.receiver.recv() {
            Ok(res) => Some(res),
            Err(_) => {
                // Channel closed — worker finished. Check for join errors.
                self.done = true;
                if let Some(handle) = self.handle.take() {
                    // Propagate panics as errors.
                    if let Err(payload) = handle.join() {
                        let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                            (*s).to_string()
                        } else if let Some(s) = payload.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "worker panicked".to_string()
                        };
                        return Some(Err(crate::Error::Merge(format!(
                            "streaming worker panicked: {msg}"
                        ))));
                    }
                }
                None
            }
        }
    }
}

impl Drop for StreamingBatchIterator {
    fn drop(&mut self) {
        // Drop the receiver to unblock the worker if it's waiting on send.
        // The worker will then exit when it tries to send.
        self.done = true;
        // Receiver is dropped here; handle's thread will be joined on drop
        // via the Option<JoinHandle> — we don't block in Drop, just let it
        // detach. The OS will clean up.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoder::{ColumnarSink, RecordParser, Splitter};
    use crate::plan::ExecutionPlan;
    use crate::value::Value;
    use crate::Result;

    #[derive(Clone, Debug, Default)]
    struct LineSplitter;
    impl Splitter for LineSplitter {
        fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
            if from >= bytes.len() {
                return None;
            }
            // If we're at a newline, advance past it.
            let start = if bytes[from] == b'\n' { from + 1 } else { from };
            if start >= bytes.len() {
                return None;
            }
            memchr::memchr(b'\n', &bytes[start..]).map(|rel| start + rel + 1)
        }

        fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize> {
            if max_chunks <= 1 || bytes.is_empty() {
                return vec![0, bytes.len()];
            }
            let mut points = vec![0usize];
            for (i, &b) in bytes.iter().enumerate() {
                if b == b'\n' {
                    let next = i + 1;
                    if next > 0 && points.len() < max_chunks {
                        points.push(next);
                    }
                }
            }
            if *points.last().unwrap() != bytes.len() {
                points.push(bytes.len());
            }
            points
        }
        fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
            let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
            (sample.len() / n).max(1)
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
            let text = std::str::from_utf8(bytes).unwrap();
            for line in text.lines() {
                if line.is_empty() {
                    continue;
                }
                sink.begin_row();
                for token in line.split_whitespace() {
                    if let Some((k, v)) = token.split_once('=') {
                        sink.put_field(k, Value::Str(Cow::Borrowed(v)));
                    }
                }
                sink.end_row();
            }
            Ok(())
        }
    }

    #[test]
    fn test_streaming_bytes_single_row_batches() {
        let data = b"A=1 B=2\nA=3 B=4\nA=5 B=6\n";
        let budget = MemoryBudget::new(6);
        let iter = StreamingBatchIterator::new_bytes(
            data.to_vec(),
            LineSplitter,
            LineParser,
            Arc::new(ExecutionPlan::new()),
            budget,
        );
        let batches: Vec<_> = iter.collect::<Result<Vec<_>>>().unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3);
        // With 6B budget and ~6B/row, we expect 1 row per batch
        assert!(batches.len() >= 2);
    }

    #[test]
    fn test_streaming_bytes_vs_bounded() {
        let data = b"A=1 B=2\nA=3 B=4\n";
        let budget = MemoryBudget::new(1024);
        let expected = BoundedExecutor::new(budget)
            .run_bytes(
                data,
                &LineSplitter,
                LineParser,
                Arc::new(ExecutionPlan::new()),
            )
            .unwrap();
        let expected_rows: usize = expected.iter().map(|b| b.num_rows()).sum();

        let iter = StreamingBatchIterator::new_bytes(
            data.to_vec(),
            LineSplitter,
            LineParser,
            Arc::new(ExecutionPlan::new()),
            budget,
        );
        let streamed: Vec<_> = iter.collect::<Result<Vec<_>>>().unwrap();
        let streamed_rows: usize = streamed.iter().map(|b| b.num_rows()).sum();
        assert_eq!(streamed_rows, expected_rows);
    }
}
