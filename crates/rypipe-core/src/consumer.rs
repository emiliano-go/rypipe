use arrow::record_batch::RecordBatch;

use crate::Result;

/// Consumer for streaming `RecordBatch` output.
///
/// The engine calls `consume` once per batch and immediately drops the batch
/// afterward. Peak memory is therefore `budget + batch` rather than
/// `sum(all batches)`. See `BoundedExecutor::run_stream`.
pub trait BatchConsumer {
    fn consume(&mut self, batch: RecordBatch) -> Result<()>;
}

/// Collecting consumer that accumulates batches into a `Vec`.
///
/// Used to implement the legacy `run` / `run_bytes` methods via `run_stream`.
pub struct CollectingConsumer(pub Vec<RecordBatch>);

impl BatchConsumer for CollectingConsumer {
    fn consume(&mut self, batch: RecordBatch) -> Result<()> {
        self.0.push(batch);
        Ok(())
    }
}

/// No-op consumer that drops batches immediately.
///
/// Useful for throughput benchmarks with constant memory.
pub struct DiscardingConsumer;

impl BatchConsumer for DiscardingConsumer {
    fn consume(&mut self, _batch: RecordBatch) -> Result<()> {
        Ok(())
    }
}
