//! Single-pass spilling sink for bounded-memory reads.
//!
//! The chunked bounded path (`BoundedExecutor`) parsed each pre-split chunk
//! into its own `TableBuilder`, then merged every chunk into a batch
//! accumulator with `merge::extend` — copying all column buffers a second
//! time. [`SpillSink`] replaces that: the parser walks the whole input once
//! and the sink flushes finished `RecordBatch`es to a consumer whenever the
//! batch target is reached, so memory stays bounded without pre-splitting or
//! merging.

use arrow::record_batch::RecordBatch;

use crate::budget::{BudgetLedger, StreamStats};
use crate::bounded::MemoryBudget;
use crate::consumer::BatchConsumer;
use crate::decoder::ColumnarSink;
use crate::engine::TableBuilder;
use crate::value::Value;
use crate::Result;

/// A `ColumnarSink` wrapping a `TableBuilder` that spills completed batches to
/// a [`BatchConsumer`].
///
/// Every `ColumnarSink` method is forwarded to the inner builder so the
/// parser's fast paths (`put_field_at` / `put_str_at` / `expect_slot`) behave
/// exactly as they would without the wrapper. A missed forward would silently
/// drop data (those trait methods default to dropping), so the wrapper
/// forwards the whole surface.
pub struct SpillSink<'a, C: BatchConsumer> {
    engine: TableBuilder,
    consumer: &'a mut C,
    rows_per_batch: usize,
    batch_target_bytes: usize,
    /// Flush early when the builder capacity reaches this even below
    /// `rows_per_batch`.
    soft_cap_bytes: usize,
    /// Budget cannot hold a single row: batches are oversize regardless, so
    /// never flush early on capacity and keep the batch size floor.
    oversize: bool,
    budget: MemoryBudget,
    ledger: &'a mut BudgetLedger,
    stats: &'a mut StreamStats,
    /// Deferred consumer/export error: `end_row` cannot return a `Result`, so a
    /// failed spill is recorded and surfaced by [`SpillSink::finish_run`].
    error: Option<crate::Error>,
}

impl<'a, C: BatchConsumer> SpillSink<'a, C> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        engine: TableBuilder,
        consumer: &'a mut C,
        rows_per_batch: usize,
        batch_target_bytes: usize,
        soft_cap_bytes: usize,
        oversize: bool,
        budget: MemoryBudget,
        ledger: &'a mut BudgetLedger,
        stats: &'a mut StreamStats,
    ) -> Self {
        Self {
            engine,
            consumer,
            rows_per_batch,
            batch_target_bytes,
            soft_cap_bytes,
            oversize,
            budget,
            ledger,
            stats,
            error: None,
        }
    }

    #[inline]
    fn should_spill(&self) -> bool {
        self.error.is_none()
            && (self.engine.num_rows() >= self.rows_per_batch
                || (!self.oversize && self.engine.capacity_bytes() >= self.soft_cap_bytes))
    }

    /// Flush all accumulated rows as one batch, if any. Errors are deferred.
    fn spill(&mut self) {
        if self.error.is_some() || self.engine.num_rows() == 0 {
            return;
        }
        if let Err(err) = self.spill_inner() {
            self.error = Some(err);
        }
    }

    fn spill_inner(&mut self) -> Result<()> {
        let rows = self.engine.num_rows();
        // `split_off` keeps the schema in the remainder, so successive batches
        // share the columns discovered so far (later columns are united by
        // `CollectingConsumer::finish`).
        let mut part = self.engine.split_off(rows);
        let batch = part.finish()?;
        self.ledger.set_builders(self.engine.capacity_bytes());
        let batch_bytes = batch.get_array_memory_size();
        self.budget.share(4).check(batch_bytes)?;
        if rows == 1 && batch_bytes > self.batch_target_bytes {
            self.stats.oversize_batches += 1;
        }
        self.ledger.charge_in_flight(batch_bytes);
        let res = self.consumer.consume(batch);
        self.ledger.release_in_flight(batch_bytes);
        res?;
        self.stats.batches += 1;
        self.stats.rows += rows;
        Ok(())
    }

    /// Flush the trailing partial batch and surface any deferred error.
    pub fn finish_run(&mut self) -> Result<()> {
        self.spill();
        if let Some(err) = self.error.take() {
            return Err(err);
        }
        if let Some(err) = self.engine.unknown_error.take() {
            return Err(crate::Error::Merge(err));
        }
        if let Some(err) = self.engine.strict_error.take() {
            return Err(err);
        }
        self.engine.check_memory_budget()?;
        self.ledger.set_builders(self.engine.capacity_bytes());
        Ok(())
    }
}

impl<C: BatchConsumer> ColumnarSink for SpillSink<'_, C> {
    #[inline]
    fn begin_row(&mut self) {
        self.engine.begin_row();
    }

    #[inline]
    fn put_field(&mut self, name: &str, value: Value<'_>) {
        self.engine.put_field(name, value);
    }

    #[inline]
    fn end_row(&mut self) {
        self.engine.end_row();
        if self.should_spill() {
            self.spill();
        }
    }

    #[inline]
    fn wants(&self, name: &str) -> bool {
        self.engine.wants(name)
    }

    #[inline]
    fn resolve<'r>(&'r self, name: &'r str) -> Option<&'r str> {
        self.engine.resolve(name)
    }

    #[inline]
    fn put_field_resolved(&mut self, resolved_name: &str, value: Value<'_>) {
        self.engine.put_field_resolved(resolved_name, value);
    }

    #[inline]
    fn put_field_at(&mut self, slot: u32, value: Value<'_>) {
        self.engine.put_field_at(slot, value);
    }

    #[inline]
    fn put_str_at(&mut self, slot: u32, value: &str) {
        self.engine.put_str_at(slot, value);
    }

    #[inline]
    fn resolve_and_put(&mut self, name: &str, value: Value<'_>) {
        self.engine.resolve_and_put(name, value);
    }

    #[inline]
    fn row_rejected(&self) -> bool {
        self.engine.row_rejected()
    }

    #[inline]
    fn should_stop(&self) -> bool {
        // Stop the parser promptly once a spill failed (consumer gone).
        self.error.is_some()
    }

    #[inline]
    fn row_satisfied(&self) -> bool {
        self.engine.row_satisfied()
    }

    #[inline]
    fn wanted_mask(&self) -> u64 {
        self.engine.wanted_mask()
    }

    #[inline]
    fn expect_slot(&self, ordinal: u32) -> Option<(u32, &[u8])> {
        self.engine.expect_slot(ordinal)
    }

    #[inline]
    fn record_slot(&mut self, ordinal: u32, slot: u32, raw_name: &[u8]) {
        self.engine.record_slot(ordinal, slot, raw_name);
    }

    #[inline]
    fn layout_broken(&mut self, ordinal: u32) {
        self.engine.layout_broken(ordinal);
    }

    #[inline]
    fn reset_child_ordinal(&mut self) {
        self.engine.reset_child_ordinal();
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        self.engine.finish()
    }
}
