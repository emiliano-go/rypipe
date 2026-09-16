//! Memory-budget partitioning, accounting, and streaming-run statistics.
//!
//! [`BudgetPartition`] derives per-component limits from a [`MemoryBudget`]
//! so that every simultaneously live, engine-controlled allocation stays
//! within it; the unit tests prove the worst case. [`BudgetLedger`] charges
//! the actual (capacity-accurate) live bytes at stage boundaries and tracks
//! the peak. [`StreamStats`] summarizes a streaming run, including that peak.
//!
//! `BoundedExecutor` uses the ledger observationally: it records charges and
//! reports the peak through [`StreamStats`] without changing sizing or
//! enforcement. [`BudgetLedger::check`] is the enforcement backstop for
//! adopters that want a hard error once live bytes exceed the budget.
//!
//! Engine-controlled memory: input chunk buffers, column builders, the batch
//! accumulator, exported Arrow batches in flight, and the parallel reorder
//! buffer. Out of scope: process baseline, caller-owned input slices, memory
//! the consumer retains after `consume` returns, Python objects, and
//! allocator retention (freed bytes the allocator keeps mapped).

use crate::bounded::MemoryBudget;

/// Per-component limits derived from a [`MemoryBudget`] so that every
/// simultaneously live, engine-controlled allocation stays within it.
///
/// This is the documented envelope behind the executors' sizing heuristics:
/// `BoundedExecutor` targets `budget / 64` batches (well under
/// `batch_target`) and `ParallelStreamingExecutor` derives per-worker chunks
/// of `budget / (32 × threads)` (well under the parallel partition's
/// per-worker chunk). The fractions below are the bounds those heuristics
/// may grow into, not the values they target.
///
/// Serial streaming keeps these components live at peak:
///
/// | component | bound |
/// |---|---|
/// | input chunk buffer | `max_chunk_bytes` |
/// | one chunk builder (doubling growth) | `2 × max_chunk_bytes` |
/// | batch accumulator (exact-capacity storage) | `2 × batch_target` |
/// | exported batches in flight (channel + consumer-held) | `2 × batch_target` |
///
/// The fractions sum to 11/16 of the budget; the remainder covers row
/// buffers, schema maps, and other small per-builder state, and provides
/// slack for estimate error.
#[derive(Clone, Copy, Debug)]
pub struct BudgetPartition {
    /// Total budget in bytes.
    pub budget: usize,
    /// Payload bytes at which the batch accumulator is flushed. Exported
    /// batches are exact-sized, so each in-flight batch is ≤ this value
    /// unless it is a flagged oversize batch.
    pub batch_target: usize,
    /// Maximum bytes of one input chunk; also bounds the reusable chunk
    /// buffer on the seek-based path.
    pub max_chunk_bytes: usize,
    /// Finished batches allowed in flight (channel slot + consumer-held).
    pub max_in_flight: usize,
    /// Parallel reorder-buffer limit in bytes (0 for the serial partition).
    pub reorder_limit: usize,
}

impl BudgetPartition {
    /// Partition for the serial streaming executor.
    pub fn serial(budget: MemoryBudget) -> Self {
        let b = budget.bytes();
        Self {
            budget: b,
            batch_target: (b / 8).max(1),
            max_chunk_bytes: (b / 16).max(1),
            max_in_flight: 2,
            reorder_limit: 0,
        }
    }

    /// Partition for the parallel streaming executor with `threads` workers.
    ///
    /// Live at peak: `threads` chunk builders (`2 × chunk` each), up to
    /// `2 × threads` finished batches (`chunk` each), and the ordered-mode
    /// reorder buffer (`budget / 2`). Per-worker chunk payload is therefore
    /// `budget / (8 × threads)`.
    pub fn parallel(budget: MemoryBudget, threads: usize) -> Self {
        let b = budget.bytes();
        let n = threads.max(1);
        let chunk = (b / (8 * n)).max(1);
        Self {
            budget: b,
            batch_target: chunk,
            max_chunk_bytes: chunk,
            max_in_flight: 2 * n,
            reorder_limit: (b / 2).max(1),
        }
    }
}

/// Fixed structural overhead tolerated by the ledger check: empty-builder
/// preallocation hints, column metadata, and schema maps. Independent of
/// data size; negligible against realistic budgets (≥ 1 MiB) but dominant
/// for toy budgets used in unit tests.
pub const STRUCTURAL_ALLOWANCE: usize = 256 * 1024;

/// Running account of engine-controlled live bytes, charged at stage
/// boundaries with capacity-accurate values.
#[derive(Debug, Default)]
pub struct BudgetLedger {
    budget: usize,
    input: usize,
    builders: usize,
    in_flight: usize,
    reorder: usize,
    peak: usize,
}

impl BudgetLedger {
    pub fn new(budget: MemoryBudget) -> Self {
        Self {
            budget: budget.bytes(),
            ..Self::default()
        }
    }

    fn live(&self) -> usize {
        self.input + self.builders + self.in_flight + self.reorder
    }

    fn note(&mut self) {
        self.peak = self.peak.max(self.live());
    }

    /// Set the input-buffer charge (chunk buffer or engine-owned input Vec).
    pub fn set_input(&mut self, bytes: usize) {
        self.input = bytes;
        self.note();
    }

    /// Set the builders charge to the current combined capacity of the live
    /// accumulator and chunk builder.
    pub fn set_builders(&mut self, bytes: usize) {
        self.builders = bytes;
        self.note();
    }

    pub fn charge_in_flight(&mut self, bytes: usize) {
        self.in_flight += bytes;
        self.note();
    }

    pub fn release_in_flight(&mut self, bytes: usize) {
        self.in_flight = self.in_flight.saturating_sub(bytes);
    }

    pub fn charge_reorder(&mut self, bytes: usize) {
        self.reorder += bytes;
        self.note();
    }

    pub fn release_reorder(&mut self, bytes: usize) {
        self.reorder = self.reorder.saturating_sub(bytes);
    }

    /// Peak simultaneously live engine-controlled bytes observed so far.
    pub fn peak(&self) -> usize {
        self.peak
    }

    /// Enforcement backstop: error if live bytes exceed the budget by more
    /// than `allowance` (e.g. [`STRUCTURAL_ALLOWANCE`], tolerated for
    /// flagged oversize batches). Budget-derived sizing should keep live
    /// bytes under the budget; exceeding it without an oversize record
    /// indicates an accounting bug.
    pub fn check(&self, allowance: usize) -> crate::Result<()> {
        let live = self.live();
        let limit = self.budget.saturating_add(allowance);
        if live > limit {
            return Err(crate::Error::Memory { used: live, limit });
        }
        Ok(())
    }
}

/// Outcome of a streaming run, including memory-budget accounting data.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamStats {
    /// Batches delivered to the consumer.
    pub batches: usize,
    /// Total rows across all batches.
    pub rows: usize,
    /// Batches emitted alone because a single record exceeded the batch
    /// target. These may exceed the budget; all other batches may not.
    pub oversize_batches: usize,
    /// Peak simultaneously live engine-controlled bytes (capacity-accurate).
    pub peak_tracked_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serial_partition_sums_within_budget() {
        let budget = MemoryBudget::new(10 * 1024 * 1024);
        let p = BudgetPartition::serial(budget);
        let worst = p.max_chunk_bytes          // chunk buffer
            + 2 * p.max_chunk_bytes            // chunk builder (doubling)
            + 2 * p.batch_target               // accumulator
            + 2 * p.batch_target; // in flight
        assert!(
            worst <= budget.bytes(),
            "serial worst case {worst} exceeds {}",
            budget.bytes()
        );
    }

    #[test]
    fn test_parallel_partition_sums_within_budget() {
        for threads in [1, 4, 16] {
            let budget = MemoryBudget::new(64 * 1024 * 1024);
            let p = BudgetPartition::parallel(budget, threads);
            let worst = threads * 2 * p.max_chunk_bytes // worker builders
                + p.max_in_flight * p.batch_target      // finished batches
                + p.reorder_limit;
            assert!(
                worst <= budget.bytes(),
                "parallel worst case {worst} exceeds {} (threads {threads})",
                budget.bytes()
            );
        }
    }

    #[test]
    fn test_ledger_check_and_peak() {
        let mut ledger = BudgetLedger::new(MemoryBudget::new(100));
        ledger.set_input(40);
        ledger.set_builders(50);
        assert_eq!(ledger.peak(), 90);
        assert!(ledger.check(0).is_ok());
        ledger.charge_in_flight(20);
        assert_eq!(ledger.peak(), 110);
        assert!(ledger.check(0).is_err());
        assert!(ledger.check(10).is_ok());
        ledger.set_builders(0);
        ledger.release_in_flight(20);
        ledger.set_input(0);
        assert_eq!(ledger.peak(), 110);
    }
}
