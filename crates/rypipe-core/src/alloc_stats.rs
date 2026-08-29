//! Counting allocator wrapper for measuring allocation pressure.
//!
//! Enabled behind the `alloc-stats` feature. Wraps the system allocator (or
//! whatever `#[global_allocator]` is set to) and records every alloc/dealloc
//! into lock-free atomics. The overhead is ~5–15% on allocation-heavy paths;
//! never enable for production throughput runs.

use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
// Note: PEAK is AtomicU64 because peak live bytes is always non-negative.
// LIVE is AtomicI64 because it can temporarily dip below zero due to
// ordering races between threads.

/// Total allocation calls.
pub static ALLOCS: AtomicU64 = AtomicU64::new(0);
/// Total deallocation calls.
pub static FREES: AtomicU64 = AtomicU64::new(0);
/// Total bytes requested (sum of layout.size()).
pub static BYTES: AtomicU64 = AtomicU64::new(0);
/// Current live bytes (allocs minus frees).
pub static LIVE: AtomicI64 = AtomicI64::new(0);
/// High-water mark of live bytes.
pub static PEAK: AtomicU64 = AtomicU64::new(0);
/// Reallocation calls (tracked separately).
pub static REALLOCS: AtomicU64 = AtomicU64::new(0);
/// Helper: create an AtomicU64 initialized to zero in const context.
const fn zero_atomic() -> AtomicU64 {
    AtomicU64::new(0)
}

/// Log2 size histogram: SIZE_HIST[k] counts allocations with size in [2^k, 2^(k+1)).
/// Index 0 = size 1, index 1 = size 2, … index 63 = size ≥ 2^63.
pub static SIZE_HIST: [AtomicU64; 64] = [const { zero_atomic() }; 64];

/// Snapshot of allocator counters.
#[derive(Debug, Clone)]
pub struct AllocStats {
    pub allocs: u64,
    pub frees: u64,
    pub bytes: u64,
    pub live: i64,
    pub peak: u64,
    pub reallocs: u64,
    /// SIZE_HIST[k] = count of allocations with size in [2^k, 2^(k+1)).
    pub size_hist: [u64; 64],
}

impl AllocStats {
    /// Delta between two snapshots.
    pub fn delta(&self, before: &Self) -> Self {
        Self {
            allocs: self.allocs - before.allocs,
            frees: self.frees - before.frees,
            bytes: self.bytes - before.bytes,
            live: self.live - before.live,
            peak: self.peak,
            reallocs: self.reallocs - before.reallocs,
            size_hist: {
                let mut h = [0u64; 64];
                for i in 0..64 {
                    h[i] = self.size_hist[i] - before.size_hist[i];
                }
                h
            },
        }
    }
}

/// Read all counters atomically into a snapshot.
pub fn snapshot() -> AllocStats {
    let mut hist = [0u64; 64];
    for (i, s) in SIZE_HIST.iter().enumerate() {
        hist[i] = s.load(Relaxed);
    }
    AllocStats {
        allocs: ALLOCS.load(Relaxed),
        frees: FREES.load(Relaxed),
        bytes: BYTES.load(Relaxed),
        live: LIVE.load(Relaxed),
        peak: PEAK.load(Relaxed),
        reallocs: REALLOCS.load(Relaxed),
        size_hist: hist,
    }
}

/// Reset all counters to zero.
pub fn reset() {
    ALLOCS.store(0, Relaxed);
    FREES.store(0, Relaxed);
    BYTES.store(0, Relaxed);
    LIVE.store(0, Relaxed);
    PEAK.store(0, Relaxed);
    REALLOCS.store(0, Relaxed);
    for s in SIZE_HIST.iter() {
        s.store(0, Relaxed);
    }
}

/// Wrapper allocator that delegates to the inner allocator while recording
/// every operation into the static counters above.
pub struct Counting<A>(pub A);

unsafe impl<A: GlobalAlloc> GlobalAlloc for Counting<A> {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(layout.size() as u64, Relaxed);
        let idx = (63 - layout.size().max(1).leading_zeros()) as usize;
        if idx < 64 {
            SIZE_HIST[idx].fetch_add(1, Relaxed);
        }
        let live = LIVE.fetch_add(layout.size() as i64, Relaxed) + layout.size() as i64;
        PEAK.fetch_max(live as u64, Relaxed);
        self.0.alloc(layout)
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        FREES.fetch_add(1, Relaxed);
        LIVE.fetch_sub(layout.size() as i64, Relaxed);
        self.0.dealloc(ptr, layout)
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        REALLOCS.fetch_add(1, Relaxed);
        let old_size = layout.size();
        let size_diff = new_size as i64 - old_size as i64;
        LIVE.fetch_add(size_diff, Relaxed);
        let live = LIVE.load(Relaxed);
        PEAK.fetch_max(live as u64, Relaxed);
        // Count the reallocation size into the histogram for the new size.
        let idx = (63 - new_size.max(1).leading_zeros()) as usize;
        if idx < 64 {
            SIZE_HIST[idx].fetch_add(1, Relaxed);
        }
        BYTES.fetch_add(new_size as u64, Relaxed);
        self.0.realloc(ptr, layout, new_size)
    }
}

/// Pretty-print an `AllocStats` snapshot.
pub fn print_stats(label: &str, stats: &AllocStats) {
    println!("=== {label} ===");
    println!(
        "  allocs: {:>12}  frees: {:>12}  reallocs: {:>8}",
        stats.allocs, stats.frees, stats.reallocs
    );
    println!(
        "  bytes:  {:>12}  live: {:>12}  peak: {:>12}",
        stats.bytes, stats.live, stats.peak
    );
    println!("  size histogram (log2 buckets):");
    for (i, &count) in stats.size_hist.iter().enumerate() {
        if count > 0 {
            let lo = 1u64 << i;
            let hi = if i < 63 { 1u64 << (i + 1) } else { u64::MAX };
            println!("    [{:>10} .. {:<10}): {:>10}", lo, hi, count);
        }
    }
}
