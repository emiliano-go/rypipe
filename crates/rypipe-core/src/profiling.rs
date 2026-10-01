//! Named stage timings for the execution engine.
//!
//! Disabled by default. When disabled, every call is one relaxed atomic load
//! and a branch; when enabled, each stage accumulates nanoseconds across all
//! workers so adapter authors can see where a read spends its time (split,
//! validate, parse/build, export, merge) without a system profiler.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Stage slots, in report order.
pub const SPLIT: usize = 0;
pub const VALIDATE: usize = 1;
pub const PARSE: usize = 2;
pub const EXPORT: usize = 3;
pub const MERGE: usize = 4;
/// Number of stages.
pub const COUNT: usize = 5;

/// Human-readable names, index-aligned with the stage constants.
pub const NAMES: [&str; COUNT] = ["split", "validate", "parse", "export", "merge"];

static ENABLED: AtomicBool = AtomicBool::new(false);
static NANOS: [AtomicU64; COUNT] = [const { AtomicU64::new(0) }; COUNT];
static CALLS: [AtomicU64; COUNT] = [const { AtomicU64::new(0) }; COUNT];

/// Whether stage timing is currently on.
#[inline]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Clear all counters and start accounting.
pub fn reset() {
    for i in 0..COUNT {
        NANOS[i].store(0, Ordering::Relaxed);
        CALLS[i].store(0, Ordering::Relaxed);
    }
    ENABLED.store(true, Ordering::Relaxed);
}

/// Stop accounting (counters keep their last values).
pub fn disable() {
    ENABLED.store(false, Ordering::Relaxed);
}

/// Add `nanos` to a stage. No-op unless enabled.
#[inline]
pub fn add(stage: usize, nanos: u64) {
    if enabled() {
        NANOS[stage].fetch_add(nanos, Ordering::Relaxed);
        CALLS[stage].fetch_add(1, Ordering::Relaxed);
    }
}

/// `(stage_name, total_nanos, calls)` for every stage.
pub fn snapshot() -> Vec<(&'static str, u64, u64)> {
    (0..COUNT)
        .map(|i| {
            (
                NAMES[i],
                NANOS[i].load(Ordering::Relaxed),
                CALLS[i].load(Ordering::Relaxed),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_accumulate_only_while_enabled() {
        disable();
        add(PARSE, 100);
        assert_eq!(snapshot()[PARSE].1, 0, "no accounting while disabled");

        reset();
        add(PARSE, 5);
        add(PARSE, 7);
        let s = snapshot();
        assert_eq!(s[PARSE].1, 12);
        assert_eq!(s[PARSE].2, 2);
        assert_eq!(s[SPLIT].1, 0);
        disable();
    }
}
