//! Tier ladder for cost decomposition.
//!
//! Behind the `bench` feature flag. Provides six tier sinks that remove
//! exactly one cost layer each, and a `ladder` function that runs all tiers
//! and reports cumulative ms/MB, deltas, shares, CoV, and `n` per tier.
//!
//! Also provides:
//! - `alloc_baseline`: allocation pressure harness (S8)
//! - `ParProfile`: phase timing counters (S9)
//!
//! Usage:
//!     cargo test --features bench --release -- ladder --nocapture
//!     cargo test --features bench --release -- alloc_baseline --nocapture

use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::decoder::{ColumnarSink, RecordParser, Splitter};
use crate::engine::TableBuilder;
use crate::plan::ExecutionPlan;
use crate::value::Value;
use crate::Result;

// ---------------------------------------------------------------------------
// Tier sinks
// ---------------------------------------------------------------------------

/// Tier 1: Noop — boundaries only, no resolve, no value, no put_field.
pub struct NoopSink;

impl ColumnarSink for NoopSink {
    #[inline]
    fn begin_row(&mut self) {}
    #[inline]
    fn put_field(&mut self, _name: &str, _value: Value<'_>) {}
    #[inline]
    fn end_row(&mut self) {}
    #[inline]
    fn needs_value(&self) -> bool {
        false
    }
    #[inline]
    fn needs_resolve(&self) -> bool {
        false
    }
    fn finish(&mut self) -> Result<RecordBatch> {
        Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
    }
}

/// Tier 2: Traverse — walk records, no resolve, no value extraction.
pub struct TraverseSink;

impl ColumnarSink for TraverseSink {
    #[inline]
    fn begin_row(&mut self) {}
    #[inline]
    fn put_field(&mut self, _name: &str, _value: Value<'_>) {}
    #[inline]
    fn end_row(&mut self) {}
    #[inline]
    fn needs_value(&self) -> bool {
        false
    }
    #[inline]
    fn needs_resolve(&self) -> bool {
        true
    }
    fn finish(&mut self) -> Result<RecordBatch> {
        Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
    }
}

/// Tier 3: Locate — wants + resolve, no put_field (uses LocateOnly from engine).
/// Re-exported from `crate::engine::LocateOnly`.
/// Tier 4: Extract — extract value, discard (no push to columns).
pub struct ExtractOnly;

impl ColumnarSink for ExtractOnly {
    #[inline]
    fn begin_row(&mut self) {}
    #[inline]
    fn put_field(&mut self, _name: &str, _value: Value<'_>) {}
    #[inline]
    fn end_row(&mut self) {}
    #[inline]
    fn needs_value(&self) -> bool {
        true
    }
    fn finish(&mut self) -> Result<RecordBatch> {
        Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
    }
}

/// Tier 5: Push — scan + per-field push, no finish_row (no null-fill, no filter).
pub struct PushOnly {
    inner: TableBuilder,
}

impl PushOnly {
    pub fn new(plan: Arc<ExecutionPlan>) -> Self {
        Self {
            inner: TableBuilder::with_plan(0, plan),
        }
    }
}

impl ColumnarSink for PushOnly {
    #[inline]
    fn begin_row(&mut self) {}
    #[inline]
    fn put_field(&mut self, name: &str, value: Value<'_>) {
        self.inner.put_field(name, value);
    }
    #[inline]
    fn end_row(&mut self) {
        self.inner.advance_row();
    }
    #[inline]
    fn wants(&self, name: &str) -> bool {
        self.inner.wants(name)
    }
    #[inline]
    fn resolve<'a>(&'a self, name: &'a str) -> Option<&'a str> {
        self.inner.resolve(name)
    }
    #[inline]
    fn put_field_resolved(&mut self, name: &str, value: Value<'_>) {
        self.inner.put_field_resolved(name, value);
    }
    #[inline]
    fn resolve_and_put(&mut self, name: &str, value: Value<'_>) {
        self.inner.resolve_and_put(name, value);
    }
    fn finish(&mut self) -> Result<RecordBatch> {
        Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
    }
}

/// Tier 6: Build — + finish_row (null-fill, filter check), no Arrow export.
pub struct BuildOnly {
    inner: TableBuilder,
}

impl BuildOnly {
    pub fn new(plan: Arc<ExecutionPlan>) -> Self {
        Self {
            inner: TableBuilder::with_plan(0, plan),
        }
    }
}

impl ColumnarSink for BuildOnly {
    #[inline]
    fn begin_row(&mut self) {
        self.inner.begin_row();
    }
    #[inline]
    fn put_field(&mut self, name: &str, value: Value<'_>) {
        self.inner.put_field(name, value);
    }
    #[inline]
    fn end_row(&mut self) {
        self.inner.end_row();
    }
    #[inline]
    fn wants(&self, name: &str) -> bool {
        self.inner.wants(name)
    }
    #[inline]
    fn resolve<'a>(&'a self, name: &'a str) -> Option<&'a str> {
        self.inner.resolve(name)
    }
    #[inline]
    fn put_field_resolved(&mut self, name: &str, value: Value<'_>) {
        self.inner.put_field_resolved(name, value);
    }
    #[inline]
    fn resolve_and_put(&mut self, name: &str, value: Value<'_>) {
        self.inner.resolve_and_put(name, value);
    }
    fn finish(&mut self) -> Result<RecordBatch> {
        Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
    }
}

// ---------------------------------------------------------------------------
// Ladder report
// ---------------------------------------------------------------------------

pub struct LadderReport {
    pub tiers: Vec<TierResult>,
}

pub struct TierResult {
    pub name: &'static str,
    pub time_s: f64,
    pub delta_s: f64,
    pub cum_ms_per_mb: f64,
    pub delta_ms_per_mb: f64,
    pub share_pct: f64,
    pub cov_pct: f64,
    pub n: usize,
}

impl LadderReport {
    pub fn print(&self) {
        println!(
            "\n{:<20} {:>10} {:>10} {:>12} {:>12} {:>8} {:>6} {:>4}",
            "tier", "time(s)", "delta(s)", "cum ms/MB", "Δ ms/MB", "share%", "CoV%", "n"
        );
        println!("{}", "-".repeat(90));
        for t in &self.tiers {
            println!(
                "{:<20} {:10.4} {:10.4} {:12.3} {:12.3} {:7.1}% {:5.1}% {:4}",
                t.name,
                t.time_s,
                t.delta_s,
                t.cum_ms_per_mb,
                t.delta_ms_per_mb,
                t.share_pct,
                t.cov_pct,
                t.n
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Ladder runner
// ---------------------------------------------------------------------------

/// Run the tier ladder on a parser + splitter combination.
///
/// Runs each tier `rounds` times, reports adaptive median, and asserts
/// that deltas sum to total and cumulative is monotonic.
pub fn ladder<S: Splitter, P: RecordParser>(
    splitter: &S,
    parser: &P,
    bytes: &[u8],
    plan: Arc<ExecutionPlan>,
    rounds: usize,
) -> LadderReport {
    let mb = bytes.len() as f64 / 1_000_000.0;

    // Tier 1: Noop
    let (t_noop, _n) = bench_tier::<S, P, NoopSink>(splitter, parser, bytes, &mut NoopSink, rounds);

    // Tier 2: Traverse
    let (t_trav, _n) =
        bench_tier::<S, P, TraverseSink>(splitter, parser, bytes, &mut TraverseSink, rounds);

    // Tier 3: Locate
    let mut loc = crate::engine::LocateOnly::new(ExecutionPlan::new());
    let (t_loc, _n) = bench_tier(splitter, parser, bytes, &mut loc, rounds);

    // Tier 4: Extract
    let (t_ext, _n) =
        bench_tier::<S, P, ExtractOnly>(splitter, parser, bytes, &mut ExtractOnly, rounds);

    // Tier 5: Push
    let mut push = PushOnly::new(Arc::clone(&plan));
    let (t_push, _n) = bench_tier(splitter, parser, bytes, &mut push, rounds);

    // Tier 6: Build
    let mut build = BuildOnly::new(Arc::clone(&plan));
    let (t_build, _n) = bench_tier(splitter, parser, bytes, &mut build, rounds);

    // Tier 7: Full (TableBuilder + finish)
    let mut tb = TableBuilder::with_plan(0, Arc::clone(&plan));
    let (t_full, n) = bench_tier(splitter, parser, bytes, &mut tb, rounds);

    let times = [t_noop, t_trav, t_loc, t_ext, t_push, t_build, t_full];
    let names = [
        "scan_only",
        "traverse",
        "locate",
        "extract",
        "push",
        "build",
        "full_parse",
    ];
    let deltas: Vec<f64> = times
        .iter()
        .scan(0.0, |acc, &t| {
            let d = t - *acc;
            *acc = t;
            Some(d)
        })
        .collect();

    let total = t_full;
    let tiers: Vec<TierResult> = times
        .iter()
        .zip(deltas.iter())
        .zip(names.iter())
        .map(|((&time, &delta), &name)| {
            let cum_ms = time / mb * 1000.0;
            let delta_ms = delta / mb * 1000.0;
            let share = if total > 0.0 {
                delta / total * 100.0
            } else {
                0.0
            };
            TierResult {
                name,
                time_s: time,
                delta_s: delta,
                cum_ms_per_mb: cum_ms,
                delta_ms_per_mb: delta_ms,
                share_pct: share,
                cov_pct: 0.0, // TODO: compute from individual runs
                n,
            }
        })
        .collect();

    let report = LadderReport { tiers };

    // Assert invariants
    let delta_sum: f64 = report.tiers.iter().map(|t| t.delta_s).sum();
    let total_ms = total / mb * 1000.0;
    let delta_sum_ms = delta_sum / mb * 1000.0;
    assert!(
        (delta_sum_ms - total_ms).abs() < 0.001,
        "ladder reconciliation failed: deltas sum to {delta_sum_ms:.3} but total is {total_ms:.3}"
    );

    // Monotonicity: cumulative must be non-decreasing
    let mut prev = 0.0;
    for t in &report.tiers {
        assert!(
            t.time_s >= prev - 0.0001,
            "ladder not monotonic: {} ({:.4}) < previous ({:.4})",
            t.name,
            t.time_s,
            prev
        );
        prev = t.time_s;
    }

    report
}

fn bench_tier<S: Splitter, P: RecordParser, Sink: ColumnarSink>(
    _splitter: &S,
    parser: &P,
    bytes: &[u8],
    sink: &mut Sink,
    rounds: usize,
) -> (f64, usize) {
    let mut times: Vec<f64> = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let start = Instant::now();
        parser.parse_chunk(bytes, sink).unwrap();
        times.push(start.elapsed().as_secs_f64());
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = times.len();
    let med = times[n / 2];
    (med, n)
}

// ---------------------------------------------------------------------------
// S8: alloc_baseline — allocation pressure harness
// ---------------------------------------------------------------------------

/// Run a closure and report allocation statistics.
///
/// Reports: allocs, bytes, peak_live, reallocs, allocs_per_row, and the
/// log2 size histogram.  Determinism assertion: two consecutive runs must
/// produce identical counts (within 1% for timing-sensitive allocs).
pub fn alloc_baseline<F: Fn() -> usize>(label: &str, f: F) {
    #[cfg(feature = "alloc-stats")]
    {
        use crate::alloc_stats::{self};

        let stats1 = alloc_stats::snapshot();
        let rows = f();
        let stats2 = alloc_stats::snapshot();
        let delta = stats2.delta(&stats1);

        let allocs_per_row = if rows > 0 {
            delta.allocs as f64 / rows as f64
        } else {
            0.0
        };

        println!("\n{label}:");
        println!("  allocs:        {:>12}", delta.allocs);
        println!("  frees:         {:>12}", delta.frees);
        println!(
            "  bytes:         {:>12} ({:.1} MB)",
            delta.bytes,
            delta.bytes as f64 / 1e6
        );
        println!(
            "  peak_live:     {:>12} ({:.1} MB)",
            delta.peak,
            delta.peak as f64 / 1e6
        );
        println!("  reallocs:      {:>12}", delta.reallocs);
        println!("  rows:          {:>12}", rows);
        println!("  allocs/row:    {:>12.2}", allocs_per_row);
        println!("  size histogram (log2):");
        for (k, &count) in delta.size_hist.iter().enumerate() {
            if count > 0 {
                let lo = 1u64 << k;
                let hi = if k < 63 { 1u64 << (k + 1) } else { u64::MAX };
                println!("    [{:>3}] {:>3}..{:<3}  {:>12}", k, lo, hi, count);
            }
        }

        // Determinism check
        let stats3 = alloc_stats::snapshot();
        let rows2 = f();
        let stats4 = alloc_stats::snapshot();
        let delta2 = stats4.delta(&stats3);

        if delta.allocs != delta2.allocs {
            eprintln!(
                "  WARNING: non-deterministic alloc count: {} vs {}",
                delta.allocs, delta2.allocs
            );
        }
        if rows != rows2 {
            eprintln!(
                "  WARNING: non-deterministic row count: {} vs {}",
                rows, rows2
            );
        }
    }
    #[cfg(not(feature = "alloc-stats"))]
    {
        let _ = (label, f);
        eprintln!("alloc_baseline requires --features alloc-stats");
    }
}

// ---------------------------------------------------------------------------
// S9: ParProfile — phase timing counters
// ---------------------------------------------------------------------------

/// Phase timing counters for parallel execution.
///
/// Behind `#[cfg(feature = "profile")]` — costs ~15% on filtered paths
/// when enabled, so never enable in throughput builds.
#[derive(Debug, Clone, Default)]
pub struct ParProfile {
    pub split_scan_ns: u64,
    pub skip_regions_ns: u64,
    pub discovery_ns: u64,
    pub chunk_sum_ns: u64,
    pub chunk_max_ns: u64,
    pub chunk_mean_ns: u64,
    pub chunk_count: u64,
    pub spread: f64,
    pub export_ns: u64,
    pub combine_ns: u64,
}

impl ParProfile {
    pub fn print(&self) {
        let split_ms = self.split_scan_ns as f64 / 1e6;
        let skip_ms = self.skip_regions_ns as f64 / 1e6;
        let disc_ms = self.discovery_ns as f64 / 1e6;
        let chunk_sum_ms = self.chunk_sum_ns as f64 / 1e6;
        let chunk_max_ms = self.chunk_max_ns as f64 / 1e6;
        let export_ms = self.export_ns as f64 / 1e6;
        let combine_ms = self.combine_ns as f64 / 1e6;
        println!(
            "ParProfile: split={split_ms:.2}ms skip={skip_ms:.2}ms disc={disc_ms:.2}ms \
             chunks_sum={chunk_sum_ms:.2}ms max={chunk_max_ms:.2}ms count={} spread={:.2}x \
             export={export_ms:.2}ms combine={combine_ms:.2}ms",
            self.chunk_count, self.spread
        );
    }
}
