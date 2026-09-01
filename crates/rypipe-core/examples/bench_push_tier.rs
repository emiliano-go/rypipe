//! Isolated push-tier benchmark for `perf stat`.
//!
//! Runs only the push path (scan + per-field push, no finish_row, no Arrow export)
//! on a TSV file. Feed through perf stat to get instruction/stall counts for the
//! push phase.
//!
//! ```sh
//! cargo run --release -p rypipe-core --example bench_push_tier -- /path/to/file.tsv
//! perf stat -e cycles,instructions,branch-misses,L1-dcache-load-misses \
//!   target/release/examples/bench_push_tier /path/to/file.tsv
//! ```

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Instant;

use rypipe_core::decoder::ColumnarSink;
use rypipe_core::decoder::RecordParser;
use rypipe_core::plan::ExecutionPlan;
use rypipe_core::value::Value;
use rypipe_core::Result;

#[derive(Clone, Debug, Default)]
struct TsvParser;

impl RecordParser for TsvParser {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text =
            std::str::from_utf8(bytes).map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            sink.begin_row();
            for token in line.split('\t') {
                if let Some((k, v)) = token.split_once('=') {
                    // Fast path: resolve once, then push with the resolved name.
                    sink.resolve_and_put(k, Value::Str(Cow::Borrowed(v)));
                }
            }
            sink.end_row();
        }
        Ok(())
    }
}

/// Wrapper around TableBuilder that skips finish_row (null-fill, dirty mask,
/// filter check) by calling advance_row() only.  Isolates per-field push cost.
struct PushOnly {
    inner: rypipe_core::TableBuilder,
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
    fn needs_value(&self) -> bool {
        true
    }
    fn finish(&mut self) -> rypipe_core::Result<arrow::record_batch::RecordBatch> {
        self.inner.finish()
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = args.get(1).expect("usage: bench_push_tier <file.tsv>");
    let data = std::fs::read(path).expect("failed to read file");
    let mb = data.len() as f64 / 1_048_576.0;

    let parser = TsvParser;
    parser.validate(&data).unwrap();

    let est_row = {
        let newline_count = data.iter().filter(|&&b| b == b'\n').count().max(1);
        (data.len() / newline_count).max(512)
    };
    let est = (data.len() / est_row).max(64);
    let fields_per_row = {
        let first_line_end = data.iter().position(|&b| b == b'\n').unwrap_or(data.len());
        data[..first_line_end]
            .split(|&b| b == b'\t')
            .filter(|t| t.iter().any(|&b| b == b'='))
            .count()
            .max(1)
    };

    // Warmup
    {
        let plan = ExecutionPlan::new();
        let mut sink = PushOnly { inner: rypipe_core::TableBuilder::with_plan(est, Arc::new(plan)) };
        parser.parse_chunk(&data, &mut sink).unwrap();
    }

    // Benchmark: 7 iterations, report median
    let n = 7;
    let mut times: Vec<f64> = Vec::with_capacity(n);
    let mut last_rows = 0usize;
    for _ in 0..n {
        let plan = ExecutionPlan::new();
        let mut sink = PushOnly { inner: rypipe_core::TableBuilder::with_plan(est, Arc::new(plan)) };
        let t0 = Instant::now();
        parser.parse_chunk(&data, &mut sink).unwrap();
        let dt = t0.elapsed().as_secs_f64();
        times.push(dt);
        last_rows = sink.inner.num_rows();
    }

    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = times[n / 2];
    let best = times[0];
    let worst = times[n - 1];
    let mean = times.iter().sum::<f64>() / n as f64;
    let stdev = (times.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / n as f64).sqrt();
    let cov = stdev / mean;
    let fields_total = last_rows * fields_per_row;

    println!("push_only {median:.4}s median ({best:.4}-{worst:.4}, CoV {:.1}%)", cov * 100.0);
    println!("  {last_rows} rows, {mb:.1} MB, {:.0} MB/s", mb / median);
    println!("  ns/field: {:.0}", median * 1e9 / fields_total as f64);
    println!();
    println!("Run with:");
    println!("  perf stat -e cycles,instructions,branch-misses,L1-dcache-load-misses \\");
    println!("    target/release/examples/bench_push_tier {path}");
}
