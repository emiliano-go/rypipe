//! Isolated push-tier benchmark for `perf stat`.
//!
//! Runs only the push path (scan + per-field push, no finish_row, no Arrow export)
//! on a Crystal Reports XML file. Feed through perf stat to get instruction/stall
//! counts for the 47% phase.
//!
//! ```sh
//! cargo run --release -p rypipe-core --example bench_push_tier -- /path/to/file.xml
//! perf stat -e cycles,instructions,branch-misses,L1-dcache-load-misses \
//!   target/release/examples/bench_push_tier /path/to/file.xml
//! ```

use std::time::Instant;

use rypipe_core::decoder::ColumnarSink;
use rypipe_core::plan::ExecutionPlan;
use rypipe_core::value::Value;
use rypipe_core::Result;

/// Wrapper around TableBuilder that skips finish_row (null-fill, dirty mask,
/// filter check) by calling advance_row() only.  Isolates per-field push cost.
struct PushOnly {
    inner: rypipe_core::TableBuilder,
}

impl ColumnarSink for PushOnly {
    #[inline] fn begin_row(&mut self) {}
    #[inline] fn put_field(&mut self, name: &str, value: Value<'_>) {
        self.inner.put_field(name, value);
    }
    #[inline] fn end_row(&mut self) {
        self.inner.advance_row();
    }
    #[inline] fn wants(&self, name: &str) -> bool { self.inner.wants(name) }
    #[inline] fn resolve<'a>(&'a self, name: &'a str) -> Option<&'a str> {
        self.inner.resolve(name)
    }
    #[inline] fn put_field_resolved(&mut self, name: &str, value: Value<'_>) {
        self.inner.put_field_resolved(name, value);
    }
    #[inline] fn needs_value(&self) -> bool { true }
    fn finish(&mut self) -> Result<arrow::record_batch::RecordBatch> {
        self.inner.finish()
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = args.get(1).expect("usage: bench_push_tier <file.xml>");
    let data = std::fs::read(path).expect("failed to read file");
    let mb = data.len() as f64 / 1_048_576.0;

    let splitter = rypipe_core::xml::CrystalXmlSplitter::with_row_tag(b"Details");
    let decoder = rypipe_core::xml::CrystalXmlDecoder::with_row_tag(b"Details");

    // Validate
    decoder.validate(&data).unwrap();

    // Warmup
    {
        let plan = ExecutionPlan::new();
        let est_row = splitter.estimate_bytes_per_row(&data[..data.len().min(65536)]).max(512);
        let est = (data.len() / est_row).max(64);
        let mut sink = PushOnly { inner: rypipe_core::TableBuilder::with_plan(est, plan) };
        decoder.parse_chunk_generic(&data, &mut sink).unwrap();
    }

    // Benchmark: 7 iterations, report median
    let n = 7;
    let mut times: Vec<f64> = Vec::with_capacity(n);
    let mut last_rows = 0usize;
    for _ in 0..n {
        let plan = ExecutionPlan::new();
        let est_row = splitter.estimate_bytes_per_row(&data[..data.len().min(65536)]).max(512);
        let est = (data.len() / est_row).max(64);
        let mut sink = PushOnly { inner: rypipe_core::TableBuilder::with_plan(est, plan) };
        let t0 = Instant::now();
        decoder.parse_chunk_generic(&data, &mut sink).unwrap();
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

    println!("push_only {median:.4}s median ({best:.4}-{worst:.4}, CoV {cov:.1%})");
    println!("  {last_rows} rows, {mb:.1} MB, {:.0} MB/s", mb / median);
    println!("  ns/field: {:.0}", median * 1e9 / (last_rows as f64 * 10.0));
    println!();
    println!("Run with:");
    println!("  perf stat -e cycles,instructions,branch-misses,L1-dcache-load-misses \\");
    println!("    target/release/examples/bench_push_tier {path}");
}
