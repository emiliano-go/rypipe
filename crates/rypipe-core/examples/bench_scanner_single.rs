//! Six-tier single-threaded benchmark for `perf stat`.
//!
//! Each tier removes exactly one cost layer:
//!   1. **scan_only** — memchr newline scan (pure byte walk ceiling).
//!   2. **traverse** — full parser walk, find field extents, no resolve, no put_field.
//!   3. **locate** — + wants() + resolve(), no put_field.
//!   4. **push_only** — + extract + push, no finish_row (per-field push only).
//!   5. **build_only** — + finish_row (null-fill, dirty mask, filter).
//!   6. **full_parse** — + Arrow export (finish() → to_arrow).
//!
//! Run with:
//!
//! ```sh
//! cargo run --release -p rypipe-core --example bench_scanner_single
//! cargo run --release -p rypipe-core --example bench_scanner_single -- /path/to/file.tsv
//!
//! # With perf stat:
//! perf stat -e cycles,instructions,branch-misses,L1-dcache-load-misses,dTLB-load-misses \
//!   target/release/examples/bench_scanner_single
//! ```

use std::sync::Arc;
use std::io::Write;
use std::time::Instant;

use rypipe_core::{
    decoder::ColumnarSink, decoder::RecordParser, engine::LocateOnly, plan::ExecutionPlan,
    value::Value, Result,
};

// ---------------------------------------------------------------------------
// Inline TSV adapter
// ---------------------------------------------------------------------------

const FIELDS_PER_ROW: usize = 5;

fn generate_tsv(rows: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(rows * 48);
    for i in 0..rows {
        writeln!(
            &mut buf,
            "id={i}\tstatus=active\tregion=north\tamount=123.45\tcount=7",
        )
        .unwrap();
    }
    buf
}

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
                    if sink.needs_resolve() && !sink.wants(k) {
                        continue;
                    }
                    if sink.needs_value() {
                        sink.put_field(k, Value::Str(v));
                    }
                }
            }
            sink.end_row();
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tier 1: scan_only — memchr newline scan
// ---------------------------------------------------------------------------

fn bench_scan_only(data: &[u8]) -> (usize, f64) {
    let tag = b"\n";
    let start = Instant::now();
    let count = memchr::memmem::find_iter(data, tag).count();
    let elapsed = start.elapsed().as_secs_f64();
    (count, elapsed)
}

// ---------------------------------------------------------------------------
// Tier 2: traverse — walk rows, find fields, no resolve, no put_field
// ---------------------------------------------------------------------------

struct TraverseOnly;

impl ColumnarSink for TraverseOnly {
    #[inline] fn begin_row(&mut self) {}
    #[inline] fn put_field(&mut self, _n: &str, _v: Value<'_>) {}
    #[inline] fn end_row(&mut self) {}
    #[inline] fn wants(&self, _name: &str) -> bool { true }
    #[inline] fn needs_value(&self) -> bool { false }
    #[inline] fn needs_resolve(&self) -> bool { false }
    fn finish(&mut self) -> Result<arrow::record_batch::RecordBatch> {
        Ok(arrow::record_batch::RecordBatch::new_empty(
            std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        ))
    }
}

fn bench_traverse(data: &[u8]) -> (usize, usize, f64) {
    let parser = TsvParser;
    parser.validate(data).unwrap();
    let mut sink = TraverseOnly;
    let start = Instant::now();
    parser.parse_chunk(data, &mut sink).unwrap();
    let elapsed = start.elapsed().as_secs_f64();
    let rows = memchr::memmem::find_iter(data, b"\n").count();
    (rows, rows * FIELDS_PER_ROW, elapsed)
}

// ---------------------------------------------------------------------------
// Tier 3: locate — wants() + resolve(), no put_field
// ---------------------------------------------------------------------------

fn bench_locate(data: &[u8], plan: ExecutionPlan) -> (usize, usize, f64) {
    let parser = TsvParser;
    parser.validate(data).unwrap();
    let mut sink = LocateOnly::new(plan);
    let start = Instant::now();
    parser.parse_chunk(data, &mut sink).unwrap();
    let elapsed = start.elapsed().as_secs_f64();
    (sink.row_count, sink.field_count, elapsed)
}

// ---------------------------------------------------------------------------
// Tier 4: push_only — extract + push, no finish_row
// ---------------------------------------------------------------------------

/// Wrapper around TableBuilder that skips finish_row (null-fill, dirty mask,
/// filter check) by calling advance_row() only.  Isolates per-field push cost
/// from per-row finalization.
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

// ---------------------------------------------------------------------------
// Tier 5: build_only — push + finish_row, no Arrow export
// ---------------------------------------------------------------------------

fn bench_build_only(data: &[u8], plan: ExecutionPlan) -> (usize, f64) {
    use rypipe_core::TableBuilder;
    let parser = TsvParser;
    parser.validate(data).unwrap();
    let mut sink = TableBuilder::with_plan((data.len() / 16).max(64), Arc::new(plan));
    let start = Instant::now();
    parser.parse_chunk(data, &mut sink).unwrap();
    let elapsed = start.elapsed().as_secs_f64();
    (sink.num_rows(), elapsed)
}

// ---------------------------------------------------------------------------
// Tier 6: full_parse — push + finish_row + Arrow export
// ---------------------------------------------------------------------------

fn bench_full_parse(data: &[u8], plan: ExecutionPlan) -> (usize, f64) {
    use rypipe_core::TableBuilder;
    let parser = TsvParser;
    parser.validate(data).unwrap();
    let mut sink = TableBuilder::with_plan((data.len() / 16).max(64), Arc::new(plan));
    let start = Instant::now();
    parser.parse_chunk(data, &mut sink).unwrap();
    let batch = sink.finish().unwrap();
    let elapsed = start.elapsed().as_secs_f64();
    (batch.num_rows(), elapsed)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (data, expected_rows, expected_fpr): (Vec<u8>, usize, usize) =
        if args.len() > 1 {
            let path = &args[1];
            let data = std::fs::read(path).expect("failed to read file");
            let mb = data.len() as f64 / 1_048_576.0;
            eprintln!("Read {path} ({mb:.1} MB)");
            let rows = memchr::memmem::find_iter(&data, b"\n").count();
            let first_line_end = data.iter().position(|&b| b == b'\n').unwrap_or(data.len());
            let fpr = data[..first_line_end]
                .split(|&b| b == b'\t')
                .filter(|t| t.iter().any(|&b| b == b'='))
                .count()
                .max(1);
            (data, rows, fpr)
        } else {
            let rows = 5_000_000;
            (generate_tsv(rows), rows, FIELDS_PER_ROW)
        };

    let bytes = data.len();
    let mb = bytes as f64 / 1_048_576.0;
    let fields_total = expected_rows * expected_fpr;
    let bpf = bytes as f64 / fields_total as f64;

    println!("=== bench_scanner_single (six-tier) ===");
    println!("{mb:.1} MB, {expected_rows} est. rows, {expected_fpr} fields/row, {bpf:.0} bytes/field");
    println!("Fields total: {fields_total}");
    println!();

    // Tier 1: scan_only
    let (newlines, t1) = bench_scan_only(&data);

    // Tier 2: traverse
    let (rows2, fields2, t2) = bench_traverse(&data);

    // Tier 3: locate
    let plan_locate = ExecutionPlan::new();
    let (rows3, fields3, t3) = bench_locate(&data, plan_locate);

    // Tier 4: push_only
    let plan_push = ExecutionPlan::new();
    let mut push_sink = PushOnly {
        inner: rypipe_core::TableBuilder::with_plan((data.len() / 16).max(64), Arc::new(plan_push)),
    };
    let start4 = Instant::now();
    TsvParser.validate(&data).unwrap();
    TsvParser.parse_chunk(&data, &mut push_sink).unwrap();
    let t4 = start4.elapsed().as_secs_f64();
    let rows4 = push_sink.inner.num_rows();

    // Tier 5: build_only
    let plan_build = ExecutionPlan::new();
    let (rows5, t5) = bench_build_only(&data, plan_build);

    // Tier 6: full_parse
    let plan_full = ExecutionPlan::new();
    let (rows6, t6) = bench_full_parse(&data, plan_full);

    // Assertions: tiers must visit the same data
    assert!(rows3 > 0, "locate produced zero rows — likely DCE'd");
    assert!(fields3 > 0, "locate saw zero fields — likely DCE'd");
    assert!(rows6 > 0, "full_parse produced zero rows");
    assert_eq!(
        rows3, rows6,
        "row count mismatch: locate={rows3} vs full={rows6}"
    );

    // Print tier table
    println!(
        "{:<16} {:>8}  {:>10} rows  {:>12} fields  {:>8} MB/s  {:>10} ns/field",
        "tier", "time", "rows", "fields", "MB/s", "ns/field"
    );
    println!("{}", "-".repeat(80));

    let print_tier = |name: &str, rows: usize, fields: usize, elapsed: f64| {
        let mb_s = mb / elapsed;
        let ns_per_field = if fields > 0 { elapsed * 1e9 / fields as f64 } else { 0.0 };
        println!("{name:<16} {elapsed:>7.4}s  {rows:>10} rows  {fields:>12} fields  {mb_s:>8.0} MB/s  {ns_per_field:>10.1} ns/field");
    };

    print_tier("scan_only", newlines, 0, t1);
    print_tier("traverse", rows2, fields2, t2);
    print_tier("locate", rows3, fields3, t3);
    print_tier("push_only", rows4, rows4 * expected_fpr, t4);
    print_tier("build_only", rows5, rows5 * expected_fpr, t5);
    print_tier("full_parse", rows6, rows6 * expected_fpr, t6);

    // ms/MB decomposition (additive, each rung adds exactly one cost layer)
    println!();
    println!("ms/MB decomposition (additive, each rung adds exactly one cost layer):");
    println!("  scan_only:    {:.3} ms/MB", t1 / mb * 1000.0);
    println!("  traverse:     {:.3} ms/MB  (+{:.3} = traversal cost)", t2 / mb * 1000.0, (t2 - t1) / mb * 1000.0);
    println!("  locate:       {:.3} ms/MB  (+{:.3} = resolution cost)", t3 / mb * 1000.0, (t3 - t2) / mb * 1000.0);
    println!("  push_only:    {:.3} ms/MB  (+{:.3} = per-field push cost)", t4 / mb * 1000.0, (t4 - t3) / mb * 1000.0);
    println!("  build_only:   {:.3} ms/MB  (+{:.3} = finish_row cost)", t5 / mb * 1000.0, (t5 - t4) / mb * 1000.0);
    println!("  full_parse:   {:.3} ms/MB  (+{:.3} = Arrow export cost)", t6 / mb * 1000.0, (t6 - t5) / mb * 1000.0);
    println!("  total:        {:.3} ms/MB", t6 / mb * 1000.0);
}
