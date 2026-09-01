//! Standalone throughput benchmark for the rypipe-core engine.
//!
//! Features (S2):
//! - Build-SHA gating: refuses to run if binary SHA doesn't match HEAD
//! - Adaptive median-of-N: sample until 1.31×CoV ≤ 5%, capped at 31
//! - Per-config subprocess isolation: `--only-config N` runs one config
//! - Provenance: commit, dirty, build_sha, thp_defrag, RSS
//! - Steady-state: 20× inner loop for runs under 50 ms
//!
//! Run with:
//!     cargo run --release -p rypipe-core --example bench_throughput
//!     cargo run --release -p rypipe-core --example bench_throughput -- --only-config 2

use std::borrow::Cow;
use std::fs::File;
use std::io::{BufRead, Write};
use std::time::Instant;

use rypipe_core::{
    bounded::MemoryBudget, decoder::ColumnarSink, decoder::RecordParser, decoder::Splitter,
    parallel::chunk_profile, pipeline::Pipeline, plan::FieldType, value::Value, Result,
};

const ROWS: usize = 5_000_000;

// ---------------------------------------------------------------------------
// S2a: Build-SHA gating
// ---------------------------------------------------------------------------

const BUILD_SHA: &str = env!("RYPIPE_BUILD_SHA");

fn verify_build_sha(allow_dirty: bool) {
    let head = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                String::from_utf8(o.stdout)
                    .ok()
                    .map(|s| s.trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".to_string());

    let dirty = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .map(|o| !o.stdout.is_empty())
        .unwrap_or(false);

    let expected = if dirty && !allow_dirty {
        format!("{head}-dirty")
    } else {
        head
    };

    if BUILD_SHA != expected {
        eprintln!(
            "ERROR: build SHA mismatch. Binary has {BUILD_SHA}, HEAD is {expected}. \
             Run: cargo build --release -p rypipe-core --example bench_throughput"
        );
        std::process::exit(1);
    }
}

// ---------------------------------------------------------------------------
// THP defrag (read active value)
// ---------------------------------------------------------------------------

fn read_thp_defrag() -> String {
    std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/defrag")
        .map(|s| {
            // Format: "always madvise defer defer+madvise [madvise] never"
            // Bracketed value is active.
            if let Some(start) = s.find('[') {
                if let Some(end) = s[start..].find(']') {
                    return s[start + 1..start + end].to_string();
                }
            }
            s.trim().to_string()
        })
        .unwrap_or_else(|_| "unknown".to_string())
}

// ---------------------------------------------------------------------------
// TSV adapter (inline, minimal)
// ---------------------------------------------------------------------------

fn generate_data(rows: usize) -> Vec<u8> {
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
struct TsvSplitter;

impl Splitter for TsvSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        if from >= bytes.len() {
            return None;
        }
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
        let target = bytes.len() / max_chunks;
        for i in 1..max_chunks {
            let approx = i * target;
            if let Some(off) = memchr::memchr(b'\n', &bytes[approx..]) {
                let next = approx + off + 1;
                if next > *points.last().unwrap() && next < bytes.len() {
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
        let newline_count = sample.iter().filter(|&&b| b == b'\n').count().max(1);
        (sample.len() / newline_count).max(1)
    }
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
                    sink.resolve_and_put(k, Value::Str(Cow::Borrowed(v)));
                }
            }
            sink.end_row();
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// RSS helper
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Default)]
struct Rss {
    total_kb: usize,
    anon_kb: usize,
    file_kb: usize,
}

fn current_rss() -> Option<Rss> {
    #[cfg(target_os = "linux")]
    {
        let file = File::open("/proc/self/status").ok()?;
        let mut rss = Rss::default();
        for line in std::io::BufReader::new(file).lines() {
            let line = line.ok()?;
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                rss.total_kb = rest
                    .split_whitespace()
                    .next()
                    .and_then(|n| n.parse().ok())?;
            } else if let Some(rest) = line.strip_prefix("RssAnon:") {
                rss.anon_kb = rest
                    .split_whitespace()
                    .next()
                    .and_then(|n| n.parse().ok())?;
            } else if let Some(rest) = line.strip_prefix("RssFile:") {
                rss.file_kb = rest
                    .split_whitespace()
                    .next()
                    .and_then(|n| n.parse().ok())?;
            }
        }
        return Some(rss);
    }
    #[allow(unreachable_code)]
    None
}

// ---------------------------------------------------------------------------
// S2b: Adaptive median-of-N
// ---------------------------------------------------------------------------

fn adaptive_median(f: impl Fn() -> Result<usize>) -> (f64, f64, usize, usize) {
    let mut times: Vec<f64> = Vec::with_capacity(31);
    let mut last_rows = 0;

    // Warmup
    last_rows = f().expect("warmup failed");

    // Adaptive: sample until 1.31 × CoV ≤ 5%, capped at 31.
    for n in 1..=31 {
        let start = Instant::now();
        last_rows = f().expect("benchmark failed");
        times.push(start.elapsed().as_secs_f64());

        if n < 3 {
            continue;
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let med = times[n / 2];
        let mean = times.iter().sum::<f64>() / n as f64;
        let stdev = (times.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / n as f64).sqrt();
        let cov = stdev / mean;
        if 1.31 * cov <= 0.05 {
            return (med, cov, last_rows, n);
        }
    }

    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = times.len();
    let med = times[n / 2];
    let mean = times.iter().sum::<f64>() / n as f64;
    let stdev = (times.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / n as f64).sqrt();
    let cov = stdev / mean;
    (med, cov, last_rows, n)
}

// ---------------------------------------------------------------------------
// S2e: Steady-state (20× inner loop for fast configs)
// ---------------------------------------------------------------------------

fn steady_state(f: impl Fn() -> Result<usize>) -> (f64, usize) {
    let mut times: Vec<f64> = Vec::with_capacity(20);
    let mut last_rows = 0;
    for _ in 0..20 {
        let start = Instant::now();
        last_rows = f().expect("steady-state failed");
        times.push(start.elapsed().as_secs_f64());
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    // Return median of the 20 steady-state runs
    let med = times[10];
    (med, last_rows)
}

// ---------------------------------------------------------------------------
// Per-config runner
// ---------------------------------------------------------------------------

struct BenchConfig {
    name: &'static str,
    rounds: usize,
}

fn run_config(
    config: &BenchConfig,
    pipeline: &Pipeline<TsvSplitter, TsvParser>,
    path: &std::path::Path,
    bytes: usize,
) {
    let (med, cov, rows, n) = adaptive_median(|| {
        let batch = pipeline.read_path(path, false, false)?;
        Ok(batch.num_rows())
    });
    let rows_per_sec = rows as f64 / med;
    let mb_per_sec = bytes as f64 / med / 1_000_000.0;
    let memory = current_rss()
        .map(|rss| {
            format!(
                "{:6.0} MB ({:6.0} anon + {:6.0} file)",
                rss.total_kb as f64 / 1024.0,
                rss.anon_kb as f64 / 1024.0,
                rss.file_kb as f64 / 1024.0
            )
        })
        .unwrap_or_else(|| "  N/A  ".to_string());
    let cov_pct = cov * 100.0;
    let marker = if cov_pct > 8.0 { " *" } else { "" };
    println!(
        "{:28} {:8.3}s median ({:5.1}% CoV, n={n}{marker})  {rows:10} rows  {:8.0} rows/s  {:6.1} MB/s  RSS {memory}",
        config.name, med, cov_pct, rows_per_sec, mb_per_sec
    );
}

fn run_par_config(
    name: &str,
    pipeline: &Pipeline<TsvSplitter, TsvParser>,
    path: &std::path::Path,
    bytes: usize,
    chunks: usize,
) {
    let (med, cov, rows, n) = adaptive_median(|| {
        let batches = pipeline.read_path_par(path, chunks, false, false)?;
        let (split_ns, sum_ns, max_ns, count) = chunk_profile();
        eprintln!(
            "    [{name} profile] split={:.2}ms chunks_sum={:.2}ms max={:.2}ms count={}",
            split_ns as f64 / 1e6,
            sum_ns as f64 / 1e6,
            max_ns as f64 / 1e6,
            count
        );
        Ok(batches.iter().map(|b| b.num_rows()).sum::<usize>())
    });
    let rows_per_sec = rows as f64 / med;
    let mb_per_sec = bytes as f64 / med / 1_000_000.0;
    let memory = current_rss()
        .map(|rss| {
            format!(
                "{:6.0} MB ({:6.0} anon + {:6.0} file)",
                rss.total_kb as f64 / 1024.0,
                rss.anon_kb as f64 / 1024.0,
                rss.file_kb as f64 / 1024.0
            )
        })
        .unwrap_or_else(|| "  N/A  ".to_string());
    let cov_pct = cov * 100.0;
    let marker = if cov_pct > 8.0 { " *" } else { "" };
    println!(
        "{name:28} {med:8.3}s median ({cov_pct:5.1}% CoV, n={n}{marker})  {rows:10} rows  {rows_per_sec:8.0} rows/s  {mb_per_sec:6.1} MB/s  RSS {memory}"
    );
}

fn run_stream_config(
    name: &str,
    pipeline: &Pipeline<TsvSplitter, TsvParser>,
    path: &std::path::Path,
    bytes: usize,
    budget: MemoryBudget,
) {
    let (med, cov, rows, n) = adaptive_median(|| {
        let batches = pipeline.read_path_stream(path, budget, false)?;
        Ok(batches.iter().map(|b| b.num_rows()).sum::<usize>())
    });
    let rows_per_sec = rows as f64 / med;
    let mb_per_sec = bytes as f64 / med / 1_000_000.0;
    let memory = current_rss()
        .map(|rss| {
            format!(
                "{:6.0} MB ({:6.0} anon + {:6.0} file)",
                rss.total_kb as f64 / 1024.0,
                rss.anon_kb as f64 / 1024.0,
                rss.file_kb as f64 / 1024.0
            )
        })
        .unwrap_or_else(|| "  N/A  ".to_string());
    let cov_pct = cov * 100.0;
    let marker = if cov_pct > 8.0 { " *" } else { "" };
    println!(
        "{name:28} {med:8.3}s median ({cov_pct:5.1}% CoV, n={n}{marker})  {rows:10} rows  {rows_per_sec:8.0} rows/s  {mb_per_sec:6.1} MB/s  RSS {memory}"
    );
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    // S2a: Build-SHA gating
    let allow_dirty = std::env::args().any(|a| a == "--allow-dirty");
    verify_build_sha(allow_dirty);

    // S2c: Per-config isolation
    let only_config: Option<usize> = std::env::args()
        .position(|a| a == "--only-config")
        .and_then(|i| std::env::args().nth(i + 1))
        .and_then(|s| s.parse().ok());

    let data = generate_data(ROWS);
    let bytes = data.len();
    let mb = bytes as f64 / 1_000_000.0;

    // S2d: Provenance
    let thp = read_thp_defrag();
    println!("rypipe-core bench_throughput  SHA={BUILD_SHA}");
    println!("  THP defrag: {thp}");
    println!("Generated {ROWS} rows ({mb:.1} MB)\n");

    let mut path = std::env::temp_dir();
    path.push("rypipe_bench_throughput.tsv");
    File::create(&path)?.write_all(&data)?;

    let pipeline = Pipeline::new(TsvSplitter, TsvParser).with_plan(
        rypipe_core::ExecutionPlan::new()
            .type_as("id", FieldType::Int64)
            .type_as("amount", FieldType::Float64)
            .type_as("count", FieldType::Int64),
    );

    let configs = vec![BenchConfig {
        name: "single-thread",
        rounds: 7,
    }];

    println!("-- single-thread --");
    if only_config.map_or(true, |c| c == 0) {
        run_config(&configs[0], &pipeline, &path, bytes);
    }

    println!("\n-- parallel --");
    for (i, chunks) in [4, 8, 16].iter().enumerate() {
        let cfg_idx = i + 1;
        if only_config.map_or(true, |c| c == cfg_idx) {
            run_par_config(&format!("par{chunks}"), &pipeline, &path, bytes, *chunks);
        }
    }

    println!("\n-- streaming --");
    if only_config.map_or(true, |c| c == 4) {
        run_stream_config(
            "bounded 64 MiB",
            &pipeline,
            &path,
            bytes,
            MemoryBudget::new(64 * 1024 * 1024),
        );
    }

    println!("\n* = CoV > 8%");
    std::fs::remove_file(&path)?;
    Ok(())
}
