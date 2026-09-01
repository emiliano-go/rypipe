//! Standalone throughput benchmark for the rypipe-core engine.
//!
//! This example uses a tiny inline TSV-like adapter so the benchmark measures
//! the engine itself, not any particular external parser. Run with:
//!
//!     cargo run --release -p rypipe-core --example bench_throughput

use std::borrow::Cow;
use std::fs::File;
use std::io::{BufRead, Write};
use std::time::Instant;

use rypipe_core::{
    bounded::MemoryBudget, decoder::ColumnarSink, decoder::RecordParser, decoder::Splitter,
    parallel::chunk_profile, pipeline::Pipeline, plan::FieldType, value::Value, Result,
};

const ROWS: usize = 5_000_000;

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
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize> {
        if max_chunks <= 1 || bytes.is_empty() {
            return vec![0, bytes.len()];
        }
        let mut points = vec![0usize];
        let target = bytes.len() / max_chunks;
        for i in 1..max_chunks {
            let approx = i * target;
            // Find the next newline after the approximate boundary so each
            // chunk ends on a row boundary. This gives roughly equal sizes
            // instead of the old first-K-newlines behaviour.
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
                    // Fast path: resolve once, then push with the resolved name.
                    // Avoids the wants() + put_field() double probe and the
                    // to_owned() fallback in the default resolve_and_put.
                    sink.resolve_and_put(k, Value::Str(Cow::Borrowed(v)));
                }
            }
            sink.end_row();
        }
        Ok(())
    }
}

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
                rss.total_kb = rest.split_whitespace().next().and_then(|n| n.parse().ok())?;
            } else if let Some(rest) = line.strip_prefix("RssAnon:") {
                rss.anon_kb = rest.split_whitespace().next().and_then(|n| n.parse().ok())?;
            } else if let Some(rest) = line.strip_prefix("RssFile:") {
                rss.file_kb = rest.split_whitespace().next().and_then(|n| n.parse().ok())?;
            }
        }
        return Some(rss);
    }
    #[allow(unreachable_code)]
    None
}

fn median_of(n: usize, f: impl Fn() -> Result<usize>) -> (f64, f64, usize) {
    let mut times: Vec<f64> = Vec::with_capacity(n);
    let mut last_rows = 0;
    for _ in 0..n {
        let start = Instant::now();
        last_rows = f().expect("benchmark failed");
        times.push(start.elapsed().as_secs_f64());
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let med = times[n / 2];
    let mean = times.iter().sum::<f64>() / n as f64;
    let stdev = (times.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / n as f64).sqrt();
    let cov = stdev / mean;
    (med, cov, last_rows)
}

fn run(name: &str, input_bytes: usize, rounds: usize, f: impl Fn() -> Result<usize>) {
    let (med, cov, rows) = median_of(rounds, &f);
    let rows_per_sec = rows as f64 / med;
    let mb_per_sec = input_bytes as f64 / med / 1_000_000.0;
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
    println!(
        "{name:24} {med:8.3}s median ({cov_pct:.1}% CoV)  {rows:10} rows  {:8.0} rows/s  {:6.1} MB/s  RSS {memory}",
        rows_per_sec, mb_per_sec
    );
}

fn main() -> Result<()> {
    let data = generate_data(ROWS);
    let bytes = data.len();
    let mb = bytes as f64 / 1_000_000.0;
    println!("Generated {ROWS} rows ({mb:.1} MB)");

    let mut path = std::env::temp_dir();
    path.push("rypipe_bench_throughput.tsv");
    File::create(&path)?.write_all(&data)?;

    let pipeline = Pipeline::new(TsvSplitter, TsvParser).with_plan(
        rypipe_core::ExecutionPlan::new()
            .type_as("id", FieldType::Int64)
            .type_as("amount", FieldType::Float64)
            .type_as("count", FieldType::Int64),
    );

    let rounds = 7;
    println!("\n({rounds} rounds each, median reported)\n");

    run("single-thread", bytes, rounds, || {
        let batch = pipeline.read_path(&path, false, false)?;
        Ok(batch.num_rows())
    });

    run("parallel (4 chunks)", bytes, rounds, || {
        let batches = pipeline.read_path_par(&path, 4, false, false)?;
        let (split_ns, sum_ns, max_ns, count) = chunk_profile();
        eprintln!("    [par4 profile] split={:.2}ms chunks_sum={:.2}ms max={:.2}ms count={}",
                  split_ns as f64 / 1e6, sum_ns as f64 / 1e6, max_ns as f64 / 1e6, count);
        Ok(batches.iter().map(|b| b.num_rows()).sum::<usize>())
    });

    run("parallel (8 chunks)", bytes, rounds, || {
        let batches = pipeline.read_path_par(&path, 8, false, false)?;
        let (split_ns, sum_ns, max_ns, count) = chunk_profile();
        eprintln!("    [par8 profile] split={:.2}ms chunks_sum={:.2}ms max={:.2}ms count={}",
                  split_ns as f64 / 1e6, sum_ns as f64 / 1e6, max_ns as f64 / 1e6, count);
        Ok(batches.iter().map(|b| b.num_rows()).sum::<usize>())
    });

    run("bounded (64 MiB)", bytes, rounds, || {
        let batches =
            pipeline.read_path_stream(&path, MemoryBudget::new(64 * 1024 * 1024), false)?;
        Ok(batches.iter().map(|b| b.num_rows()).sum::<usize>())
    });

    std::fs::remove_file(&path)?;
    Ok(())
}
