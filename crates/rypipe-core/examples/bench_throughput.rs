//! Standalone throughput benchmark for the rypipe-core engine.
//!
//! This example uses a tiny inline TSV-like adapter so the benchmark measures
//! the engine itself, not any particular external parser. Run with:
//!
//!     cargo run --release -p rypipe-core --example bench_throughput

use std::fs::File;
use std::io::Write;
use std::time::Instant;

use rypipe_core::{
    bounded::MemoryBudget, decoder::ColumnarSink, decoder::RecordParser, decoder::Splitter,
    pipeline::Pipeline, plan::FieldType, value::Value, Result,
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
        let mut last = 0;
        for (i, &b) in bytes.iter().enumerate() {
            if b == b'\n' {
                let next = i + 1;
                if next > last && points.len() < max_chunks {
                    points.push(next);
                    last = next;
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
                    if sink.wants(k) {
                        sink.put_field(k, Value::Str(v));
                    }
                }
            }
            sink.end_row();
        }
        Ok(())
    }
}

fn run(name: &str, f: impl FnOnce() -> Result<usize>) {
    let start = Instant::now();
    let rows = f().expect("benchmark failed");
    let elapsed = start.elapsed();
    let secs = elapsed.as_secs_f64();
    println!(
        "{name:24} {secs:8.3}s  {rows:10} rows  {:8.0} rows/s",
        rows as f64 / secs
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

    run("single-thread", || {
        let batch = pipeline.read_path(&path, false, false)?;
        Ok(batch.num_rows())
    });

    run("parallel (4 chunks)", || {
        let batches = pipeline.read_path_par(&path, 4, false, false)?;
        Ok(batches.iter().map(|b| b.num_rows()).sum::<usize>())
    });

    run("parallel (8 chunks)", || {
        let batches = pipeline.read_path_par(&path, 8, false, false)?;
        Ok(batches.iter().map(|b| b.num_rows()).sum::<usize>())
    });

    run("bounded (64 MiB)", || {
        let batches =
            pipeline.read_path_stream(&path, MemoryBudget::new(64 * 1024 * 1024), false)?;
        Ok(batches.iter().map(|b| b.num_rows()).sum::<usize>())
    });

    std::fs::remove_file(&path)?;
    Ok(())
}
