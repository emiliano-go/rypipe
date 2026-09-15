use std::borrow::Cow;
use std::hint::black_box;
use std::io::{self, Write};
use std::time::Instant;

use arrow::array::AsArray;
use arrow::record_batch::RecordBatch;
use rypipe_core::{
    consumer::BatchConsumer, find_next_record_boundary, ColumnarSink, InputBuffer, MemoryBudget,
    Pipeline, RecordParser, Splitter, Value,
};

#[derive(Clone)]
struct Records(bool);

impl Splitter for Records {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        find_next_record_boundary(bytes, from, self.0.then_some(b'\\'), &[], false)
    }

    fn estimate_bytes_per_row(&self, _: &[u8]) -> usize {
        if self.0 {
            131
        } else {
            129
        }
    }
}

impl RecordParser for Records {
    fn validate(&self, bytes: &[u8]) -> rypipe_core::Result<()> {
        simdutf8::basic::from_utf8(bytes)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> rypipe_core::Result<()> {
        let text = simdutf8::basic::from_utf8(bytes)?;
        let mut lines = text.lines();
        while let Some(line) = lines.next() {
            let value = if self.0 {
                Cow::Owned(format!(
                    "{}{}",
                    line.strip_suffix('\\').unwrap(),
                    lines.next().unwrap()
                ))
            } else {
                Cow::Borrowed(line)
            };
            sink.begin_row();
            sink.put_field("value", Value::Str(value));
            sink.end_row();
        }
        Ok(())
    }
}

#[derive(Default)]
struct Check {
    rows: usize,
    payload: usize,
    max_batch: usize,
}

impl BatchConsumer for Check {
    fn consume(&mut self, batch: RecordBatch) -> rypipe_core::Result<()> {
        self.max_batch = self.max_batch.max(batch.get_array_memory_size());
        for value in batch.column(0).as_string::<i32>().iter() {
            let value = value.unwrap();
            assert_eq!(value.len(), 128);
            assert!(value.bytes().all(|byte| byte == b'x'));
            self.rows += 1;
            self.payload += value.len();
        }
        Ok(())
    }
}

fn boundaries(bytes: &[u8]) {
    let scan = |declarative| {
        let start = Instant::now();
        let mut from = 0;
        let mut sum = 0usize;
        loop {
            let next = if declarative {
                find_next_record_boundary(black_box(bytes), from, None, &[], false)
            } else {
                memchr::memchr(b'\n', black_box(&bytes[from..])).map(|at| from + at + 1)
            };
            let Some(next) = next else { break };
            from = next;
            sum = sum.wrapping_add(next);
        }
        (start.elapsed().as_secs_f64(), black_box(sum))
    };
    let mut declarative = Vec::new();
    let mut manual = Vec::new();
    for run in 0..7 {
        let (d, m) = if run % 2 == 0 {
            (scan(true), scan(false))
        } else {
            let m = scan(false);
            (scan(true), m)
        };
        assert_eq!(d.1, m.1);
        declarative.push(d.0);
        manual.push(m.0);
    }
    declarative.sort_by(f64::total_cmp);
    manual.sort_by(f64::total_cmp);
    println!(
        "{{\"declarative_seconds\":{},\"manual_seconds\":{},\"ratio\":{}}}",
        declarative[3],
        manual[3],
        declarative[3] / manual[3]
    );
}

fn main() -> rypipe_core::Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("boundary");
    let Some(path) = args.get(2) else {
        eprintln!("usage: engine_probe <boundary|bytes|parallel|mmap|stream> FILE [continued]");
        return Ok(());
    };
    let path = std::path::Path::new(path);
    let continued = args.get(3).is_some_and(|arg| arg == "continued");
    let bytes = std::fs::metadata(path)?.len() as usize;
    let input = if matches!(mode, "bytes" | "parallel" | "boundary") {
        Some(InputBuffer::open(path, false, false)?)
    } else {
        None
    };
    println!("ready");
    io::stdout().flush()?;
    io::stdin().read_line(&mut String::new())?;
    if mode == "boundary" {
        boundaries(input.as_ref().unwrap().as_slice());
        return Ok(());
    }
    let pipeline = Pipeline::new(Records(continued), Records(continued));
    let mut check = Check::default();
    let start = Instant::now();
    match mode {
        "bytes" => check.consume(pipeline.read_bytes(input.as_ref().unwrap().as_slice())?)?,
        "parallel" => {
            for batch in pipeline.read_bytes_par(input.as_ref().unwrap().as_slice(), 4)? {
                check.consume(batch)?;
            }
        }
        "mmap" => check.consume(pipeline.read_path(path, true, false)?)?,
        "stream" => pipeline.read_path_stream_consumer(
            path,
            MemoryBudget::new(10 * 1024 * 1024),
            false,
            &mut check,
        )?,
        _ => panic!("unknown mode: {mode}"),
    }
    assert_eq!(check.rows, bytes / if continued { 131 } else { 129 });
    println!(
        "{{\"seconds\":{},\"rows\":{},\"payload_bytes\":{},\"max_batch_bytes\":{}}}",
        start.elapsed().as_secs_f64(),
        check.rows,
        check.payload,
        check.max_batch
    );
    Ok(())
}
