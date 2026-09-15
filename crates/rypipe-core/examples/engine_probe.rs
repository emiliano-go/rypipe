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
struct Records {
    kind: String,
    width: usize,
    fields: Vec<String>,
}

impl Splitter for Records {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        find_next_record_boundary(
            bytes,
            from,
            (self.kind == "continued").then_some(b'\\'),
            if self.kind == "comments" {
                &[b"#"]
            } else {
                &[]
            },
            self.kind == "blank",
        )
    }

    fn estimate_bytes_per_row(&self, _: &[u8]) -> usize {
        self.width
            + match self.kind.as_str() {
                "continued" => 3,
                "comments" => 11,
                "blank" => 2,
                _ => 1,
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
            if line.is_empty() || (self.kind == "comments" && line.starts_with('#')) {
                continue;
            }
            let value = if self.kind == "continued" {
                Cow::Owned(format!(
                    "{}{}",
                    line.strip_suffix('\\').unwrap(),
                    lines.next().unwrap()
                ))
            } else {
                Cow::Borrowed(line)
            };
            sink.begin_row();
            for field in &self.fields {
                sink.put_field(field, Value::Str(Cow::Borrowed(value.as_ref())));
            }
            sink.end_row();
        }
        Ok(())
    }
}

#[derive(Default)]
struct Check {
    width: usize,
    columns: usize,
    rows: usize,
    payload: usize,
    max_batch: usize,
}

impl BatchConsumer for Check {
    fn consume(&mut self, batch: RecordBatch) -> rypipe_core::Result<()> {
        self.max_batch = self.max_batch.max(batch.get_array_memory_size());
        assert_eq!(batch.num_columns(), self.columns);
        for column in batch.columns() {
            for value in column.as_string::<i32>().iter() {
                let value = value.unwrap();
                assert_eq!(value.len(), self.width);
                assert!(value.bytes().all(|byte| byte == b'x'));
                self.payload += value.len();
            }
        }
        self.rows += batch.num_rows();
        Ok(())
    }
}

fn manual_boundary(bytes: &[u8], mut from: usize, kind: &str) -> Option<usize> {
    loop {
        let newline = from + memchr::memchr(b'\n', bytes.get(from..)?)?;
        match kind {
            "continued" => {
                let end = newline - usize::from(newline > 0 && bytes[newline - 1] == b'\r');
                let slashes = bytes[..end]
                    .iter()
                    .rev()
                    .take_while(|&&b| b == b'\\')
                    .count();
                if slashes % 2 == 0 {
                    return Some(newline + 1);
                }
            }
            "comments" => {
                let start = memchr::memrchr(b'\n', &bytes[..newline]).map_or(0, |i| i + 1);
                if !bytes[start..newline].starts_with(b"#") {
                    return Some(newline + 1);
                }
            }
            "blank" => {
                let end = newline - usize::from(newline > 0 && bytes[newline - 1] == b'\r');
                if end > 0 && bytes[end - 1] == b'\n' {
                    return Some(newline + 1);
                }
            }
            _ => return Some(newline + 1),
        }
        from = newline + 1;
    }
}

fn boundaries(bytes: &[u8], records: &Records) {
    type Boundary = fn(&[u8], usize) -> Option<usize>;
    let (declarative_boundary, reference_boundary): (Boundary, Boundary) =
        match records.kind.as_str() {
            "continued" => (
                |b, f| find_next_record_boundary(b, f, Some(b'\\'), &[], false),
                |b, f| manual_boundary(b, f, "continued"),
            ),
            "comments" => (
                |b, f| find_next_record_boundary(b, f, None, &[b"#"], false),
                |b, f| manual_boundary(b, f, "comments"),
            ),
            "blank" => (
                |b, f| find_next_record_boundary(b, f, None, &[], true),
                |b, f| manual_boundary(b, f, "blank"),
            ),
            _ => (
                |b, f| find_next_record_boundary(b, f, None, &[], false),
                |b, f| manual_boundary(b, f, "plain"),
            ),
        };
    let mut from = 0;
    loop {
        let expected = reference_boundary(bytes, from);
        assert_eq!(declarative_boundary(bytes, from), expected);
        let Some(next) = expected else { break };
        from = next;
    }
    let scan = |boundary: Boundary| {
        let start = Instant::now();
        let mut from = 0;
        let mut sum = 0usize;
        loop {
            let next = boundary(black_box(bytes), from);
            let Some(next) = next else { break };
            from = next;
            sum = sum.wrapping_add(next);
        }
        (start.elapsed().as_secs_f64(), black_box(sum))
    };
    let mut declarative = Vec::new();
    let mut manual = Vec::new();
    let mut ratios = Vec::new();
    for run in 0..7 {
        let (d, m) = if run % 2 == 0 {
            (scan(declarative_boundary), scan(reference_boundary))
        } else {
            let m = scan(reference_boundary);
            (scan(declarative_boundary), m)
        };
        assert_eq!(d.1, m.1);
        declarative.push(d.0);
        manual.push(m.0);
        ratios.push(d.0 / m.0);
    }
    declarative.sort_by(f64::total_cmp);
    manual.sort_by(f64::total_cmp);
    ratios.sort_by(f64::total_cmp);
    println!(
        "{{\"declarative_seconds\":{},\"manual_seconds\":{},\"ratio\":{}}}",
        declarative[3], manual[3], ratios[3]
    );
}

fn main() -> rypipe_core::Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("boundary");
    let Some(path) = args.get(2) else {
        eprintln!("usage: engine_probe MODE FILE [plain|continued|comments|blank] [WIDTH] [BUDGET_BYTES] [COLUMNS]");
        return Ok(());
    };
    let path = std::path::Path::new(path);
    let kind = args.get(3).cloned().unwrap_or_else(|| "plain".into());
    assert!(matches!(
        kind.as_str(),
        "plain" | "continued" | "comments" | "blank"
    ));
    let width = args.get(4).map_or(128, |v| v.parse().unwrap());
    let budget = args.get(5).map_or(10 * 1024 * 1024, |v| v.parse().unwrap());
    let columns = args.get(6).map_or(1, |v| v.parse().unwrap());
    assert!(width > 0 && columns > 0 && budget > 0);
    let records = Records {
        kind,
        width,
        fields: (0..columns).map(|i| format!("value{i}")).collect(),
    };
    let bytes = std::fs::metadata(path)?.len() as usize;
    let input = if matches!(mode, "bytes" | "parallel" | "boundary" | "stream-bytes") {
        Some(InputBuffer::open(path, false, false)?)
    } else {
        None
    };
    println!("ready");
    io::stdout().flush()?;
    io::stdin().read_line(&mut String::new())?;
    if mode == "boundary" {
        boundaries(input.as_ref().unwrap().as_slice(), &records);
        return Ok(());
    }
    let record_bytes = records.estimate_bytes_per_row(&[]);
    let pipeline = Pipeline::new(records.clone(), records.clone());
    let mut check = Check {
        width,
        columns,
        ..Check::default()
    };
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
            MemoryBudget::new(budget),
            false,
            &mut check,
        )?,
        "stream-bytes" => pipeline.read_bytes_stream_consumer(
            input.as_ref().unwrap().as_slice(),
            MemoryBudget::new(budget),
            &mut check,
        )?,
        "iterator" => {
            for batch in rypipe_core::streaming::StreamingBatchIterator::new(
                path.to_path_buf(),
                records.clone(),
                records,
                std::sync::Arc::new(rypipe_core::ExecutionPlan::new()),
                MemoryBudget::new(budget),
                false,
            ) {
                check.consume(batch?)?;
            }
        }
        "stream-parallel" => {
            for batch in pipeline.read_path_stream_par(
                path,
                MemoryBudget::new(budget),
                false,
                rypipe_core::parallel_stream::ParallelStreamOpts {
                    threads: 4,
                    ..Default::default()
                },
            )? {
                check.consume(batch?)?;
            }
        }
        _ => panic!("unknown mode: {mode}"),
    }
    assert_eq!(check.rows, bytes / record_bytes);
    println!(
        "{{\"seconds\":{},\"rows\":{},\"payload_bytes\":{},\"max_batch_bytes\":{}}}",
        start.elapsed().as_secs_f64(),
        check.rows,
        check.payload,
        check.max_batch
    );
    Ok(())
}
