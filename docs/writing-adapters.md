# Writing a format adapter

A `rypipe` adapter is just two small types: a `Splitter` and a `RecordParser`.
Once you have those, the existing engine (`TableBuilder`, `ParallelExecutor`,
`BoundedExecutor`) handles the rest.

## Adapter crate layout

```
crates/rypipe-csv/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── splitter.rs
    └── decoder.rs
```

```toml
[dependencies]
rypipe-core = { path = "../rypipe-core" }
memchr = "2"
```

## Implement `Splitter`

The splitter finds safe chunk boundaries. For CSV that means newline outside
quotes; for JSONL it is just newline; for JSON arrays it means brace balance.

```rust
use rypipe_core::Splitter;

pub struct CsvSplitter;

impl Splitter for CsvSplitter {
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize> {
        if max_chunks <= 1 || bytes.is_empty() {
            return vec![0, bytes.len()];
        }
        let mut points = vec![0];
        let mut in_quotes = false;
        for (i, &b) in bytes.iter().enumerate().skip(1) {
            if b == b'"' {
                in_quotes = !in_quotes;
            } else if b == b'\n' && !in_quotes && points.len() < max_chunks {
                points.push(i);
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
```

Rules:

- The first point must be `0`; the last must be `bytes.len()`.
- Adjacent equal points produce empty ranges that the engine ignores.
- Each chunk must start at a valid row boundary.

## Implement `RecordParser`

```rust
use rypipe_core::{RecordParser, ColumnarSink, Value, Result};

pub struct CsvDecoder {
    header: Vec<String>,
}

impl RecordParser for CsvDecoder {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Utf8(e.to_string()))?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            sink.begin_row();
            for (col, value) in self.header.iter().zip(line.split(',')) {
                if sink.wants(col) {
                    sink.put_field(col, Value::Str(value));
                }
            }
            sink.end_row();
        }
        Ok(())
    }
}
```

Key points:

- Call `sink.wants(name)` before expensive extraction to skip dropped fields.
- Emit `Value::Str` for stringly formats; emit typed `Value` variants when the
  format has native numbers/booleans.
- Do not call `end_row()` for partial trailing rows; the engine will discard
  them.

## Wire it into the engine

```rust
use rypipe_core::{InputBuffer, TableBuilder, ExecutionPlan, parallel::ParallelExecutor};
use rypipe_csv::{CsvDecoder, CsvSplitter};

let input = InputBuffer::open("data.csv".as_ref(), false, false)?;
let splitter = CsvSplitter;
let decoder = CsvDecoder { header: vec!["a".into(), "b".into()] };
let batches = ParallelExecutor::parse(
    input.as_slice(),
    &splitter,
    decoder,
    ExecutionPlan::new(),
    4,
)?;
```

## Adding Python bindings

If you want a standalone `_rypipe_csv` extension, create a PyO3 crate that:

1. Builds an `ExecutionPlan` from kwargs (reuse `rypipe_python::plan_kwargs`).
2. Calls `ParallelExecutor::parse` or `BoundedExecutor::run`.
3. Exports via `rypipe_python::export::record_batches_to_pyarrow_table`.

Or add the new adapter behind a format selector in `rypipe-python` itself.

## Testing an adapter

Recommended tests:

- Empty input.
- Single row.
- Multi-row with all field types.
- Rename/drop/type/filter pushdown via `ExecutionPlan`.
- Splitter invariants (monotonic points, no inverted ranges, coverage).
- Multi-chunk equivalence: parse whole file vs. split + merge.
- Partial trailing row discarded cleanly.

See `rypipe-xml/tests/integration_test.rs` for a concrete example.
