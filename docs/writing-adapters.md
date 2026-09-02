# Writing a format adapter

A `rypipe` adapter is just two small types: a `Splitter` and a `RecordParser`.
Once you have those, the `Pipeline` API wires them into one-file, parallel, and
bounded-memory execution with one line of code.

Adapters are **separate packages**, not part of `rypipe`. This keeps `rypipe`
pure: it is only the ingestion-to-Arrow engine. Your adapter crate depends on
`rypipe-core` and, if you want Python bindings, on `rypipe-python` for the
plan/export helpers.

## Adapter crate layout

```
rypipe-csv/
├── Cargo.toml
├── pyproject.toml            # optional, for a Python package
└── src/
    ├── lib.rs
    ├── splitter.rs
    └── decoder.rs
```

```toml
[dependencies]
rypipe-core = "0.1"

# Only if you build Python bindings for the adapter:
rypipe-python = "0.1"
pyo3 = { version = "0.29", features = ["extension-module", "abi3-py310"] }
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
                // Point at the FIRST BYTE of the next record, not at the
                // newline itself; otherwise chunks begin with a stray '\n'.
                points.push(i + 1);
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
- Each chunk must start at a valid row boundary. Split points point at the
  first byte of a record, never at the delimiter itself.

## Implement `RecordParser`

```rust
use std::borrow::Cow;
use rypipe_core::{RecordParser, ColumnarSink, Value, Result};

pub struct CsvDecoder {
    header: Vec<String>,
}

impl RecordParser for CsvDecoder {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text = simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            sink.begin_row();
            for (col, value) in self.header.iter().zip(line.split(',')) {
                if sink.wants(col) {
                    sink.put_field(col, Value::Str(Cow::Borrowed(value)));
                }
            }
            sink.end_row();
        }
        Ok(())
    }

    // Fastest when extraction is expensive : one resolve instead of two:
    // (requires rypipe-core >=0.1.1)
    fn parse_chunk_fast(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text = simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            sink.begin_row();
            for (col, value) in self.header.iter().zip(line.split(',')) {
                if let Some(resolved) = sink.resolve(col) {
                    // ... expensive unescaping / decoding of `value` here ...
                    sink.put_field_resolved(resolved, Value::Str(Cow::Borrowed(value)));
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
  When extraction itself is expensive (entity unescaping, base64, …), use the
  zero-copy pair `sink.resolve(name)` + `sink.put_field_resolved(resolved, value)`
  to pay the `rename→drop` hash only once instead of twice.
- Emit `Value::Str` for stringly formats; emit typed `Value` variants when the
  format has native numbers/booleans. `Value::Str` wraps a `Cow<'_, str>`:
  borrow from the input buffer when possible, and move an owned `String` in
  when extraction allocates (entity unescaping, base64 decode, …); the
  buffered filter path may hold values past the end of your parse function,
  so a borrow of a temporary would dangle.
- Do not call `end_row()` for partial trailing rows; the engine will discard
  them.
- Columns are now stored as `Vec<ColumnBuilder>` with a `field_index: FxHashMap<String,usize>`
  beside `column_order`. The hot path `push_field` is a single index lookup
  (`field_index.get` → `columns[idx]`) instead of two `HashMap` probes; this
  benefits every adapter equally.

## Run it with `Pipeline`

`Pipeline` is the recommended entry point. It handles file opening, plan
application, and all execution modes.

```rust
use rypipe_core::{ExecutionPlan, FieldType, Pipeline};

let pipeline = Pipeline::new(CsvSplitter, CsvDecoder {
    header: vec!["a".into(), "b".into()],
});

// Single-file parse.
let batch = pipeline.read_path("data.csv", false, false)?;

// Parallel parse.
let batches = pipeline.read_path_par("data.csv", 4, false, false)?;

// Bounded-memory streaming.
let batches = pipeline.read_path_stream(
    "huge.csv",
    rypipe_core::MemoryBudget::new(128 * 1024 * 1024),
    false,
)?;
```

## Pushdown plans with the builder API

```rust
use rypipe_core::{CompareOp, ExecutionPlan, FieldType};

let plan = ExecutionPlan::new()
    .rename("raw_amount", "amount")
    .drop("internal_id")
    .type_as("amount", FieldType::Float64)
    .type_as("quantity", FieldType::Int64)
    .dictionary("status")
    .filter_eq("status", "active")
    .schema_order(["quantity", "amount", "status"]);

let batch = pipeline.with_plan(plan).read_path("data.csv", false, false)?;
```

## Adding Python bindings

Your adapter package can expose its own Python module. Reuse `rypipe-python`
for the plan and export helpers:

```rust
use rypipe_python::{execution_plan_from_kwargs, record_batches_to_pyarrow_table};

#[pyfunction]
fn read_csv<'py>(
    py: Python<'py>,
    path: String,
    field_mapping: Option<HashMap<String, String>>,
    // ... other kwargs
) -> PyResult<Bound<'py, PyAny>> {
    let plan = execution_plan_from_kwargs(...)?;
    let batches = py.allow_threads(|| {
        // ... use Pipeline::read_path_par or BoundedExecutor
    })?;
    record_batches_to_pyarrow_table(py, &batches)
}
```

Then register the adapter with `rypipe` from Python:

```python
import rypipe

class CsvAdapter:
    def read(self, path, **kwargs):
        return _rypipe_csv.read_csv(path, **kwargs)

rypipe.register_adapter("csv", CsvAdapter(), extensions=[".csv"])
```

## Python `Adapter` subclass (pipeline API)

If your adapter returns a `pyarrow.Table`, the easiest way to expose a source
is to subclass `rypipe.Adapter` and implement ``read(self, path, **kwargs)``.
Plan kwargs are merged and passed through automatically::

```python
import rypipe
from rypipe import Adapter
import _rypipe_csv

class CsvAdapter(Adapter):
    def __init__(self, path, *, delimiter=",", **kwargs):
        super().__init__(path, **kwargs)
        self._delimiter = delimiter

    def read(self, path, **kwargs):
        return _rypipe_csv.read_csv(
            path, delimiter=self._delimiter, **kwargs
        )
```

For adapters that need full control, subclass `rypipe.Source` directly and
override `_read_arrow` and `_build_plan_kwargs`.

Users can now write::

```python
from rypipe import RenameFields, DropFields, FilterRows, CastTypes

src = CsvSource("data.csv")
df = (
    src
    | RenameFields({"old_name": "new_name"})
    | DropFields(["internal_id"])
    | FilterRows(field="status", op="==", value="active")
    | CastTypes({"amount": float})
).to_dataframe()
```

`RenameFields`, `DropFields`, `CastTypes`, and `FilterRows` predicates (both
constant and column-to-column forms) expose `_plan_kwargs()` so the pipeline
fusion layer pushes them into `_read_arrow`. Your Rust `read_csv` receives the
merged kwargs and applies them in the parse loop. Non-fusable stages (custom
callables, stateful transforms) run over the returned table automatically.

## Fast delimiter scanning with `BlockMasks` (engine asset)

36.5% of parse is `memchr` on 50-100 byte spans (5-7 searches per field). `BlockMasks`
computes delimiter positions once per 64-byte block and answers all queries from
cached bitmasks - an engine asset, not a crxml trick.

```rust
use rypipe_core::block_masks::BlockMasks;

const DELIMS: &[u8] = &[b',', b'"', b'\n', b'\r']; // CSV
let mut masks = BlockMasks::new(buf, DELIMS);
let comma = masks.next(pos, b',');
let quote = masks.next_any(pos, &[b'"', b'\'']);
let far = masks.next_far(pos, b'\n', 400); // >256 → memchr fallback
```

* `BlockMasks::new(buf, delims)` - `delims` static ≤8, e.g. crxml `&[b'<',b'>',b'"',b'\'',b'=']` (MAX_DELIMS=8 covers CSV `, " \n \r` and JSONL `{ } " \n \ \`)
* Lazy per-delimiter: first query for `','` costs 2×AVX2 loads (64B), next 4 queries reuse the cached mask (`tzcnt` only). 5 searches → 20 instr vs 5× memchr prologue.
* `next_any` ORs masks, one `tzcnt` for `memchr3` replacement (`scan_open_tag`).
* Tail: final partial block copied to 64B stack array, masked off.
* AVX2 (`_mm256_cmpeq_epi8`+`movemask`) → SSE2 (4×16B) → scalar, runtime dispatch via `is_x86_feature_detected!("avx2")` cached in `OnceLock` (same as `memchr`, no manylinux question).
* `next_far(hint>256)` falls back to `memchr` for long spans (between rows).

Crxml prototype (P2 `next_lt` only) measured **-68% on traverse** (0.574→0.968 ms/MB) - short 50B spans don't amortize; `memchr` already optimal there. Full integration (P3-P4 `scan_open_tag`/`raw_text_until` candidate+verify) is available but gated. For CSV/JSONL with 1KB rows and 5-7 searches per field on the same block, the win should appear; use the P1 microbench (`cargo test -p rypipe-core block_masks -lib`) to gate.

## Testing an adapter

Recommended tests:

- Empty input.
- Single row.
- Multi-row with all field types.
- Rename/drop/type/filter pushdown via `ExecutionPlan`.
- Splitter invariants (monotonic points, no inverted ranges, coverage).
- Multi-chunk equivalence: parse whole file vs. split + merge.
- Partial trailing row discarded cleanly.
- `Pipeline::read_path`, `read_path_par`, and `read_path_stream` agree.
- `BlockMasks` equivalence: `masks.next(pos,d) == memchr(d, &buf[pos..])` (property test in `block_masks.rs`).

See the `rypipe-core` tests and the `bench_throughput` example for small
self-contained splitter/parser samples.

## See also

- [Rust API](./rust-api.md): `Pipeline`, `ExecutionPlan`, and `Value`.
- [Architecture](./architecture/): how splitters, parsers, and the engine interact.
- [Python API](./python-api.md): registering adapters with the `rypipe` package.
- [crxml adapter](./crxml-adapter.md): a production adapter that reaches 2.4 GB/s on Crystal Reports XML.
