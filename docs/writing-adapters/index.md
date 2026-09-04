# Writing a Format Adapter

A `rypipe` adapter is two small types: a **Splitter** and a **RecordParser**.
Once you have those, the engine wires them into single-file, parallel, and
bounded-memory execution with one line of code.

Adapters are **separate packages** that depend on `rypipe-core`. They are not
part of `rypipe` itself. This keeps `rypipe` pure: it is only the
ingestion-to-Arrow engine.

## Architecture

### Data flow

```
Input bytes (file or mmap)
  |
  v
Splitter::next_record_start    (find safe chunk boundaries)
  |
  v  [one chunk]
RecordParser::parse_chunk      (per-chunk, feeds ColumnarSink)
  |  calls: begin_row -> put_field x N -> end_row
  v
ColumnarSink (TableBuilder)    (accumulates typed columns)
  |
  v
Arrow RecordBatch              (zero-copy export)
  |
  v
pyarrow.Table                  (Python API)
```

### What the engine handles

The engine handles all format-agnostic work. Your adapter only provides
the format-specific parsing logic:

- **Parallel execution** via rayon: split the file, parse chunks concurrently
  on multiple threads. Each chunk gets its own `TableBuilder`.
- **Bounded-memory streaming**: parsing memory is independent of file size.
  The engine processes one chunk at a time, keeping only the current chunk
  in memory.
- **Pushdown plans**: rename, drop, filter, type coercion, dictionary
  encoding. These are pushed into the Rust parse loop when the adapter
  supports fusion.
- **Zero-copy Arrow export**: move column buffers directly into Arrow arrays.
  No serialization, no copying.
- **Schema discovery**: find field names from a sample of the file. For
  files >128 MiB, samples 16x2 MiB windows in parallel.
- **Predicate-first evaluation**: reject rows before scanning all fields.
  The `row_satisfied` byte-jump skips remaining fields once all wanted
  columns arrive.
- **Memory budgets**: configurable memory limits. The engine splits large
  files into chunks that fit within the budget.
- **Dictionary encoding**: automatic or explicit low-cardinality string
  encoding. Reduces memory 5-20x for columns like status codes.

See [Architecture](../architecture/) for how these pieces interact internally.

### What your adapter provides

Your adapter provides two things:

1. **Splitter**: tells the engine where rows start
2. **RecordParser**: extracts field values from each row

Everything else (column building, Arrow export, parallel scheduling,
memory management) is handled by the engine.

## The three traits

| Trait | Purpose | Required methods |
|-------|---------|-----------------|
| [`Splitter`](./splitter.md) | Find safe chunk boundaries | `next_record_start`, `estimate_bytes_per_row` |
| [`RecordParser`](./parser.md) | Turn bytes into field/value events | `validate`, `parse_chunk` |
| [`ColumnarSink`](./sink.md) | Accumulate values into Arrow columns | `begin_row`, `put_field`, `end_row`, `finish` |

The engine provides `TableBuilder` as the production `ColumnarSink`. You rarely
implement `ColumnarSink` yourself unless writing a profiling harness or a
custom streaming adapter.

### The Splitter

The Splitter finds where each row starts in the byte stream. The engine
calls `next_record_start` repeatedly to find chunk boundaries:

```
pos = 0
while let Some(next) = splitter.next_record_start(bytes, pos) {
    // bytes[pos..next] is one chunk
    pos = next
}
```

For line-based formats, find the next newline. For XML, find the next row
tag. See [Splitter](./splitter.md) for details.

### The RecordParser

The RecordParser turns raw bytes into field/value events. The engine calls
`parse_chunk` once per chunk. Your implementation must:

1. Iterate over rows in the chunk
2. For each row, call `sink.begin_row()`
3. For each field, call `sink.put_field(name, value)`
4. Call `sink.end_row()`

See [Parser](./parser.md) for details.

### The ColumnarSink

The ColumnarSink accumulates values into Arrow columns. The engine provides
`TableBuilder` as the production implementation. You call its methods from
your `parse_chunk` implementation. See [Sink](./sink.md) for the full
method reference.

## Performance

| Page | What you learn |
|------|---------------|
| [Schema](./schema.md) | The biggest performance lever: declare columns upfront |
| [Techniques](./techniques.md) | 10 optimizations for production adapters |
| [Anti-patterns](./anti-patterns.md) | 15 common mistakes and how to fix them |

### The performance model

The hot path is:

```
parse_chunk -> begin_row -> [put_field x N] -> end_row -> [repeat]
```

Each `put_field` call goes through:

1. **Scan**: find the field's byte extent in the input (your parser does this)
2. **Resolve**: map raw name to output column name (engine does this)
3. **Push**: write the value into the column builder (engine does this)
4. **Filter**: check if the row passes the predicate (engine does this)

The engine optimizes steps 2-4. Your parser's job is to make step 1 fast.

### Performance budget

For a 533 MB file on a Ryzen 5800X:

| Phase | Budget | Your responsibility |
|-------|--------|-------------------|
| Splitting | ~5% | `next_record_start` must be fast |
| Validation | ~2% | UTF-8 check |
| Parsing | ~80% | `parse_chunk` is the hot path |
| Export | ~5% | Engine handles this |
| Schema | ~5% | Declare upfront if known |

### How to squeeze performance

1. **Declare schema upfront**: `schema_order` + `field_types` skip discovery,
   stabilize column order, and enable typed arrays (+80% with projection)
2. **Use `scan::find` instead of raw `memchr`**: adds O(1) fast path
3. **Borrow strings with `Cow::Borrowed`**: avoids allocation for non-entity text
4. **Check `wants()` before expensive extraction**: skip dropped fields entirely
5. **Use `resolve` + `put_field_resolved`**: single hash probe for expensive extraction
6. **Emit typed `Value` variants**: `Int64`/`Float64` skip string-to-number conversion
7. **Implement `skip_regions`**: let the engine reject false-positive split points
8. **Implement `parse_chunk_generic`**: devirtualized sink calls for hot paths

### Throughput numbers

With the crxml reference adapter on a Ryzen 5800X:

| Mode | Throughput | Notes |
|------|-----------|-------|
| Single-thread | ~950 MB/s | Columnar mode |
| Parallel (par128) | ~4,200 MB/s | 128 chunks of 4 MB |
| Streaming auto | ~4,500 MB/s | Bounded at 88 MB RSS |
| Streaming explicit schema | ~5,000 MB/s | +11% vs par128 |
| Streaming + projection | ~7,600 MB/s | row_satisfied byte-jump |

## Reference

| Page | What you learn |
|------|---------------|
| [Quick start](./quickstart.md) | Build a complete adapter from scratch (15 min) |
| [Python wiring](./python-wiring.md) | Wire Rust to Python with PyO3 |
| [Rust creation](./rust-creation.md) | Deep dive into Splitter, RecordParser, ColumnarSink |
| [Schema](./schema.md) | The biggest performance lever: declare columns upfront |
| [Techniques](./techniques.md) | 10 optimizations for production adapters |
| [Anti-patterns](./anti-patterns.md) | 20 common mistakes and how to fix them |
| [Splitter](./splitter.md) | `Splitter` trait: `next_record_start`, `estimate_bytes_per_row` |
| [Parser](./parser.md) | `RecordParser` trait: `validate`, `parse_chunk`, `parse_chunk_generic` |
| [Sink](./sink.md) | `ColumnarSink`: all 21 methods, fast paths, projection |
| [Scan primitives](./scan.md) | `find`, `find2`, `starts_with`, `find_literal` |
| [Skip regions](./skip-regions.md) | `SkipRegionFinder` for comments, CDATA, quoted fields |
| [Chunk planning](./chunk-planning.md) | `plan_chunk_count`, `MIN_CHUNK_BYTES`, thread caps |
| [Examples](./examples.md) | Worked CSV, JSONL, and TSV adapters |

## Hello World: complete adapter

Here is a minimal, complete adapter for a newline-delimited `key=value` log
format:

### Rust: `src/lib.rs`

```rust
use std::borrow::Cow;
use rypipe_core::{Splitter, RecordParser, ColumnarSink, Value, Result};

#[derive(Clone, Default)]
pub struct LogSplitter;

impl Splitter for LogSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        memchr::memchr(b'\n', &bytes[from..]).map(|r| from + r + 1)
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
        (sample.len() / n).max(1)
    }
}

#[derive(Clone, Default)]
pub struct LogParser;

impl RecordParser for LogParser {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes).map_err(|e| rypipe_core::Error::Utf8(e))?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
        for line in text.lines() {
            if line.is_empty() { continue; }
            sink.begin_row();
            for part in line.split(',') {
                if let Some((k, v)) = part.split_once('=') {
                    if sink.wants(k) {
                        sink.put_field(k, Value::Str(Cow::Borrowed(v)));
                    }
                }
            }
            sink.end_row();
        }
        Ok(())
    }
}
```

### Python: `my_adapter/__init__.py`

```python
import rypipe
import _rypipe_log

class LogAdapter:
    def read(self, path, **kwargs):
        return _rypipe_log.read_log(path, **kwargs)

rypipe.register_adapter("log", LogAdapter(), extensions=[".log"])
```

### Usage

```python
import rypipe
import my_adapter

table = rypipe.read("app.log")
```

## Adapter crate layout

```
rypipe-csv/
├── Cargo.toml
├── pyproject.toml            # optional, for a Python package
├── src/
│   ├── lib.rs                # PyO3 module + read functions
│   ├── splitter.rs           # Splitter implementation
│   └── parser.rs             # RecordParser implementation
└── my_csv/
    └── __init__.py           # Python adapter + registration
```

### `Cargo.toml`

```toml
[package]
name = "rypipe-csv"
version = "0.1.0"
edition = "2021"

[lib]
name = "_rypipe_csv"
crate-type = ["cdylib"]

[dependencies]
rypipe-core = "2"
pyo3 = { version = "0.29", features = ["extension-module", "abi3-py310"] }
memchr = "2"
simdutf8 = "0.1"
```

### Building

```bash
# Install maturin if not already installed
pip install maturin

# Build and install the extension
maturin develop --release

# Or build a wheel for distribution
maturin build --release
```

### Testing

```bash
# Rust tests
cargo test --workspace

# Python tests
pip install -e ".[dev]"
pytest tests/
```

## Registration and discovery

### How registration works

When your adapter calls `rypipe.register_adapter`, it adds your adapter
to a global registry:

```python
rypipe.register_adapter(
    "csv",              # name: used for format="csv" lookups
    CsvAdapter(),       # adapter: object with read() method
    extensions=[".csv"] # extensions: for auto-detection
)
```

### How discovery works

When a user calls `rypipe.read("data.csv")`, the engine:

1. Extracts the file extension (`.csv`)
2. Looks up the extension in the registry
3. Finds the adapter registered for `.csv`
4. Calls `adapter.read("data.csv", **kwargs)`

If no adapter is registered for the extension, it raises `RypipeError`.

### Registration timing

Register at module load time. The standard pattern is to call
`register_adapter` at the bottom of your `__init__.py`:

```python
# my_adapter/__init__.py
import rypipe
import _my_adapter

class MyAdapter:
    def read(self, path, **kwargs):
        return _my_adapter.read_file(path, **kwargs)

# Register when the module is imported
rypipe.register_adapter("myfmt", MyAdapter(), extensions=[".myfmt"])
```

Now `import my_adapter` registers the adapter, and `rypipe.read("file.myfmt")`
works automatically.

## Error handling

### Rust errors

The `rypipe_core::Error` enum covers parse errors:

```rust
pub enum Error {
    Utf8(std::str::Utf8Error),
    Io(std::io::Error),
    Plan(String),
    Merge(String),
    Arrow(arrow::error::ArrowError),
}
```

In your parser, return errors for invalid input:

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
    // ... parse text ...
    Ok(())
}
```

### Python exceptions

The Python API exposes these exceptions:

| Exception | Meaning |
|-----------|---------|
| `rypipe.ParseError` | Malformed input or parse failure |
| `rypipe.PlanError` | Invalid pushdown plan |
| `rypipe.MergeError` | Chunk-merge conflict |
| `rypipe.RypipeError` | Invalid API usage |

Users catch these with standard `try/except`:

```python
try:
    table = rypipe.read("bad_file.csv")
except rypipe.ParseError as e:
    print(f"Parse error: {e}")
```

## Common formats

### Line-based formats (CSV, TSV, JSONL)

Split on newlines. Each line is a row. Fields are delimited by a separator.

```
Splitter: find \n
Parser: split by separator, emit key=value pairs
```

### XML formats

Split on row tags. Each row is an XML element with child elements.

```
Splitter: find <Row> tags
Parser: walk child elements, extract field names and values
```

### Binary formats

Split on fixed-size records or length-prefixed headers.

```
Splitter: advance by record size
Parser: read fields at fixed offsets
```

### Nested formats (JSON, TOML)

May need to flatten nested structures before emitting.

```
Splitter: find top-level objects
Parser: flatten nested keys (e.g., "user.name" -> "name")
```

## Pure-Python adapters (no Rust)

You can write an adapter entirely in Python by subclassing `rypipe.Adapter`:

```python
import rypipe, pyarrow as pa

class CSVAdapter(rypipe.Adapter):
    def read(self, path, **kwargs):
        with open(path) as f:
            rows = [line.strip().split(",") for line in f if line.strip()]
        header = rows[0]
        data = {col: [rows[i+1][j] for i in range(len(rows)-1)]
                for j, col in enumerate(header)}
        return pa.table(data)

rypipe.register_adapter("csv", CSVAdapter(), extensions=[".csv"])
table = rypipe.read("data.csv")
```

> **Performance warning:** Pure-Python adapters run at Python speed, not Rust
> speed. The Rust `Splitter`/`RecordParser` traits deliver 4+ GB/s by parsing
> bytes directly into Arrow buffers with SIMD scanning, parallel chunking, and
> zero-copy export. A pure-Python adapter parses via Python loops and PyArrow
> compute, which is typically 10-50x slower. Use pure Python for correctness
> and prototyping; use Rust for throughput.

## Checklist for new adapters

Before releasing your adapter:

- [ ] `Splitter` implements `next_record_start` and `estimate_bytes_per_row`
- [ ] `RecordParser` implements `validate` and `parse_chunk`
- [ ] `parse_chunk` checks `sink.wants()` before expensive extraction
- [ ] `parse_chunk` uses `Cow::Borrowed` for string values
- [ ] Adapter registers with `rypipe.register_adapter`
- [ ] `read()` returns `pyarrow.Table`
- [ ] Empty input is handled gracefully
- [ ] Invalid UTF-8 returns an error (not panic)
- [ ] Tests cover single-row, multi-row, and empty input
- [ ] Performance is benchmarked on representative data

## How the engine calls your adapter

### Splitting phase

The engine calls `Splitter::next_record_start` to find chunk boundaries:

```
bytes: [record1][record2][record3][record4][record5]...
         ^       ^       ^       ^       ^
         0       |       |       |       |
                 pos1    pos2    pos3    pos4
```

The engine creates chunks from these positions. Each chunk is sent to
a thread for parsing.

### Parsing phase

For each chunk, the engine calls:

1. `RecordParser::validate(bytes)` - check UTF-8
2. `RecordParser::parse_chunk(bytes, sink)` - parse fields

Your `parse_chunk` implementation must call `sink.begin_row()`,
`sink.put_field()`, and `sink.end_row()` for each row and field.

### Export phase

After parsing, the engine calls `TableBuilder::finish()` which:

1. Sorts columns to match `schema_order` (if set)
2. Upgrades low-cardinality columns to dictionary encoding (if `auto_dict`)
3. Exports each chunk as a `RecordBatch`
4. Merges batches if needed (merge path) or exports in parallel (fast path)

### Memory management

In streaming mode, the engine processes one chunk at a time:

```
Chunk 1: [parse] -> [export] -> [free]
Chunk 2:         [parse] -> [export] -> [free]
Chunk 3:                 [parse] -> [export] -> [free]
```

Memory stays bounded regardless of file size. The `memory` parameter
controls how much memory each chunk can use.

## Further reading

- [Architecture](../architecture/): Internal design of the engine
- [Schema](./schema.md): The biggest performance lever
- [Techniques](./techniques.md): Performance optimizations
- [Anti-patterns](./anti-patterns.md): Common mistakes
- [Examples](./examples.md): Worked CSV, JSONL, and TSV adapters
