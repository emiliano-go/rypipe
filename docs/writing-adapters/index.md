# Writing a Format Adapter

A `rypipe` adapter is two small types: a **Splitter** and a **RecordParser**.
Once you have those, the `Pipeline` API wires them into single-file, parallel,
and bounded-memory execution with one line of code.

Adapters are **separate packages** that depend on `rypipe-core`. They are not
part of `rypipe` itself. This keeps `rypipe` pure: it is only the
ingestion-to-Arrow engine.

## Architecture

```
Input bytes
  → Splitter::find_split_points    (find safe chunk boundaries)
  → RecordParser::parse_chunk      (per-chunk, feeds ColumnarSink)
  → ColumnarSink (TableBuilder)    (accumulates typed columns)
  → Arrow RecordBatch              (zero-copy export)
```

The engine handles:
- **Parallel execution** via rayon: split the file, parse chunks concurrently
- **Bounded-memory streaming**: constant RSS regardless of file size
- **Pushdown plans**: rename, drop, filter, type coercion, dictionary encoding
- **Zero-copy Arrow export**: move column buffers directly into Arrow arrays
- **Schema discovery**: find field names from a sample of the file
- **Predicate-first evaluation**: reject rows before scanning all fields

See [Architecture](../architecture/) for how these pieces interact internally.

## Hello World: complete runnable adapter

Here is a minimal, complete adapter for a newline-delimited `key=value` log
format. This is the smallest possible rypipe adapter:

### Rust: `src/lib.rs`

```rust
use std::borrow::Cow;
use rypipe_core::{Splitter, RecordParser, ColumnarSink, Value, Result};

// --- Splitter: find newline boundaries ---
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

// --- RecordParser: extract fields from each line ---
#[derive(Clone, Default)]
pub struct LogParser;

impl RecordParser for LogParser {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes).map_err(rypipe_core::Error::Utf8)?;
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

### Python registration: `my_adapter/__init__.py`

```python
import rypipe
import _rypipe_log

class LogAdapter:
    def read(self, path, **kwargs):
        return _rypipe_log.read_log(path, **kwargs)

rypipe.register_adapter("log", LogAdapter(), extensions=[".log"])
```

Note: the adapter class does **not** inherit from `rypipe.Adapter` or
`rypipe.Source`. It only needs a `read(path, **kwargs)` method that returns
a `pyarrow.Table`. The `Adapter`/`Source` base classes are for adapters
that want the pipeline `|` operator and fusion support.

### Usage

```python
import rypipe
import my_adapter  # registers the "log" adapter

# One-shot read (returns pyarrow.Table directly)
table = rypipe.read("app.log")

# For pipeline syntax, create a Source subclass instead (see below)
```

## Implementing `plan_overrides` for fusion

Adapters that subclass `rypipe.Source` receive fused plan kwargs through
`_read_arrow`. This is how fusion stays active through your adapter:

```python
from rypipe import Source

class MySource(Source):
    def _read_arrow(self, *, plan_overrides=None, **kwargs):
        # plan_overrides contains the fused stage kwargs:
        # {"field_mapping": {...}, "drop_fields": [...], "filter": {...}, ...}
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        # Pass the merged plan to your Rust reader:
        return my_rust_read(str(self._path), **plan)
```

If you ignore `plan_overrides`, fused stages silently fall back to Python
execution over a full table. Always forward them.

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
rypipe-core = "2.1"
arrow = { version = "=55.2.0", default-features = false, features = ["pyarrow"] }
pyo3 = { version = "0.24", features = ["extension-module", "abi3-py310"] }
memchr = "2"
simdutf8 = "0.1"
```

## The three traits

| Trait | Purpose | Required methods |
|-------|---------|-----------------|
| [`Splitter`](./splitter.md) | Find safe chunk boundaries | `next_record_start`, `estimate_bytes_per_row` |
| [`RecordParser`](./parser.md) | Turn bytes into field/value events | `validate`, `parse_chunk` |
| [`ColumnarSink`](./sink.md) | Accumulate values into Arrow columns | `begin_row`, `put_field`, `end_row`, `finish` |

The engine provides `TableBuilder` as the production `ColumnarSink`. You rarely
implement `ColumnarSink` yourself unless writing a profiling harness or a
custom streaming adapter.

## Performance model

The hot path is:

```
parse_chunk → begin_row → [put_field × N] → end_row → [repeat]
```

Each `put_field` call goes through:

1. **Scan**: find the field's byte extent in the input (your parser does this)
2. **Resolve**: map raw name to output column name (engine does this)
3. **Push**: write the value into the column builder (engine does this)
4. **Filter**: check if the row passes the predicate (engine does this)

The engine optimizes steps 2-4. Your parser's job is to make step 1 fast.

See [Sink](./sink.md) for the full method reference and fast paths.

## How to squeeze performance

1. **Use `scan::find` instead of raw `memchr`**: adds O(1) fast path
2. **Borrow strings with `Cow::Borrowed`**: avoids allocation for non-entity text
3. **Check `wants()` before expensive extraction**: skip dropped fields entirely
4. **Use `resolve` + `put_field_resolved`**: single hash probe for expensive extraction
5. **Emit typed `Value` variants**: `Int64`/`Float64` skip string-to-number conversion
6. **Implement `skip_regions`**: let the engine reject false-positive split points
7. **Implement `parse_chunk_generic`**: devirtualized sink calls for hot paths

See [Scan primitives](./scan.md) for byte-searching, [Skip regions](./skip-regions.md)
for comment/CDATA handling, and [Chunk planning](./chunk-planning.md) for the
2 MiB floor that prevents sub-MB chunk collapse.

## Pages

| Page | Lines | What it covers |
|------|-------|---------------|
| [Splitter](./splitter.md) | ~200 | Finding chunk boundaries, `next_record_start`, default `find_split_points` |
| [Parser](./parser.md) | ~200 | `RecordParser` trait, `parse_chunk`, `parse_chunk_generic`, performance tips |
| [Sink](./sink.md) | ~350 | `ColumnarSink`: all 21 methods, fast paths, projection, layout prediction |
| [Scan primitives](./scan.md) | ~150 | `find`, `find2`, `starts_with`, `find_literal` |
| [Skip regions](./skip-regions.md) | ~100 | `SkipRegionFinder` for comments, CDATA, quoted fields |
| [Chunk planning](./chunk-planning.md) | ~100 | `plan_chunk_count`, `MIN_CHUNK_BYTES`, thread caps |
| [Examples](./examples.md) | ~300 | Worked CSV, JSONL, and TSV adapters |
