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
rypipe-core = "2.0"

# Only if you build Python bindings:
rypipe-python = "2.0"
pyo3 = { version = "0.29", features = ["extension-module", "abi3-py310"] }
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
2. **Resolve**: map raw name → output column name (engine does this)
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
| [Sink](./sink.md) | ~350 | `ColumnarSink` — all 21 methods, fast paths, projection, layout prediction |
| [Scan primitives](./scan.md) | ~150 | `find`, `find2`, `starts_with`, `find_literal` |
| [Skip regions](./skip-regions.md) | ~100 | `SkipRegionFinder` for comments, CDATA, quoted fields |
| [Chunk planning](./chunk-planning.md) | ~100 | `plan_chunk_count`, `MIN_CHUNK_BYTES`, thread caps |
| [Examples](./examples.md) | ~300 | Worked CSV, JSONL, and TSV adapters |
