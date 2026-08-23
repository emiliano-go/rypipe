# rypipe-core

Format-agnostic columnar ingestion engine. Parses row-oriented byte streams
into Apache Arrow record batches with parallel scheduling, memory-bounded
execution, and query pushdown.

This crate is the pure Rust engine: no Python, no FFI, and no format-specific
logic. It ships no parsers. Formats such as XML, JSON, CSV, and HTML live in
separate adapter crates.

Part of the [rypipe](https://github.com/emiliano-go/rypipe) project. The
`rypipe` Python package wraps this engine through PyO3.

## What it does

The engine separates format-specific parsing (splitting, row extraction) from
format-agnostic execution (typed column builders, projection, filtering,
dictionary encoding, parallel scheduling, memory-bounded execution, and Arrow
export).

Adding a format means implementing two traits, `Splitter` and `RecordParser`.
An adapter answers two questions, where are the rows and what are the fields,
and the engine handles the rest.

## Usage

```toml
[dependencies]
rypipe-core = "0.1"
```

```rust
use rypipe_core::{ExecutionPlan, FieldType, Pipeline};
use my_adapter::{MySplitter, MyDecoder}; // separate adapter crate

let batch = Pipeline::new(MySplitter::new(), MyDecoder::new())
    .with_plan(
        ExecutionPlan::new()
            .type_as("amount", FieldType::Float64)
            .type_as("qty", FieldType::Int64)
            .filter_eq("status", "active"),
    )
    .read_path("data.myfmt", false, false)?;
```

## Features

- **Zero-copy friendly**: decoders emit borrowed strings; the engine copies
  only when necessary.
- **Parallel by default**: chunked parsing with `rayon` scales to many cores.
- **Memory bounded**: stream files larger than RAM with a configurable budget.
- **Typed columns**: cast strings to `int64`, `float64`, or `bool` during parse.
- **Pushdown**: rename, drop, type, and filter rows while parsing.
- **Dictionary encoding**: explicit or automatic low-cardinality encoding.
- **Arrow native**: produces `RecordBatch` and exports via the C Data Interface.

## Cargo features

| Feature | Effect |
|---------|--------|
| `mmap`  | Memory-mapped file input via `memmap2` |

## Public API

`Pipeline`, `ExecutionPlan`, `TableBuilder`, `Value`, `InputBuffer`,
`MemoryBudget`, `FieldType`, `CompareOp`, `FilterPredicate`,
`engines_to_record_batches`, and `apply_compare_filter`. The decoder API is
`Splitter`, `RecordParser`, and `ColumnarSink`.

## Arrow version pin

`arrow` is pinned exactly (`=55.2.0`). The Arrow C Data Interface is not stable
across minor versions, and this crate exports through it. Bump only after
testing both directions.

## Documentation

Full guides live at [rypipe.emiliano-go.com](https://rypipe.emiliano-go.com/),
including [architecture](https://rypipe.emiliano-go.com/architecture/),
the [Rust API](https://rypipe.emiliano-go.com/rust-api/), and
[writing a format adapter](https://rypipe.emiliano-go.com/writing-adapters/).

## License

MIT
