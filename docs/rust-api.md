# Rust API

`rypipe-core` is a pure-Rust crate. This guide shows how to use it directly and
how its pieces compose. Format-specific adapters (for XML, CSV, JSON, etc.) are
separate crates that implement `Splitter` and `RecordParser`.

## Dependencies

```toml
[dependencies]
rypipe-core = { path = "../rypipe/crates/rypipe-core" }
# adapter crate of your choice, e.g.:
# my-csv-adapter = "0.1"
```

## Recommended entry point: `Pipeline`

`Pipeline` wires a `Splitter` and `RecordParser` together and removes the
boilerplate of opening files and choosing an execution mode.

```rust
use rypipe_core::{ExecutionPlan, FieldType, Pipeline};
// Import the adapter for your format from its own crate:
use my_csv_adapter::{CsvDecoder, CsvSplitter};

fn main() -> rypipe_core::Result<()> {
    let pipeline = Pipeline::new(CsvSplitter, CsvDecoder {
        header: vec!["id".into(), "status".into(), "amount".into()],
    })
        .with_plan(
            ExecutionPlan::new()
                .rename("old_name", "new_name")
                .drop("junk")
                .type_as("amount", FieldType::Float64)
                .dictionary("status")
                .filter_eq("status", "active")
                .schema_order(["id", "status", "amount"])
                .with_auto_dict(true),
        );

    // Single-file parse.
    let batch = pipeline.read_path("data.csv", false, false)?;
    println!("rows={} cols={}", batch.num_rows(), batch.num_columns());

    // Parallel parse.
    let batches = pipeline.read_path_par("data.csv", 8, false, false)?;

    // Bounded-memory streaming.
    let batches = pipeline.read_path_stream(
        "huge.csv",
        rypipe_core::MemoryBudget::new(500_000_000),
        false,
    )?;

    // In-memory byte slices (useful for BytesIO, stdin buffers, or decompressed
    // content) without file I/O.
    let bytes = std::fs::read("data.csv")?;
    let batch = pipeline.read_bytes(&bytes)?;
    let batches = pipeline.read_bytes_par(&bytes, 8)?;
    let batches =
        pipeline.read_bytes_stream(&bytes, rypipe_core::MemoryBudget::new(500_000_000))?;

    Ok(())
}
```

## A minimal example (low level)

If you prefer to control every step, use `TableBuilder` directly:

```rust
use std::sync::Arc;
use rypipe_core::{InputBuffer, TableBuilder, ExecutionPlan};
use my_csv_adapter::{CsvDecoder, CsvSplitter}; // separate adapter crate
use std::path::Path;

fn main() -> rypipe_core::Result<()> {
    let input = InputBuffer::open(Path::new("data.csv"), false, false)?;
    let mut builder = TableBuilder::with_plan(1024, Arc::new(ExecutionPlan::new()));

    let decoder = CsvDecoder {
        header: vec!["id".into(), "status".into(), "amount".into()],
    };
    decoder.validate(input.as_slice())?;
    decoder.parse_chunk(input.as_slice(), &mut builder)?;

    let batch = builder.finish()?;
    println!("rows={} cols={}", batch.num_rows(), batch.num_columns());
    Ok(())
}
```

## `ExecutionPlan`

The builder API is the recommended way to construct a plan:

```rust
use rypipe_core::{CompareOp, ExecutionPlan, FieldType};

let plan = ExecutionPlan::new()
    .rename("old_name", "new_name")
    .drop("junk")
    .type_as("amount", FieldType::Float64)
    .dictionary("status")
    // leaf filters; combine with And/Or/Not trees (see below)
    .filter_eq("status", "active");

// Filter trees from ExecutionPlan helpers or directly:

let any = FilterPredicate::any(
    FilterPredicate::Equal { field: "a".into(), value: "1".into() },
    FilterPredicate::not(FilterPredicate::Equal { field: "b".into(), value: "2".into() }),
);
let all = FilterPredicate::all(
    FilterPredicate::Compare { field_a: "amount".into(), op: CompareOp::Gt, field_b: "threshold".into() },
    FilterPredicate::Equal { field: "status".into(), value: "active".into() },
);

let plan_with_tree = ExecutionPlan {
    filter: Some(FilterPredicate::any(any, all)),
    field_types: plan.field_types.clone(),
    ..plan.clone()
};
```

You can still mutate the fields directly when you need to:

```rust
let mut plan = ExecutionPlan::new();
plan.field_map.insert("old_name".into(), "new_name".into());
plan.drop_fields.insert("junk".into());
plan.field_types.insert("amount".into(), FieldType::Float64);
plan.dictionary_columns.insert("status".into());
plan.filter = Some(rypipe_core::FilterPredicate::Equal {
    field: "status".into(),
    value: "active".into(),
});
plan.schema_order = vec!["id".into(), "status".into(), "amount".into()];
plan.auto_dict = true;
```

## `Value`

Decoders emit `Value<'a>`:

```rust
use rypipe_core::Value;

sink.put_field("amount", Value::Float64(123.45));
sink.put_field("name", Value::Str("Alice"));
sink.put_field("flag", Value::Bool(true));
sink.put_field("missing", Value::Null);
```

For stringly formats everything is a string, but JSON or CSV adapters can emit
native typed values and skip string parsing.

## Parallel parse (low level)

```rust
use rypipe_core::{parallel::ParallelExecutor, ExecutionPlan};
use my_csv_adapter::{CsvDecoder, CsvSplitter}; // separate adapter crate

let bytes = std::fs::read("data.csv")?;
let splitter = CsvSplitter;
let decoder = CsvDecoder {
    header: vec!["id".into(), "status".into(), "amount".into()],
};
let plan = ExecutionPlan::new();

let batches = ParallelExecutor::parse(&bytes, &splitter, decoder, plan, 8)?;
```

`ParallelExecutor::parse` returns a `Vec<RecordBatch>`. The fast path emits one
batch per chunk with a unified schema; the merge path returns a single merged
batch when `auto_dict` is enabled or chunks disagree irreconcilably on column
types.

## Bounded parse (low level)

```rust
use rypipe_core::{
    bounded::{BoundedExecutor, MemoryBudget},
    ExecutionPlan,
};
use my_csv_adapter::{CsvDecoder, CsvSplitter}; // separate adapter crate
use std::path::Path;

let splitter = CsvSplitter;
let decoder = CsvDecoder {
    header: vec!["id".into(), "status".into(), "amount".into()],
};
let batches = BoundedExecutor::new(MemoryBudget::new(500_000_000))
    .run(Path::new("huge.csv"), &splitter, decoder, ExecutionPlan::new(), false)?;

// From an in-memory buffer (transparent decompression in InputBuffer::open
// also lands here when the file was compressed; see Cargo features below).
let bytes = std::fs::read("huge.csv")?;
let batches = BoundedExecutor::new(MemoryBudget::new(500_000_000))
    .run_bytes(&bytes, &splitter, decoder, ExecutionPlan::new())?;
```

## Transparent compressed-file decoding

File inputs are auto-detected by magic bytes and transparently decompressed
when the corresponding Cargo feature is enabled:

```toml
[dependencies]
rypipe-core = { version = "0.1", features = ["gzip"] } # or "zstd", "lz4", "compress-all"
```

Supported magics: gzip `1f 8b`, zstd `28 b5 2f fd`, lz4 frame `04 22 4d 18`.
Detection happens in `InputBuffer::open`; decompressed bytes are served from
memory (`InputBuffer::Owned`) across all execution modes.

## Apply a Compare filter to an existing batch

Pure column-comparison filters (``Compare`` and ``And`` of ``Compare``) are
normally evaluated per-row during parsing but can also be applied to an
already-built ``RecordBatch``. Trees involving ``Or``, ``Not``, ``Equal``, or
``NotEqual`` are no-ops at this layer (per-row evaluation is already
authoritative) so valid rows can never be dropped twice. To filter an
already-built ``RecordBatch`` use
``apply_compare_filter``:

```rust
use rypipe_core::{apply_compare_filter, FilterPredicate, CompareOp};

let predicate = FilterPredicate::Compare {
    field_a: "amount".into(),
    op: CompareOp::Gt,
    field_b: "threshold".into(),
};
let filtered = apply_compare_filter(batch, &predicate)?;
```

## Export helpers

`rypipe-python` provides Rust helper functions for adapter crates:

```rust
use rypipe_python::{
    execution_plan_from_kwargs, record_batch_to_pyarrow,
    record_batches_to_pyarrow_batches, record_batches_to_pyarrow_table,
};

// Inside a PyO3 function:
let plan = execution_plan_from_kwargs(...)?;
let table = record_batches_to_pyarrow_table(py, &batches)?;
// Or export batches individually for streaming-style APIs:
let batch_list = record_batches_to_pyarrow_batches(py, &batches)?;
```

For a pure-Rust program you do not need this; `arrow::record_batch::RecordBatch`
is already sufficient.

## Writing a custom sink

You can implement `ColumnarSink` yourself for specialized behavior. Most users
should use `TableBuilder`.

```rust
use rypipe_core::{ColumnarSink, Value, Result};
use arrow::record_batch::RecordBatch;

struct RowCounter { rows: usize }

impl ColumnarSink for RowCounter {
    fn begin_row(&mut self) {}
    fn put_field(&mut self, _name: &str, _value: Value<'_>) {}
    fn end_row(&mut self) { self.rows += 1; }
    fn finish(&mut self) -> Result<RecordBatch> {
        Ok(RecordBatch::new_empty(std::sync::Arc::new(
            arrow::datatypes::Schema::empty(),
        )))
    }
}
```

## See also

- [Writing a format adapter](./writing-adapters.md): implement `Splitter` and `RecordParser` in a separate package.
- [Architecture](./architecture/): how the pieces fit together.
- [Python API](./python-api.md): the Python bindings over the same engine.
