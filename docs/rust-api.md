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
use my_csv_adapter::{CsvSplitter, CsvDecoder};

fn main() -> rypipe_core::Result<()> {
    let pipeline = Pipeline::new(CsvSplitter::new(), CsvDecoder::new())
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

    Ok(())
}
```

## A minimal example (low level)

If you prefer to control every step, use `TableBuilder` directly:

```rust
use rypipe_core::{InputBuffer, TableBuilder, ExecutionPlan};
use my_csv_adapter::{CsvSplitter, CsvDecoder}; // separate adapter crate

fn main() -> rypipe_core::Result<()> {
    let input = InputBuffer::open("data.csv".as_ref(), false, false)?;
    let mut builder = TableBuilder::with_plan(1024, ExecutionPlan::new());

    let decoder = CsvDecoder::new();
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
    .filter_eq("status", "active")
    .filter_compare("amount", CompareOp::Gt, "threshold")
    .schema_order(["id", "status", "amount"])
    .with_auto_dict(true);
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
use my_csv_adapter::{CsvSplitter, CsvDecoder}; // separate adapter crate

let bytes = std::fs::read("data.csv")?;
let splitter = CsvSplitter::new();
let decoder = CsvDecoder::new();
let plan = ExecutionPlan::new();

let batches = ParallelExecutor::parse(&bytes, &splitter, decoder, plan, 8)?;
```

`ParallelExecutor::parse` returns a `Vec<RecordBatch>`. The fast path emits one
batch per chunk; the merge path returns a single merged batch when `auto_dict`
or a `Compare` filter is enabled.

## Bounded parse (low level)

```rust
use rypipe_core::{
    bounded::{BoundedExecutor, MemoryBudget},
    ExecutionPlan,
};
use my_csv_adapter::{CsvSplitter, CsvDecoder}; // separate adapter crate
use std::path::Path;

let splitter = CsvSplitter::new();
let decoder = CsvDecoder::new();
let batches = BoundedExecutor::new(MemoryBudget::new(500_000_000))
    .run(Path::new("huge.csv"), &splitter, decoder, ExecutionPlan::new(), false)?;
```

## Apply a post-reduce Compare filter

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
use rypipe_python::{execution_plan_from_kwargs, record_batches_to_pyarrow_table};

// Inside a PyO3 function:
let plan = execution_plan_from_kwargs(...)?;
let table = record_batches_to_pyarrow_table(py, &batches)?;
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
- [Architecture](./architecture.md): how the pieces fit together.
- [Python API](./python-api.md): the Python bindings over the same engine.
