# Rust API

`rypipe-core` is a pure-Rust crate. This guide shows how to use it directly and
how its pieces compose.

## Dependencies

```toml
[dependencies]
rypipe-core = { path = "../rypipe/crates/rypipe-core" }
rypipe-xml = { path = "../rypipe/crates/rypipe-xml" }
```

## A minimal XML example

```rust
use rypipe_core::{InputBuffer, TableBuilder, ExecutionPlan};
use rypipe_xml::{CrystalXmlDecoder, CrystalXmlSplitter};

fn main() -> rypipe_core::Result<()> {
    let input = InputBuffer::open("data.xml".as_ref(), false, false)?;
    let mut builder = TableBuilder::with_plan(1024, ExecutionPlan::new());

    let decoder = CrystalXmlDecoder::with_row_tag(b"Row");
    decoder.validate(input.as_slice())?;
    decoder.parse_chunk(input.as_slice(), &mut builder)?;

    let batch = builder.finish()?;
    println!("rows={} cols={}", batch.num_rows(), batch.num_columns());
    Ok(())
}
```

## `ExecutionPlan`

```rust
use rypipe_core::{ExecutionPlan, FieldType, FilterPredicate};

let mut plan = ExecutionPlan::new();
plan.field_map.insert("old_name".into(), "new_name".into());
plan.drop_fields.insert("junk".into());
plan.field_types.insert("amount".into(), FieldType::Float64);
plan.dictionary_columns.insert("status".into());
plan.filter = Some(FilterPredicate::Equal {
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

For XML everything is a string, but JSON or CSV adapters can emit native typed
values and skip string parsing.

## Parallel parse

```rust
use rypipe_core::{parallel::ParallelExecutor, ExecutionPlan};
use rypipe_xml::{CrystalXmlDecoder, CrystalXmlSplitter};

let bytes = std::fs::read("data.xml")?;
let splitter = CrystalXmlSplitter::with_row_tag(b"Row");
let decoder = CrystalXmlDecoder::with_row_tag(b"Row");
let plan = ExecutionPlan::new();

let batches = ParallelExecutor::parse(&bytes, &splitter, decoder, plan, 8)?;
```

`ParallelExecutor::parse` returns a `Vec<RecordBatch>`. The fast path emits one
batch per chunk; the merge path returns a single merged batch when `auto_dict`
or a `Compare` filter is enabled.

## Bounded parse

```rust
use rypipe_core::{
    bounded::{BoundedExecutor, MemoryBudget},
    ExecutionPlan,
};
use rypipe_xml::{CrystalXmlDecoder, CrystalXmlSplitter};
use std::path::Path;

let splitter = CrystalXmlSplitter::with_row_tag(b"Row");
let decoder = CrystalXmlDecoder::with_row_tag(b"Row");
let batches = BoundedExecutor::new(MemoryBudget::new(500_000_000))
    .run(Path::new("huge.xml"), &splitter, decoder, ExecutionPlan::new(), false)?;
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

`rypipe-python` provides:

```rust
use rypipe_python::export::record_batches_to_pyarrow_table;

// Inside a PyO3 function:
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
