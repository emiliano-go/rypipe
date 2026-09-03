# Schema and types

`rypipe` can infer column names and types, but inference passes cost time and memory. Providing `schema_order` and `field_types` up front avoids those passes, stabilizes column order, and enables numeric compare filters.

## Avoiding inference passes

Some formats need a discovery pass to infer column names. For example, an XML adapter may scan the file to find all field names before parsing. This doubles I/O work and delays the first row.

Provide `schema_order` when the columns are known:

```python
source = MyAdapter(
    "data.log",
    schema_order=["id", "ts", "amount", "status"],
)
```

With `schema_order`, the engine does not need to discover column names. It also sorts columns to this order at finish time, making output deterministic.

## Stable column order across chunks

In parallel mode, each chunk may encounter columns in a different order. Without a shared `schema_order`, the engine must reconcile column order at merge time. This adds a small per-chunk cost and can produce unexpected ordering when chunks disagree.

`schema_order` fixes the output order regardless of the order in which fields arrive.

## Casting during parse

`field_types` tells the engine which storage type to build for each column:

```python
source = MyAdapter(
    "data.log",
    field_types={
        "id": "int64",
        "ts": "string",
        "amount": "float64",
        "is_active": "bool",
    },
)
```

The engine builds the correct Arrow array from the first row. It does not store intermediate strings and recast later. This saves memory and CPU.

Supported types include:

| Type | Rust `FieldType` | Notes |
|------|------------------|-------|
| `string` / `str` | `FieldType::String` | Default for text data. |
| `int64` / `int` | `FieldType::Int64` | Parses integer strings during parse. |
| `float64` / `float` | `FieldType::Float64` | Parses float strings during parse. |
| `bool` / `boolean` | `FieldType::Boolean` | Parses common bool representations. |
| `dictionary` | `FieldType::Dictionary` | Dictionary encoding; equivalent to listing the column in `dictionary_columns`. |
| `date32` | `FieldType::Date32` | ISO dates (`YYYY-MM-DD`) stored as days since the Unix epoch. |
| `timestamp`, `timestamp[s]`, `timestamp[ms]`, `timestamp[us]`, `timestamp[ns]` | `FieldType::Timestamp(unit)` | ISO-8601 timestamps stored as integers in the given unit (default µs). |

`field_types={"status": "dictionary"}` and `dictionary_columns=["status"]` are
two spellings of the same storage decision; prefer `dictionary_columns` (or
`auto_dict`) so encoding choices stay separate from value types.

In Rust:

```rust
use rypipe_core::{ExecutionPlan, FieldType};

let plan = ExecutionPlan::new()
    .type_as("amount", FieldType::Float64)
    .type_as("quantity", FieldType::Int64);
```

## Numeric compare filters

Casting during parse is especially important for filters. When both sides of a
column-to-column comparison (`Compare`) are stored as `Int64` or `Float64`, the
engine compares them natively per-row during parsing with numeric promotion
(Int64 vs Float64 widens to f64), no Python-level comparisons and no
post-assembly pass.

If the columns are left as strings, the comparison falls back to string
ordering, which is rarely what you want for numbers. Declare the types
explicitly to keep numeric comparisons native.

## Combining schema hints with fusion

`schema_order` and `field_types` are part of the `ExecutionPlan`. They merge cleanly with `RenameFields`, `DropFields`, and `FilterRows`:

```python
result = (
    MyAdapter("data.log", schema_order=["id", "amount"], field_types={"amount": "float64"})
    | RenameFields({"old_name": "amount"})
    | FilterRows(field="amount", op=">", value="100.0")
).to_arrow()
```

The filter runs on the renamed, typed column. Without `field_types`, the filter would fall back to Python or be skipped.

## Summary

- Provide `schema_order` to skip inference and stabilize output columns.
- Provide `field_types` to cast during parse and enable numeric Arrow filters.
- Combine both with fused stages for the fastest path through the engine.
