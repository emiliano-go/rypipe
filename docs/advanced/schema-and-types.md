# Schema and types { #schema-and-types }

This is the single largest performance lever for adapters with known schemas.
In the crxml reference adapter, declaring `schema=[...]` lifts throughput from
4.2 GB/s to 7.6 GB/s on production data (+80%), and the `row_satisfied`
byte-jump reaches 11 GB/s on benchmarks.

`rypipe` can infer column names and types, but inference passes cost time and
memory. Providing `schema_order` and `field_types` up front avoids those
passes, stabilizes column order, and enables numeric compare filters.

## Avoiding inference passes { #avoiding-inference-passes }

Some formats need a discovery pass to infer column names. For example, an XML adapter may scan the file to find all field names before parsing. This doubles I/O work and delays the first row.

Provide `schema` when the columns are known:

```python
source = MyAdapter(
    "data.log",
    schema=["id", "ts", "amount", "status"],
)
```

The Python `schema` kwarg maps to `ExecutionPlan::schema_order` on the Rust side. It is a **projection + order declaration**: the output contains exactly the listed columns, in the listed order. With a declared schema, the engine does not need to discover column names, and fields in the data that are not listed are skipped during parsing: `wants()`/`resolve()` reject them, so adapters never scan or decode them. Listed columns that are absent from the data come out null-filled; extra/unknown fields are ignored, not an error.

## Stable column order across chunks { #stable-column-order-across-chunks }

In parallel mode, each chunk may encounter columns in a different order. Without a shared `schema_order`, the engine must reconcile column order at merge time. This adds a small per-chunk cost and can produce unexpected ordering when chunks disagree.

`schema_order` fixes the output order regardless of the order in which fields arrive.

## Casting during parse { #casting-during-parse }

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

Supported type strings (as accepted by `FieldType::from_str`):

| Type string | Rust `FieldType` | Notes |
|-------------|------------------|-------|
| `string` | `FieldType::String` | Default for text data. |
| `int64` | `FieldType::Int64` | Parses integer strings during parse. |
| `float64` | `FieldType::Float64` | Parses float strings during parse. |
| `bool` / `boolean` | `FieldType::Boolean` | Parses common bool representations. |
| `dictionary` | `FieldType::Dictionary` | Dictionary encoding; equivalent to listing the column in `dictionary_columns`. |
| `date32` | `FieldType::Date32` | ISO dates (`YYYY-MM-DD`) stored as days since the Unix epoch. |
| `timestamp`, `timestamp[s]`, `timestamp[ms]`, `timestamp[us]`, `timestamp[ns]` | `FieldType::Timestamp(unit)` | ISO-8601 timestamps stored as integers in the given unit (default µs). |
| `timestamp[unit,format=…]` | `FieldType::Timestamp(unit, format)` | Custom chrono format tried before the ISO layouts, e.g. `timestamp[ms,format=%Y%m%d %H:%M]`. Unit may be omitted: `timestamp[format=%d/%m/%Y]`. |
| `decimal128`, `decimal128(N)` | `FieldType::Decimal128(scale)` | Fixed-precision decimal; default scale 18, or `N` when given. |

There are no `str`, `int`, or `float` aliases; an unknown type string raises
`PlanError` at construction time.

### Null and malformed values { #null-and-malformed-values }

Parse-time casting is lenient by design:

- A field that is missing from a row (or null in the data) comes out null,
  regardless of the declared type. `field_types` never rejects nullability.
- A value that does not parse as the declared type (say `"abc"` in an
  `int64` column) also comes out null. The row is kept, no error is raised.

This keeps dirty real-world files parseable, but it means a typo'd column
or a format change can silently turn a whole column into nulls. Pass
`strict_types=True` to make malformed values an error instead:

```python
source = MyAdapter(
    "data.log",
    field_types={"amount": "float64"},
    strict_types=True,   # "abc" in amount raises instead of becoming null
)
```

Under `strict_types`, the first value that fails to parse into its declared
type aborts the read with an error naming the column, the value, the
declared type, and the row index. Missing or null fields are still allowed;
strict mode is about malformed data, not nullability. The default is
`False`, and a fused `CastTypes` stays lenient either way.

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

## Numeric compare filters { #numeric-compare-filters }

Casting during parse is especially important for filters. Column-to-column
comparisons (`Compare`) and constant comparisons with ordering operators
(`CompareLiteral`, produced by `FilterRows(field=..., op=">", value=...)`)
are evaluated natively per-row during parsing with numeric promotion:
`Int64` versus `Float64` widens to `f64`. There is no Python-level
comparison and no post-assembly pass.

If the columns are left as strings, the comparison uses string ordering,
which is rarely what you want for numbers (`"9" > "100"` is true for
strings). Declare the types explicitly to keep numeric comparisons native.

## Combining schema hints with fusion { #combining-schema-hints-with-fusion }

`schema` and `field_types` are part of the `ExecutionPlan`. They merge cleanly with `RenameFields`, `DropFields`, and `FilterRows`:

```python
result = (
    MyAdapter("data.log", schema=["id", "amount"], field_types={"amount": "float64"})
    | RenameFields({"old_name": "amount"})
    | FilterRows(field="amount", op=">", value="100.0")
).to_arrow()
```

The filter runs on the renamed, typed column. Without `field_types`, the
same filter would compare string values instead of numbers.

## Summary { #summary }

- Provide `schema` to skip discovery and stabilize output column order.
- Provide `field_types` to cast during parse and keep compare filters numeric.
- Combine both with fused stages for the fastest path through the engine.
