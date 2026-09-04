# Schema for Adapter Authors { #schema-for-adapter-authors }

When your format has a known set of columns, declaring them upfront with
`schema_order` and `field_types` is the single largest performance gain
available to adapter authors. In the crxml reference adapter, explicit schema
lifts throughput from 4.2 GB/s to 7.6 GB/s on production data (+80%).

## Why schema matters { #why-schema-matters }

Without schema declaration, the engine must discover column names (one full
I/O pass), reconcile column order across parallel chunks (merge-time sorting),
and store intermediate strings before casting (double memory, double CPU).

With schema declaration, column names are known before parsing, every chunk
produces identical column order, and Arrow arrays are built directly from
parsed values with no intermediate strings.

| Mode | 533 MB throughput | RSS | Notes |
|------|-------------------|-----|-------|
| Auto discovery | 4,497 MB/s | 88 MB | Discovery adds ~5.3 ms |
| Explicit schema | 4,980 MB/s | 88 MB | No discovery, fast export |
| Explicit schema + projection | 7,630 MB/s | 87 MB | `row_satisfied` byte-jump |

## The two schema knobs { #the-two-schema-knobs }

### `schema_order`: column names and output order { #schema_order }

`schema_order` tells the engine which columns exist and in what order they
appear in the output. When set, the engine skips column discovery entirely.

```rust
use rypipe_core::ExecutionPlan;
// Declare the exact columns and their output order
let plan = ExecutionPlan::new()
    .schema_order(["id", "timestamp", "amount", "status"]);
```

```python
# Python: pass schema as a list of column names
source = MyAdapter("data.log", schema=["id", "timestamp", "amount", "status"])
```

### `field_types`: typed arrays during parse { #field_types }

`field_types` tells the engine which Arrow storage type to use for each
column. Without it, all values are stored as strings and cast later.

```rust
use rypipe_core::{ExecutionPlan, FieldType};
// Declare types so values are parsed directly into Arrow arrays
let plan = ExecutionPlan::new()
    .schema_order(["id", "amount", "timestamp"])
    .type_as("id", FieldType::Int64)
    .type_as("amount", FieldType::Float64)
    .type_as("timestamp", FieldType::Timestamp(arrow::datatypes::TimeUnit::Microsecond));
```

```python
# Python: map column names to type strings for direct parsing
source = MyAdapter(
    "data.log",
    schema=["id", "amount", "timestamp"],
    field_types={"id": "int64", "amount": "float64", "timestamp": "timestamp[us]"},
)
```

### Supported types { #supported-types }

| Type string | Rust variant | Arrow type |
|-------------|-------------|------------|
| `string` | `FieldType::String` | `Utf8Array` |
| `int64` | `FieldType::Int64` | `Int64Array` |
| `float64` | `FieldType::Float64` | `Float64Array` |
| `bool` / `boolean` | `FieldType::Boolean` | `BooleanArray` |
| `dictionary` | `FieldType::Dictionary` | `DictionaryArray<Int32>` |
| `date32` | `FieldType::Date32` | `Date32Array` |
| `timestamp` | `FieldType::Timestamp(Microsecond)` | `Timestamp<Microsecond>` |
| `timestamp[s]` | `FieldType::Timestamp(Second)` | `Timestamp<Second>` |
| `timestamp[ms]` | `FieldType::Timestamp(Millisecond)` | `Timestamp<Millisecond>` |
| `timestamp[us]` | `FieldType::Timestamp(Microsecond)` | `Timestamp<Microsecond>` |
| `timestamp[ns]` | `FieldType::Timestamp(Nanosecond)` | `Timestamp<Nanosecond>` |

## How the parser uses schema { #how-the-parser-uses-schema }

### Checking `wants()` before scanning { #checking-wants }

`ColumnarSink::wants` returns `true` if the engine needs a particular field.
Check it before doing expensive extraction:

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    for row in self.split_rows(bytes) {
        sink.begin_row();
        for field in self.fields(row) {
            // Skip dropped fields entirely (no scanning, no decoding)
            if sink.wants(field.name) {
                sink.put_field(field.name, Value::Str(Cow::Borrowed(field.value)));
            }
        }
        sink.end_row();
    }
    Ok(())
}
```

When `schema_order` is set and a field is not in it, `wants` returns `false`,
meaning no byte scanning, no UTF-8 decoding, and no hash lookup. In crxml,
this saves ~66% of parse time on `drop_all` workloads.

### Using `resolve` + `put_field_resolved` { #using-resolve }

For fields that appear rarely or require expensive extraction, use the
`resolve` + `put_field_resolved` pair for a single hash probe instead of two:

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    for row in self.split_rows(bytes) {
        sink.begin_row();
        for field in self.fields(row) {
            // Single hash probe: resolve returns the column index
            if let Some(idx) = sink.resolve(field.name) {
                sink.put_field_resolved(idx, Value::Str(Cow::Borrowed(field.value)));
            }
        }
        sink.end_row();
    }
    Ok(())
}
```

### Emitting typed values { #emitting-typed-values }

When `field_types` is set, emit the correct `Value` variant to skip
string-to-number conversion:

```rust
// Instead of emitting a string (requires later conversion):
sink.put_field("amount", Value::Str(Cow::Borrowed("123.45")));
// Emit the typed value directly (no intermediate string allocation)
let value: f64 = field.value.parse().map_err(|e| Error::Plan(e.to_string()))?;
sink.put_field("amount", Value::Float64(value));
```

## Combining schema with fusion { #combining-schema-with-fusion }

`schema_order` and `field_types` compose cleanly with pipeline stages:

```python
result = (
    MyAdapter(
        "data.log",
        schema=["id", "amount", "status"],
        field_types={"id": "int64", "amount": "float64"},
    )
    | RenameFields({"id": "record_id"})
    | DropFields(["internal_debug"])
    | FilterRows(field="amount", op=">", value="100.0")
).to_arrow()
```

The execution order: schema defines columns (no discovery), `DropFields`
removes "internal_debug", `RenameFields` maps "id" to "record_id",
`FilterRows` adds a predicate. During parse, `wants("internal_debug")`
returns `false` (skipped), `put_field("id", ...)` maps to "record_id",
and `put_field("amount", ...)` builds `Float64Array` directly. The filter
checks `amount > 100.0` per row with native f64 comparison. Without
`field_types`, the filter falls back to string comparison or is skipped.

## Common patterns { #common-patterns }

```rust
// All columns needed, fully typed
let plan = ExecutionPlan::new()
    .schema_order(["timestamp", "user_id", "action", "value"])
    .type_as("timestamp", FieldType::Timestamp(TimeUnit::Microsecond))
    .type_as("user_id", FieldType::Int64)
    .type_as("value", FieldType::Float64);
```

```rust
// Some columns dropped, one renamed
let plan = ExecutionPlan::new()
    .schema_order(["id", "name", "amount", "status"])
    .type_as("id", FieldType::Int64)
    .type_as("amount", FieldType::Float64)
    .drop("internal_debug")
    .rename("uid", "id")
    .filter_eq("status", "active");
```

```rust
// Partial schema: declare known columns, unknowns are appended automatically
let plan = ExecutionPlan::new()
    .schema_order(["id", "name", "amount"])
    .type_as("id", FieldType::Int64)
    .type_as("amount", FieldType::Float64);
```

## Implementing schema in your adapter { #implementing-schema }

### Step 1: Accept schema kwargs { #step-1-accept-kwargs }

In your Python adapter, accept `schema` and `field_types` kwargs and pass
them to the Rust reader:

```python
class MyAdapter(rypipe.Adapter):
    def read(self, path, *, schema=None, field_types=None, **kwargs):
        # Forward schema kwargs to the Rust core
        plan_kwargs = {}
        if schema:
            plan_kwargs["schema"] = schema
        if field_types:
            plan_kwargs["field_types"] = field_types
        return _my_rust_core.read_file(path, **plan_kwargs, **kwargs)
```

### Step 2: Build the plan in Rust { #step-2-build-plan }

In your Rust parser, build the `ExecutionPlan` from the kwargs:

```rust
pub fn read_file(path: &str, schema: Option<Vec<String>>, field_types: Option<HashMap<String, String>>) -> Result<PyArrowTable> {
    let mut plan = ExecutionPlan::new();
    // Apply schema_order if provided
    if let Some(names) = schema { plan.schema_order = names; }
    // Apply field_types if provided
    if let Some(types) = field_types {
        for (name, type_str) in &types {
            if let Some(ft) = FieldType::from_str(type_str) {
                plan.field_types.insert(name.clone(), ft);
            }
        }
    }
    let pipeline = Pipeline::new(MySplitter::new(), MyParser::new())
        .with_plan(plan);
    pipeline.read_path(path, false, false)
}
```

### Step 3: Use `wants()` and emit typed values { #step-3-use-wants }

Check `sink.wants()` before scanning, and emit typed values when possible:

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    for row in self.split_rows(bytes) {
        sink.begin_row();
        for field in self.fields(row) {
            // Skip fields the user doesn't want
            if sink.wants(field.name) {
                let value = self.parse_value(field);
                sink.put_field(field.name, value);
            }
        }
        sink.end_row();
    }
    Ok(())
}

fn parse_value(&self, field: &Field) -> Value {
    match self.plan.column_type(field.name) {
        // Parse directly into Int64 (no string intermediate)
        FieldType::Int64 => Value::Int64(field.value.parse().unwrap_or(0)),
        // Parse directly into Float64 (no string intermediate)
        FieldType::Float64 => Value::Float64(field.value.parse().unwrap_or(0.0)),
        // Parse boolean from common truthy values
        FieldType::Boolean => Value::Bool(matches!(field.value, "true" | "1" | "yes")),
        // Fall back to string for unknown types
        _ => Value::Str(Cow::Borrowed(field.value)),
    }
}
```

## Performance characteristics { #performance-characteristics }

**Memory**: Without `field_types`, each string value is heap-allocated and a
casting pass converts strings to typed arrays afterward (peak 2x). With
`field_types`, values are parsed directly into typed builders (peak 1x). For a
533 MB file with 10 columns, this saves ~200-400 MB.

**CPU**: Without `field_types`, every value is stored as a string then
post-parse casting runs `str::parse::<i64>()` for each value (two passes).
With `field_types`, values are parsed once directly into the correct type (one
pass). Typical savings: 10-20% of total parse time for numeric-heavy workloads.

**Export speed**: Without `schema_order`, each batch may have different column
order, forcing sequential merge (~4,497 MB/s). With `schema_order`, identical
column order enables parallel export (~4,980 MB/s, +11%). With `schema_order`
+ projection, the scanner skips unwanted fields via `row_satisfied` byte-jump
(~7,630 MB/s, +80%).

## Troubleshooting { #troubleshooting }

**"unknown field not in frozen schema"** -- a field appeared that was not in
`schema_order`. Add it, omit `schema_order` for full-scan discovery, or rely
on partial schema (unknown columns are automatically appended in parallel
streaming).

**Column order does not match `schema_order`** -- check that you are calling
`sort_columns` or using the fast export path. The merge path preserves
discovery order.

**Filter not working on numeric columns** -- ensure `field_types` is set for
the filtered columns. Without it, values are strings and comparison is
lexicographic.

**Memory higher than expected** -- ensure `field_types` is set for all numeric
columns. Without it, intermediate strings double memory for those columns.
