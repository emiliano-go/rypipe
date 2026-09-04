# Schema for Adapter Authors

When your format has a known set of columns, declaring them upfront with
`schema_order` and `field_types` is the single largest performance gain
available to adapter authors. In the crxml reference adapter, explicit schema
lifts throughput from 4.2 GB/s to 7.6 GB/s on production data (+80%).

This page explains how schema declaration works inside the Rust parser, how
it interacts with the engine, and how to implement it correctly in your
adapter.

## Why schema matters

Without schema declaration, the engine must:

1. **Discover column names** by scanning the file (one full I/O pass)
2. **Reconcile column order** across parallel chunks (merge-time sorting)
3. **Store intermediate strings** and cast later (double memory, double CPU)

With schema declaration, all three costs disappear:

1. Column names are known before parsing starts
2. Every chunk produces identical column order (fast export path)
3. Arrow arrays are built directly from parsed values (no intermediate strings)

The performance difference is dramatic:

| Mode | 533 MB throughput | RSS | Notes |
|------|-------------------|-----|-------|
| Auto discovery | 4,497 MB/s | 88 MB | Discovery adds ~5.3 ms |
| Explicit schema | 4,980 MB/s | 88 MB | No discovery, fast export |
| Explicit schema + projection | 7,630 MB/s | 87 MB | `row_satisfied` byte-jump |

## The two schema knobs

### `schema_order`: column names and output order

`schema_order` tells the engine which columns exist and in what order they
appear in the output. When set, the engine skips column discovery entirely.

```rust
use rypipe_core::ExecutionPlan;

let plan = ExecutionPlan::new()
    .schema_order(["id", "timestamp", "amount", "status"]);
```

In Python:

```python
source = MyAdapter("data.log", schema=["id", "timestamp", "amount", "status"])
```

How it works inside the engine:

1. `FrozenSchema::from_plan` builds an immutable schema from the names and
   the plan's type/drop/rename rules
2. Each worker's `TableBuilder` calls `ensure_schema` to pre-size all
   columns before parsing starts
3. At export time, `sort_columns` reorders columns to match `schema_order`
4. Every batch has identical column order, enabling the fast export path

### `field_types`: typed arrays during parse

`field_types` tells the engine which Arrow storage type to use for each
column. Without it, all values are stored as strings and cast later.

```rust
use rypipe_core::{ExecutionPlan, FieldType};

let plan = ExecutionPlan::new()
    .schema_order(["id", "amount", "timestamp"])
    .type_as("id", FieldType::Int64)
    .type_as("amount", FieldType::Float64)
    .type_as("timestamp", FieldType::Timestamp(arrow::datatypes::TimeUnit::Microsecond));
```

In Python:

```python
source = MyAdapter(
    "data.log",
    schema=["id", "amount", "timestamp"],
    field_types={"id": "int64", "amount": "float64", "timestamp": "timestamp[us]"},
)
```

Supported types:

| Type string | Rust variant | Arrow type |
|-------------|-------------|------------|
| `string` | `FieldType::String` | `Utf8Array` |
| `int64` / `int` | `FieldType::Int64` | `Int64Array` |
| `float64` / `float` | `FieldType::Float64` | `Float64Array` |
| `bool` / `boolean` | `FieldType::Boolean` | `BooleanArray` |
| `dictionary` | `FieldType::Dictionary` | `DictionaryArray<Int32>` |
| `date32` | `FieldType::Date32` | `Date32Array` |
| `timestamp` | `FieldType::Timestamp(Microsecond)` | `Timestamp<Microsecond>` |
| `timestamp[s]` | `FieldType::Timestamp(Second)` | `Timestamp<Second>` |
| `timestamp[ms]` | `FieldType::Timestamp(Millisecond)` | `Timestamp<Millisecond>` |
| `timestamp[us]` | `FieldType::Timestamp(Microsecond)` | `Timestamp<Microsecond>` |
| `timestamp[ns]` | `FieldType::Timestamp(Nanosecond)` | `Timestamp<Nanosecond>` |

How type casting works:

1. `plan.column_type(name)` returns the `FieldType` for a column
2. `TableBuilder::ensure_schema` creates typed column builders for each type
3. When `put_field` is called, the engine writes directly into the typed
   builder (e.g., `Int64Builder` for `FieldType::Int64`)
4. No intermediate string is stored; the value is parsed from bytes directly
   into the Arrow array

## How the parser uses schema

### Checking `wants()` before scanning

The `ColumnarSink::wants` method returns `true` if the engine needs a
particular field. Check this before doing expensive extraction:

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

When `schema_order` is set and a field is not in it, `wants` returns `false`.
This means:

- No byte scanning for the field's value
- No UTF-8 decoding
- No hash lookup in the engine
- The scanner can byte-jump past the entire field (via `find_close_after`)

In crxml, this saves ~66% of parse time on `drop_all` workloads (4,183 MB/s
vs 2,706 MB/s baseline).

### Using `resolve` + `put_field_resolved`

For fields that appear rarely or require expensive extraction, use the
`resolve` + `put_field_resolved` pair. This does a single hash probe instead
of two:

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    for row in self.split_rows(bytes) {
        sink.begin_row();
        for field in self.fields(row) {
            if let Some(idx) = sink.resolve(field.name) {
                // Single hash probe: resolve returns the column index
                sink.put_field_resolved(idx, Value::Str(Cow::Borrowed(field.value)));
            }
        }
        sink.end_row();
    }
    Ok(())
}
```

### Emitting typed values

When `field_types` is set, emit the correct `Value` variant. The engine skips
string-to-number conversion:

```rust
// Instead of:
sink.put_field("amount", Value::Str(Cow::Borrowed("123.45")));

// Emit directly:
let value: f64 = field.value.parse().map_err(|e| Error::Plan(e.to_string()))?;
sink.put_field("amount", Value::Float64(value));
```

This saves both memory (no intermediate string) and CPU (no post-parse
conversion).

## FrozenSchema internals

`FrozenSchema` is the engine's internal representation of the output column
layout. It is built once and shared (via `Arc`) across all workers.

### Construction paths

**Explicit schema** (`FrozenSchema::from_plan`):

```rust
pub fn from_plan(names: &[&str], plan: &ExecutionPlan) -> Self {
    // Builds index: raw_name -> Option<slot>
    // Applies field_types from plan
    // exact = true
}
```

When `schema_order` is set, the engine calls `from_plan` with the declared
names. This is the fast path: no discovery, no sampling, no cache lookup.

**Discovered schema** (`FrozenSchema::from_discovered`):

```rust
pub fn from_discovered(names_in_order: &[String], plan: &ExecutionPlan) -> Self {
    // Applies renames from plan.field_map
    // Applies drops from plan.drop_fields
    // exact = false (sampled)
}
```

When `schema_order` is empty, the engine discovers column names from the file.
For files >128 MiB, it samples 16x2 MiB windows in parallel. The discovered
names are cached by layout signature.

### The index: one lookup for everything

The `index` field in `FrozenSchema` maps raw input field names to output slot
indices:

```rust
index: FxHashMap<Box<str>, Option<u32>>
```

- `Some(slot)` = field maps to output column `slot`
- `None` = field is dropped by the plan

This collapses three operations into one lookup:

1. **Rename**: if the plan renames "old" to "new", the index maps "old" to
   the slot for "new"
2. **Drop**: if the plan drops "secret", the index maps "secret" to `None`
3. **Column lookup**: the slot index is the position in the output batch

The engine calls `schema.resolve(raw_name)` in the hot path. With `FxHashMap`,
this is a single hash probe (~15 cycles per field).

### Schema cache

For batch workloads (many files with the same layout), the engine caches
discovered schemas by layout signature:

```rust
pub static SCHEMA_CACHE: LazyLock<RwLock<FxHashMap<SchemaCacheKey, SchemaCacheValue>>>
```

- Key: `(file_len, sample_hash)` computed by `layout_signature`
- Value: `Arc<Vec<String>>` of discovered column names
- Cap: 128 entries (arbitrary entry evicted on overflow)

Cache hits skip the discovery pass entirely. For 1,000 files with the same
layout, this saves 5 ms per file (5 seconds total).

Use `crxml.discover_schema("sample.xml")` to pre-compute the schema and pass
it as `schema=` to every file. This is the recommended pattern for batch
workloads.

## Ensuring schema across chunks

### `ensure_schema`: pre-sizing columns

When a worker starts parsing a chunk, it calls `ensure_schema` to guarantee
that all declared columns exist in the builder:

```rust
pub fn ensure_schema(&mut self, schema: &FrozenSchema) -> Result<()> {
    for (idx, name) in schema.column_names().iter().enumerate() {
        if !self.column_names().contains(&name.to_string()) {
            // Add column with correct type, pre-sized to current row count
            self.add_typed_column(name, schema.column_types()[idx], self.num_rows());
        }
    }
    Ok(())
}
```

This ensures:

- Every batch has the same columns (even if a chunk does not contain a field)
- Sparse columns are all-null (not missing)
- Column order matches `schema_order`

### Sort columns at finish time

When `schema_order` is set, `TableBuilder::finish` sorts columns to match:

```rust
pub fn finish(mut self) -> Result<Vec<RecordBatch>> {
    if !self.plan.schema_order.is_empty() {
        self.sort_columns();
    }
    // ... export batches
}
```

`sort_columns` reorders the internal column list to match `schema_order`.
Columns not in `schema_order` appear after, in first-appearance order.

### Fast export path

When all batches have identical schemas (guaranteed by `ensure_schema` +
`sort_columns`), the engine can export each batch independently in parallel.
This is the "fast path" that enables 4,980 MB/s streaming.

Without explicit schema, the engine must merge batches sequentially to
reconcile column order. This is the "merge path" and is ~10% slower.

## Combining schema with fusion

`schema_order` and `field_types` are part of the `ExecutionPlan`. They compose
cleanly with pipeline stages:

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

The execution order:

1. Schema defines columns and types (no discovery)
2. `DropFields` removes "internal_debug" from the plan
3. `RenameFields` maps "id" to "record_id" in the plan
4. `FilterRows` adds a predicate to the plan
5. During parse: `wants("internal_debug")` returns `false` (skipped)
6. During parse: `put_field("id", ...)` maps to "record_id" column
7. During parse: `put_field("amount", ...)` builds `Float64Array` directly
8. During parse: filter checks `amount > 100.0` per row (native f64 compare)
9. At finish: columns sorted to ["record_id", "amount", "status"]

Without `field_types`, the filter would fall back to string comparison (wrong
for numbers) or be skipped entirely.

## Numeric compare filters

When both sides of a column-to-column comparison are typed, the engine
compares them natively during parsing:

```python
# This works correctly only with field_types:
result = (
    MyAdapter("data.log", schema=["price", "cost"], field_types={"price": "float64", "cost": "float64"})
    | FilterRows(field="price", op=">", field2="cost")
).to_arrow()
```

Without `field_types`, both columns are strings, and the comparison falls
back to lexicographic ordering (`"9" > "10"` is `true`, which is wrong).

With `field_types`, the engine emits a native `Float64` comparison with
numeric promotion (Int64 vs Float64 widens to f64).

## Common patterns

### Pattern 1: Known schema, all columns needed

```rust
let plan = ExecutionPlan::new()
    .schema_order(["timestamp", "user_id", "action", "value"])
    .type_as("timestamp", FieldType::Timestamp(TimeUnit::Microsecond))
    .type_as("user_id", FieldType::Int64)
    .type_as("value", FieldType::Float64);
```

### Pattern 2: Known schema, some columns dropped

```rust
let plan = ExecutionPlan::new()
    .schema_order(["id", "name", "amount"])
    .type_as("id", FieldType::Int64)
    .type_as("amount", FieldType::Float64)
    .drop("internal_debug")
    .drop("row_checksum");
```

### Pattern 3: Known schema, rename + filter

```rust
let plan = ExecutionPlan::new()
    .schema_order(["user_id", "amount", "status"])
    .type_as("user_id", FieldType::Int64)
    .type_as("amount", FieldType::Float64)
    .rename("uid", "user_id")
    .filter_eq("status", "active");
```

### Pattern 4: Partial schema (some columns unknown)

If you know most columns but not all, set `schema_order` for the known ones.
The engine discovers unknown columns at parse time and appends them after the
declared columns:

```rust
let plan = ExecutionPlan::new()
    .schema_order(["id", "name", "amount"])  // known columns first
    .type_as("id", FieldType::Int64)
    .type_as("amount", FieldType::Float64);
// Any other columns in the file are appended in discovery order
```

### Pattern 5: Dictionary encoding

```rust
let plan = ExecutionPlan::new()
    .schema_order(["id", "status", "amount"])
    .type_as("id", FieldType::Int64)
    .type_as("amount", FieldType::Float64)
    .dictionary("status");  // or field_types={"status": "dictionary"}
```

### Pattern 6: Auto-dictionary with threshold

```rust
let plan = ExecutionPlan::new()
    .schema_order(["id", "category", "value"])
    .type_as("id", FieldType::Int64)
    .type_as("value", FieldType::Float64)
    .auto_dict(true)
    .dict_threshold(0.03)  // upgrade if <3% distinct
    .dict_max_size(512);   // max 512 entries
```

## Implementing schema in your adapter

### Step 1: Accept schema kwargs

In your Python adapter, accept `schema` and `field_types` kwargs and pass
them to the Rust reader:

```python
class MyAdapter(rypipe.Adapter):
    def read(self, path, *, schema=None, field_types=None, **kwargs):
        plan_kwargs = {}
        if schema:
            plan_kwargs["schema"] = schema
        if field_types:
            plan_kwargs["field_types"] = field_types
        return _my_rust_core.read_file(path, **plan_kwargs, **kwargs)
```

### Step 2: Build the plan in Rust

In your Rust parser, build the `ExecutionPlan` from the kwargs:

```rust
pub fn read_file(path: &str, schema: Option<Vec<String>>, field_types: Option<HashMap<String, String>>) -> Result<PyArrowTable> {
    let mut plan = ExecutionPlan::new();

    if let Some(names) = schema {
        plan.schema_order = names;
    }

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

### Step 3: Use `wants()` in your parser

Check `sink.wants()` before scanning field bytes:

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    for row in self.split_rows(bytes) {
        sink.begin_row();
        for field in self.fields(row) {
            if sink.wants(field.name) {
                // Parse and emit the value
                let value = self.parse_value(field);
                sink.put_field(field.name, value);
            }
            // else: skip entire field (no scanning)
        }
        sink.end_row();
    }
    Ok(())
}
```

### Step 4: Emit typed values when possible

If `field_types` is set, parse values directly into the correct type:

```rust
fn parse_value(&self, field: &Field) -> Value {
    match self.plan.column_type(field.name) {
        FieldType::Int64 => {
            let v: i64 = field.value.parse().unwrap_or(0);
            Value::Int64(v)
        }
        FieldType::Float64 => {
            let v: f64 = field.value.parse().unwrap_or(0.0);
            Value::Float64(v)
        }
        FieldType::Boolean => {
            let v = matches!(field.value, "true" | "1" | "yes");
            Value::Boolean(v)
        }
        _ => Value::Str(Cow::Borrowed(field.value)),
    }
}
```

## Testing schema behavior

### Unit test: schema ordering

```rust
#[test]
fn test_schema_ordering() {
    let plan = ExecutionPlan::new()
        .schema_order(["C", "A", "B"]);

    let schema = FrozenSchema::from_plan(&["C", "A", "B"], &plan);
    assert_eq!(schema.column_names(), &["C", "A", "B"]);
    assert_eq!(schema.resolve("A"), Some(1)); // slot 1, not 0
    assert_eq!(schema.resolve("C"), Some(0)); // slot 0, first
}
```

### Unit test: field_types casting

```rust
#[test]
fn test_field_types() {
    let plan = ExecutionPlan::new()
        .schema_order(["id", "amount"])
        .type_as("id", FieldType::Int64)
        .type_as("amount", FieldType::Float64);

    let schema = FrozenSchema::from_plan(&["id", "amount"], &plan);
    assert_eq!(schema.column_types(), &[FieldType::Int64, FieldType::Float64]);
}
```

### Integration test: schema with drops

```rust
#[test]
fn test_schema_with_drops() {
    let mut plan = ExecutionPlan::new()
        .schema_order(["A", "B", "C"])
        .drop("B");

    let schema = FrozenSchema::from_plan(&["A", "B", "C"], &plan);
    // B is in the schema but marked as dropped in the plan
    // At parse time, wants("B") returns false
    assert_eq!(schema.num_columns(), 3);
}
```

### Integration test: ensure_schema pre-sizes

```rust
#[test]
fn test_ensure_schema_preserves_existing() {
    let plan = ExecutionPlan::new()
        .schema_order(["X", "Y", "Z"]);

    let schema = FrozenSchema::from_plan(&["X", "Y", "Z"], &plan);
    let mut builder = TableBuilder::with_plan(100, Arc::new(plan));

    // Pre-add "X" with a push
    builder.begin_row();
    builder.put_field("X", Value::Str(Cow::Borrowed("hello")));
    builder.end_row();

    let rows_before = builder.num_rows();
    let cols_before = builder.num_columns();

    // ensure_schema adds Y and Z, but does not duplicate X
    builder.ensure_schema(&schema).unwrap();
    assert_eq!(builder.num_rows(), rows_before);
    assert_eq!(builder.num_columns(), cols_before + 2);
}
```

## Performance characteristics

### Memory savings

Without `field_types`:

- Each string value is stored as `String` (heap allocation)
- After parse, a casting pass converts strings to typed arrays
- Peak memory: original strings + typed arrays = 2x

With `field_types`:

- Values are parsed directly into typed builders (no `String` allocation)
- No casting pass needed
- Peak memory: typed arrays only = 1x

For a 533 MB file with 10 columns, this saves ~200-400 MB of peak memory.

### CPU savings

Without `field_types`:

- Every value is stored as a string (allocation + copy)
- Post-parse casting: `str::parse::<i64>()` for each value
- Two passes over the data

With `field_types`:

- Values are parsed once, directly into the correct type
- No post-parse casting
- One pass over the data

Typical savings: 10-20% of total parse time for numeric-heavy workloads.

### Export speed

Without `schema_order`:

- Each batch may have different column order
- Engine must merge batches sequentially (merge path)
- ~4,497 MB/s on 533 MB

With `schema_order`:

- Every batch has identical column order
- Engine exports batches in parallel (fast path)
- ~4,980 MB/s on 533 MB (+11%)

With `schema_order` + projection:

- Scanner skips unwanted fields via `row_satisfied` byte-jump
- ~7,630 MB/s on 533 MB (+80%)

## Troubleshooting

### "unknown field not in frozen schema"

This error means a field appeared in the file that was not in `schema_order`,
and the schema is exact (default behavior). Options:

1. Add the field to `schema_order` (complete the schema)
2. Use full-scan discovery instead of explicit schema (omit `schema_order`)
3. Unknown columns are automatically appended when using `schema_order`
   (partial schema is supported by default in parallel streaming)

### Column order does not match `schema_order`

Check that you are calling `sort_columns` or that the engine is using the
fast export path. The merge path preserves discovery order, not `schema_order`.

### Filter not working on numeric columns

Ensure `field_types` is set for the filtered columns. Without it, values are
strings and the filter falls back to lexicographic comparison.

### Memory higher than expected

Check that `field_types` is set for all numeric columns. Without it, the
engine stores intermediate strings, doubling memory for those columns.
