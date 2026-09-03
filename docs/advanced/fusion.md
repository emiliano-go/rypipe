# Pushdown fusion

`rypipe` splits ingestion into two layers:

1. **Adapter layer**: reads bytes, splits them into records, and emits `Value` rows.
2. **Engine layer**: builds typed Arrow arrays, applies filters, and exports record batches.

Pushdown fusion is the process by which the Python `Pipeline` rewrites a chain of lightweight stages into a single `ExecutionPlan`. The engine then applies rename, drop, filter, and cast while it parses each row, instead of materializing a full table and running Python transforms afterward.

## A fused pipeline

```python
from rypipe import RenameFields, DropFields, FilterRows, CastTypes

source = MyAdapter("data.log")
result = (
    source
    | RenameFields({"old_name": "new_name"})
    | DropFields(["internal_id"])
    | FilterRows(field="status", op="==", value="active")
    | CastTypes({"amount": float})
).to_arrow()
```

When the pipeline reaches `to_arrow()`, the stage list is collapsed into one plan. The Rust parser:

- renames `old_name` to `new_name` as fields arrive;
- skips `internal_id` entirely (it is not allocated);
- drops rows whose `status` is not `active` before they leave the builder;
- casts `amount` to `float64` once, during parse.

Rows that fail the filter are never materialized; dropped columns are never allocated; and casts happen once inside Rust instead of twice in Python and Rust.

## The `ExecutionPlan` fields

```rust
pub struct ExecutionPlan {
    pub field_map: HashMap<String, String>,       // rename
    pub drop_fields: HashSet<String>,             // drop
    pub field_types: HashMap<String, FieldType>,  // cast
    pub dictionary_columns: HashSet<String>,      // explicit dict encoding
    pub filter: Option<FilterPredicate>,          // per-row (all kinds)
    pub schema_order: Vec<String>,                // output column order
    pub auto_dict: bool,                          // auto-dict upgrade
    pub dict_threshold: Option<f64>,              // auto-dict ratio (default 0.05)
    pub dict_max_size: Option<usize>,             // auto-dict max entries (default 256)
}
```

The plan is built by `_build_plan_kwargs()` on the Python side and consumed by `TableBuilder` on the Rust side.

## Field resolution order

Inside `TableBuilder`, every emitted field goes through this pipeline:

1. `field_map` renames the raw field name.
2. `drop_fields` checks the resolved name; if dropped, the field is ignored.
3. `field_types` / `dictionary_columns` chooses the storage type.
4. `filter` rejects rows during `end_row`.

This order matters. A filter runs on the resolved name, so it must be written in post-rename terms. A cast type is attached to the resolved name as well.

## What is fusable

Fusable stages implement `_plan_kwargs()` and merge cleanly into an `ExecutionPlan`:

| Stage | Plan field | Notes |
|-------|------------|-------|
| `RenameFields` | `field_map` | Multiple renames merge into one map. |
| `DropFields` | `drop_fields` | Merges as a set union. |
| `CastTypes` | `field_types` | Later casts overwrite earlier ones for the same field. |
| `FilterRows` predicate | `filter` | Constant (`field`/`op`/`value`) with `==`/`!=`, or column-to-column (`field_a`/`op`/`field_b`); both are evaluated per-row during parse. |
| `FilterRowsAny` / `FilterRowsAll` / `FilterRowsNot` | `filter` | `And`, `Or`, `Not` trees built from the same leaf shapes; evaluated per-row with short circuiting; fully fusable. |

`FilterRows` is fusable when it uses a constant predicate (`field`, `op`, `value`) with `==` or `!=`, or a column-to-column predicate (`field_a`, `op`, `field_b`). `FilterRowsAny`, `FilterRowsAll`, and `FilterRowsNot` are also fusable; they build `And`, `Or`, `Not` trees from the same leaves. All are evaluated per-row during parsing with native-typed comparison and numeric promotion; mismatched types or nulls fail the row, with `Not` flipping the result. Chaining `FilterRows` stages is an implicit `And` (see `plan_split`).

## What is not fusable

Non-fusable stages still work, but they run over the Arrow table after the engine finishes:

- Python callables (`lambda` or any callable stage).
- Stateful transforms such as window or aggregate stages.
- `FilterRows` wrapping a callable predicate (runtime-computed values).
- Custom stages that do not implement `_plan_kwargs()`.

When a non-fusable stage is present, the pipeline automatically falls back to a row stream or table transform path. The fusable prefix still runs in Rust; only the suffix runs in Python.

## Inspecting `plan_overrides` in an adapter

Adapters that subclass `rypipe.Source` receive fused plan kwargs through `_read_arrow`:

```python
class MySource(Source):
    def _read_arrow(self, *, plan_overrides=None, **kwargs):
        plan_overrides = plan_overrides or {}
        print(plan_overrides)
        # {
        #   "field_mapping": {"old_name": "new_name"},
        #   "drop_fields": ["internal_id"],
        #   "filter": {"field": "status", "op": "==", "value": "active"},
        #   "field_types": {"amount": "float64"},
        # }
        return my_rust_read(path=self.path, **plan_overrides, **kwargs)
```

The Rust side merges these kwargs into an `ExecutionPlan` with `execution_plan_from_kwargs` from `rypipe-python`, or constructs the plan manually:

```rust
use rypipe_core::{ExecutionPlan, FieldType};

let plan = ExecutionPlan::new()
    .rename("old_name", "new_name")
    .drop("internal_id")
    .type_as("amount", FieldType::Float64)
    .filter_eq("status", "active");
```

If an adapter ignores `plan_overrides`, fused stages silently fall back to Python execution over a full table. That is one of the most expensive anti-patterns.

## Order of operations across the pipeline

Fusable stages commute in the plan, but the engine applies them in a fixed order:

```
raw field name
    |
    v
rename (field_map)
    |
    v
drop check (drop_fields)
    |
    v
type selection (field_types / dictionary_columns)
    |
    v
per-row filter (Equal / NotEqual / Compare / And / Or / Not)
    |
    v
builder append (Vec plus map, single hash, dirty bitmask)
    |
    v
finish: sort by schema_order, auto-dict, Arrow export
```

Because drop happens before type selection, you cannot cast a dropped field.
Because every filter : including column-to-column comparisons : runs before the
row is committed, rejected rows consume no Arrow storage.

## When fusion does not help

Fusion is not free if the adapter cannot act on the plan. An adapter that always parses every field into Python objects and then builds Arrow will not benefit; the engine must receive fields through `put_field` and honor `wants()`. If the adapter is a thin wrapper around a library that returns full Python dicts, fusion only removes a small amount of Python overhead.

Fusion also cannot help when the workload is dominated by I/O. If the file is on a slow network share, reducing CPU work may not change wall-clock time. Profile first.

## Summary

- Fuse `RenameFields`, `DropFields`, `CastTypes`, and `FilterRows` predicates
  (constant and column-to-column) by implementing `_read_arrow(plan_overrides=...)`.
- Inspect `plan_overrides` to confirm that stages reach the Rust parser.
- Non-fusable stages run after the engine; keep them out of the hot path when throughput matters.
