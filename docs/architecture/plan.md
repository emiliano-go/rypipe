# Execution Plan

`plan.rs` defines the compiled execution plan that controls all pipeline
operations. An `ExecutionPlan` is created once per parse and shared (via
`Arc`) across all chunks.

## Structure

```rust
pub struct ExecutionPlan {
    pub field_map: HashMap<String, String>,
    pub drop_fields: HashSet<String>,
    pub field_types: HashMap<String, FieldType>,
    pub dictionary_columns: HashSet<String>,
    pub filter: Option<FilterPredicate>,
    pub schema_order: Vec<String>,
    pub auto_dict: bool,
    pub dict_threshold: Option<f64>,
    pub dict_max_size: Option<usize>,
}
```

### Fields

- **`field_map`** — Rename mapping: raw name → output name.
- **`drop_fields`** — Fields to drop entirely (not stored).
- **`field_types`** — Type overrides (Int64, Float64, Bool, Dictionary, etc.).
- **`dictionary_columns`** — Explicit dictionary columns.
- **`filter`** — Composable predicate tree.
- **`schema_order`** — Desired output column order.
- **`auto_dict`** — Enable automatic dictionary upgrade.
- **`dict_threshold`** — Max distinct ratio (default 0.05).
- **`dict_max_size`** — Max dictionary entries (default 256).

## Builder API

```rust
ExecutionPlan::new()
    .rename("raw_name", "output_name")
    .drop("internal_id")
    .type_as("amount", FieldType::Float64)
    .dictionary("status")
    .filter_eq("status", "active")
    .schema_order(["quantity", "amount", "status"])
```

## resolve_field

The hot path for name resolution:

```rust
pub fn resolve_field<'a>(&'a self, raw: &'a str) -> Option<&'a str> {
    // 1. Apply rename first (field_map)
    let resolved = self.field_map.get(raw).map_or(raw, |s| s.as_str());
    // 2. Then check drop set on the resolved name
    if self.drop_fields.contains(resolved) {
        return None;
    }
    // 3. Return resolved (or original if no rename)
    Some(resolved)
}
```

Application order: rename first, then drop, matching left-to-right pipeline
semantics. Returns `None` for dropped fields. The adapter checks this before
extraction.

## column_type

Determines storage variant for a field:

```rust
pub fn column_type(&self, name: &str) -> FieldType {
    if let Some(ft) = self.field_types.get(name) {
        return ft.clone();
    }
    if self.dictionary_columns.contains(name) {
        return FieldType::Dictionary;
    }
    FieldType::String
}
```

## FieldType enum

```rust
pub enum FieldType {
    String,
    Int64,
    Float64,
    Boolean,
    Dictionary,
    Date32,
    Timestamp(TimeUnit),
}
```

## FilterPredicate

```rust
pub enum FilterPredicate {
    Equal { field: String, value: String },
    NotEqual { field: String, value: String },
    Compare { field_a: String, op: CompareOp, field_b: String },
    And(Box<FilterPredicate>, Box<FilterPredicate>),
    Or(Box<FilterPredicate>, Box<FilterPredicate>),
    Not(Box<FilterPredicate>),
}
```

### Check method

`check(&columns, &field_index, row_index, &plan)` evaluates the predicate
against column values for a given row. Short-circuits on `And` (first false)
and `Or` (first true). For `Compare`, it uses native-typed comparison with
numeric promotion via `TypedValue`.

### CompareOp

```rust
pub enum CompareOp { Gt, Lt, Ge, Le, Eq, Ne }
```

Compare uses native-typed comparison with numeric promotion (Int64↔Float64).

## Plan from Python kwargs

`rypipe-python` converts Python kwargs to `ExecutionPlan` via
`execution_plan_from_kwargs` in `plan_kwargs.rs`:
- `field_mapping` → `field_map`
- `drop_fields` → `drop_fields`
- `field_types` → `field_types`
- `filter` → `filter` (nested And/Or/Not trees)
- `schema` → `schema_order`
- `dictionary_columns` → `dictionary_columns`
- `auto_dict` → `auto_dict`

The conversion is done by `execution_plan_from_kwargs` in `plan_kwargs.rs`.
It handles nested filter specs (`and`, `or`, `not`) and validates that
compare filters have both fields typed or both untyped.

## How plans flow through the system

1. **Adapter creation**: `Pipeline::new(splitter, parser).with_plan(plan)`
2. **Plan sharing**: `Arc::clone(&self.plan)` for each chunk's `TableBuilder`
3. **Name resolution**: `plan.resolve_field(name)` called per field per row
4. **Type selection**: `plan.column_type(name)` called once per new column
5. **Filter evaluation**: `plan.filter.check(...)` called per row in `finish_row`
6. **Schema ordering**: `plan.schema_order` used in `sort_columns` and
   `schema_insert_index`

## Filter predicate evaluation

`FilterPredicate::check` evaluates the tree against column values:

```rust
pub fn check(&self, columns: &[ColumnBuilder], field_index: &HashMap<String, usize>,
             row_index: usize, plan: &ExecutionPlan) -> bool {
    match self {
        Equal { field, value } => {
            let resolved = plan.resolve_field(field)?;
            let idx = field_index.get(resolved)?;
            columns[*idx].get_filter_value(row_index) == *value
        }
        Compare { field_a, op, field_b } => {
            let va = get_typed_value(columns, field_index, plan, field_a, row_index);
            let vb = get_typed_value(columns, field_index, plan, field_b, row_index);
            compare_typed(va, *op, vb)
        }
        And(a, b) => a.check(...) && b.check(...),
        Or(a, b) => a.check(...) || b.check(...),
        Not(a) => !a.check(...),
    }
}
```

Short-circuiting: `And` stops on first `false`, `Or` stops on first `true`.

## C2 reorder optimization

For `Compare` predicates, operands are reordered by document position so
the predicate column is checked first. This enables earlier short-circuit
in the `And` tree.

## Plan construction from Python

```python
from rypipe import RenameFields, DropFields, FilterRows, CastTypes

src | RenameFields({"old": "new"}) | DropFields(["id"]) | \
    FilterRows(field="status", op="==", value="active") | \
    CastTypes({"amount": float})
```

Each stage has `_plan_kwargs()` that returns a dict. `plan_split` merges
all kwargs into a single `ExecutionPlan`. Non-fusable stages (custom
callables) run over the returned table.

