# Execution Plan { #execution-plan }

`plan.rs` defines the compiled execution plan that controls all pipeline
operations. An `ExecutionPlan` is created once per parse and shared (via
`Arc`) across all chunks.

## Structure { #structure }

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
    pub strict_types: bool,
    pub max_split_chunks: Option<usize>,
    pub observer: Option<Arc<dyn RowObserver>>,
}
```

### Fields { #fields }

- **`field_map`**: Rename mapping: raw name → output name.
- **`drop_fields`**: Fields to drop entirely (not stored).
- **`field_types`**: Type overrides (Int64, Float64, Boolean, Dictionary, etc.).
- **`dictionary_columns`**: Explicit dictionary columns.
- **`filter`**: Composable predicate tree.
- **`schema_order`**: Desired output column order; when non-empty, also acts
  as a projection (see `resolve_field`).
- **`auto_dict`**: Enable automatic dictionary upgrade.
- **`dict_threshold`**: Max distinct ratio (default 0.05).
- **`dict_max_size`**: Max dictionary entries (default 256).
- **`strict_types`**: Abort on non-null values that fail to parse into their
  declared type instead of silently storing null (builder: `with_strict_types`).
- **`max_split_chunks`**: Cap on bounded-streaming chunks (default 100,000;
  builder: `with_max_split_chunks`).
- **`observer`**: Optional `RowObserver` whose hooks fire from parse threads
  (builder: `with_observer`).

## Builder API { #builder-api }

```rust
ExecutionPlan::new()
    .rename("raw_name", "output_name")
    .drop("internal_id")
    .type_as("amount", FieldType::Float64)
    .dictionary("status")
    .filter_eq("status", "active")
    .schema_order(["quantity", "amount", "status"])
```

## resolve_field { #resolve_field }

The hot path for name resolution:

```rust
pub fn resolve_field<'a>(&'a self, raw: &'a str) -> Option<&'a str> {
    // 1. Apply rename first (field_map)
    let resolved = self.field_map.get(raw).map_or(raw, |s| s.as_str());
    // 2. Then check drop set on the resolved name
    if self.drop_fields.contains(resolved) {
        return None;
    }
    // 3. Schema projection: when schema_order is non-empty, drop resolved
    //    names not listed there unless the filter references them
    if !self.schema_order.is_empty()
        && !self.schema_order.contains(resolved)
        && !self.filter_references(resolved)
    {
        return None;
    }
    // 4. Return resolved (or original if no rename)
    Some(resolved)
}
```

Application order: rename first, then drop, then schema projection, matching
left-to-right pipeline semantics. Returns `None` for dropped fields. When
`schema_order` is non-empty it acts as a projection: unlisted fields resolve
to `None` (unless referenced by the filter, so predicates still evaluate;
they are projected out of the final batch). The adapter checks this before
extraction.

## column_type { #column_type }

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

## FieldType enum { #fieldtype-enum }

```rust
pub enum FieldType {
    String,
    Int64,
    Float64,
    Boolean,
    Dictionary,
    Date32,
    Timestamp(TimeUnit, Option<Box<str>>),
    Decimal128(u8),
}
```

`Timestamp` carries an optional chrono format string, parsed from specs like
`timestamp[ms,format=%Y%m%d]` (unit defaults to `us`). `Decimal128(u8)` takes
a scale (default 18 via the `decimal128` spec, capped at 38 for Arrow compatibility).

## FilterPredicate { #filterpredicate }

```rust
pub enum FilterPredicate {
    // Value comparison
    Equal { field: String, value: String },
    NotEqual { field: String, value: String },
    Compare { field_a: String, op: CompareOp, field_b: String },
    CompareLiteral { field: String, op: CompareOp, value: String },

    // String predicates
    StartsWith { field: String, value: String },
    EndsWith { field: String, value: String },
    Contains { field: String, value: String },
    Strip { field: String, op: CompareOp, value: String },
    Lower { field: String, op: CompareOp, value: String },
    Upper { field: String, op: CompareOp, value: String },
    Replace { field: String, old: String, new: String, op: CompareOp, value: String },
    Length { field: String, op: CompareOp, value: String },
    Regex { field: String, re: RegexSpec },

    // Membership
    In { field: String, values: Vec<String> },
    NotIn { field: String, values: Vec<String> },

    // Type/null predicates
    IsNull { field: String },
    IsType { field: String, field_type: FieldType },

    // Arithmetic
    ArithmeticCompare {
        field: String,
        arith_op: ArithOp,
        arith_value: f64,
        cmp_op: CompareOp,
        cmp_value: String,
    },

    // Boolean
    NotField { field: String },
    Always(bool),

    // Logical combinators
    And(Box<FilterPredicate>, Box<FilterPredicate>),
    Or(Box<FilterPredicate>, Box<FilterPredicate>),
    Not(Box<FilterPredicate>),
}
```

`RegexSpec` pairs the pattern string with its compiled `regex::Regex`; the
Python filter spec op `"regex"` builds one. Compiled patterns are capped at
**1 MiB** to limit memory use and reduce ReDoS risk.

### Check method { #check-method }

`check(&columns, &field_index, row_index, &plan)` (crate-internal) evaluates
the predicate against column values for a given row. Short-circuits on `And`
(first false) and `Or` (first true). For `Compare`, it uses native-typed
comparison with numeric promotion via `TypedValue`.

### CompareOp { #compareop }

```rust
pub enum CompareOp { Gt, Lt, Ge, Le, Eq, Ne }
```

### ArithOp { #arithop }

```rust
pub enum ArithOp { Add, Sub, Mul, Div }
```

Compare uses native-typed comparison with numeric promotion (Int64↔Float64).

## Plan from Python kwargs { #plan-from-python-kwargs }

`rypipe-python` converts Python kwargs to `ExecutionPlan` via
`execution_plan_from_kwargs` in `plan_kwargs.rs`:

- `field_mapping` → `field_map`
- `drop_fields` → `drop_fields`
- `field_types` → `field_types`
- `filter` → `filter` (nested And/Or/Not trees)
- `schema` → `schema_order`
- `dictionary_columns` → `dictionary_columns`
- `auto_dict` → `auto_dict`
- `auto_dict_threshold` → `dict_threshold`
- `auto_dict_max_size` → `dict_max_size`
- `strict_types` → `strict_types`
- `max_split_chunks` → `max_split_chunks`
- `observer` → `observer` (dict of hook callables, e.g. `{"on_row_rejected": fn}`)

The conversion is done by `execution_plan_from_kwargs` in `plan_kwargs.rs`.
It handles nested filter specs (`and`, `or`, `not`) and leaf ops such as
`is_null`, `is_type`, and `regex`.

## How plans flow through the system { #how-plans-flow-through-the-system }

1. **Adapter creation**: `Pipeline::new(splitter, parser).with_plan(plan)`
2. **Plan sharing**: `Arc::clone(&self.plan)` for each chunk's `TableBuilder`
3. **Name resolution**: `plan.resolve_field(name)` called per field per row
4. **Type selection**: `plan.column_type(name)` called once per new column
5. **Filter evaluation**: `plan.filter.check(...)` called per row in `finish_row`
6. **Schema ordering**: `plan.schema_order` used in `sort_columns` and
   `schema_insert_index`

## Filter predicate evaluation { #filter-predicate-evaluation }

`FilterPredicate::check` evaluates the tree against committed column values.
The engine uses two evaluation paths:

1. **Post-commit** (`check`): reads from finished column arrays via
   `get_value`; `IsType` uses `ColumnBuilder::is_type_at`. Used for
   predicates that only need committed data.
2. **Pre-commit** (`eval_predicate` in `table_builder.rs`): a three-state
   evaluator (`PredicateState::Pass` / `Fail` / `Undecided`) that reads the
   row buffer via `get_buffered_value()`. It handles all predicate variants
   during predicate-first deferred materialization; `Undecided` means the
   field has not been seen yet in the current row.

```rust
pub(crate) fn check(&self, columns: &[ColumnBuilder], field_index: &HashMap<String, usize>,
                    row_index: usize, plan: &ExecutionPlan) -> bool {
    match self {
        Equal { field, value } => {
            let actual = get_value(columns, field_index, field, plan, row_index);
            actual.as_deref() == Some(value.as_str())
        }
        Compare { field_a, op, field_b } => {
            let va = get_typed_value(columns, field_index, plan, field_a, row_index);
            let vb = get_typed_value(columns, field_index, plan, field_b, row_index);
            compare_typed(va, *op, vb)
        }
        StartsWith { field, value } => actual.starts_with(value),
        EndsWith { field, value } => actual.ends_with(value),
        Contains { field, value } => actual.contains(value),
        In { field, values } => values.contains(actual),
        IsNull { field } => get_value(columns, field_index, field, plan, row_index).is_none(),
        IsType { field, field_type } => column.is_type_at(row_index, field_type),
        Regex { field, re } => re.compiled.is_match(actual),
        NotField { field } => actual.is_none_or(|v| v.is_empty()),
        Always(b) => *b,
        And(a, b) => a.check(...) && b.check(...),
        Or(a, b) => a.check(...) || b.check(...),
        Not(a) => !a.check(...),
        // ... other variants follow same pattern
    }
}
```

Short-circuiting: `And` stops on first `false`, `Or` stops on first `true`.
Every variant reads committed columns through `get_value` / `is_type_at`;
the row buffer is only consulted on the pre-commit `eval_predicate` path.

## C2 reorder optimization { #c2-reorder-optimization }

For `And` and `Or` predicates, operands are reordered by document position
(via `pred_ordinal`) so the predicate on the earlier column is checked
first. This enables earlier short-circuit in both combinators.

## Plan construction from Python { #plan-construction-from-python }

```python
import crxml

src | crxml.RenameFields({"old": "new"}) | crxml.DropFields(["id"]) | \
    crxml.FilterRows(field="status", op="==", value="active") | \
    crxml.CastTypes({"amount": float})
```

Each stage has `_plan_kwargs()` that returns a dict. `plan_split` merges
all kwargs into a single `ExecutionPlan`; multiple `FilterRows` specs are
combined with an implicit `and`, and `observer` hook dicts merge per-hook
(chaining callables when several stages provide the same hook). Non-fusable
stages (custom callables) run over the returned table.

Filters can also be built with the expression API (the adapter's
re-exported `col(...)`); such predicates expose `_to_spec()` and are fusable
like plain spec dicts.
`FilterRows(field=..., is_null=False)` becomes
`{"not": {"field": ..., "op": "is_null"}}`.

