# Execution plan

`crates/rypipe-core/src/plan.rs` is the pushdown target. Everything an adapter can fuse is expressed here.

## ExecutionPlan

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

Default is a no op. Builder methods (`rename`, `drop`, `drop_many`, `type_as`, `dictionary`, `filter_eq`, `filter_ne`, `filter_compare`, `schema_order`, `with_auto_dict`, `with_dict_threshold`, `with_dict_max_size`) each insert or set the field and return `Self` for chaining. Fields are public for direct mutation when needed.

Resolution order (also documented in `docs/architecture/index.md`):

1. `field_map` renames raw field (`raw -> output`)
2. `drop_fields` checks the resolved name (`None` means drop)
3. `field_types` or `dictionary_columns` chooses storage (`column_type`)
4. `filter` rejects rows in `finish_row`
5. `schema_order` reorders `column_order` in `finish`

## FieldType

```rust
pub enum FieldType { String, Int64, Float64, Boolean, Dictionary, Date32, Timestamp(TimeUnit) }
```

`from_str` parses: `string`, `int64`, `float64`, `bool` or `boolean`, `dictionary`, `date32`, `timestamp` (defaults to Microsecond), `timestamp[s]`, `timestamp[ms]`, `timestamp[us]` or `timestamp[µs]`, `timestamp[ns]`.

`column_type(&self, name: &str) -> FieldType` checks `field_types` first, then `dictionary_columns`, else `String`. This is called once per distinct column at first use (in `ensure_column_idx`), not per row.

## FilterPredicate

```rust
pub enum FilterPredicate {
    NotEqual { field: String, value: String },
    Equal { field: String, value: String },
    Compare { field_a: String, op: CompareOp, field_b: String },
    And(Box<FilterPredicate>, Box<FilterPredicate>),
    Or(Box<FilterPredicate>, Box<FilterPredicate>),
    Not(Box<FilterPredicate>),
}
```

`Compare` is column to column with numeric promotion; `Equal`/`NotEqual` are constant string comparisons. `And`/`Or`/`Not` compose trees arbitrarily. Helpers `FilterPredicate::all`, `any`, `not` build boxed trees.

`CompareOp` (`Gt`, `Lt`, `Ge`, `Le`, `Eq`, `Ne`) parses from `>`, `gt`, `<`, `lt`, `>=`, `ge`, `<=`, `le`, `==`, `eq`, `!=`, `ne`.

## Resolve

```rust
pub fn resolve_field<'a>(&'a self, raw: &'a str) -> Option<&'a str> {
    let resolved = self.field_map.get(raw).map_or(raw, |s| s.as_str());
    if self.drop_fields.contains(resolved) { return None; }
    Some(resolved)
}
```

Rename first, then drop on the resolved name. Fast path in `TableBuilder::push_field` skips this entirely when both maps are empty (`field_map.is_empty() && drop_fields.is_empty()`), so the hot keep-all case pays zero hash.

When maps are not empty, each field still pays one or two hashes. The optimization in `decoder::ColumnarSink` (`resolve` plus `put_field_resolved`) lets adapters pay it once instead of twice (`wants` plus `put_field`). See [Decoder](./decoder.md) and [Optimizations](./optimizations.md).

## Check

```rust
pub(crate) fn check(&self, columns: &[ColumnBuilder], field_index: &HashMap<String, usize>, row_index: usize, plan: &ExecutionPlan) -> bool
```

This is the per row filter used in `TableBuilder::finish_row`. It takes the `Vec<ColumnBuilder>` plus `field_index` (single hash per field) instead of a `HashMap` view (which would double hash). Steps:

* `Equal`/`NotEqual` call `get_value` which does `get_column(columns, field_index, resolve(field, plan)).and_then(|b| b.get_filter_value(row_index))` and compares `Option<String>` to the constant.

* `Compare` fetches `get_column` for each side, then `get_typed_value(row_index)` to get `TypedValue`, then `compare_typed(&a, op, &b)`. `compare_typed` promotes `Int64` vs `Float64` to `f64` via `partial_cmp`; other same type pairs use `Ord`; mixed non numeric or different temporal units return false. Null operands fail the row (caller checks `Some`).

* `And` short circuits on first failure, `Or` on first success, `Not` negates. A missing field fails the inner leaf, which `Not` can flip to keep. There is a `#[cfg(test)]` shim `check_map` that preserves old HashMap semantics for tests that still hold a map view.

`check` is called with `row_index == self.row_count` (the uncommitted row) before `row_count` is advanced. If it returns false, `finish_row` pops every column (undo) and returns without counting the row.

## Python surface

`crates/rypipe-python/src/plan_kwargs.rs` converts Python kwargs to `ExecutionPlan`. It accepts `filter` as a dict that may be a leaf (`field`, `op`, `value` or `field_a`, `op`, `field_b`) or a tree (`and: [spec, ...]`, `or: [spec, ...]`, `not: spec`) and recurses via `parse_filter_spec`, `combine_list`, `parse_leaf_spec`. Valid ops and types are validated there and surface as `PlanError`.

On the Python side, `FilterRows` plus `FilterRowsAny`, `FilterRowsAll`, `FilterRowsNot` build those dicts via `_plan_kwargs`, and `fusion::plan_split` merges multiple `filter` specs into `{"and": [...]}` so chaining `FilterRows` stages does not drop the earlier filter.

## Schema order

`schema_order: Vec<String>` is the desired output column order. Columns not listed keep first appearance order after the listed ones. `TableBuilder::sort_columns` and `schema_insert_index` implement this. The order does not affect `field_index` or `columns` Vec order, only `column_order`.

## Auto dict

`auto_dict: bool` plus `dict_threshold` (default 0.05) and `dict_max_size` (default 256) feed `ColumnBuilder::try_upgrade_to_dict`. See [Columnar](./columnar.md) for the heuristic and [Execution](./execution.md) for fast vs merge path interaction.
