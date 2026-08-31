# Columnar storage

This page documents `crates/rypipe-core/src/columnar.rs` in full. It is the storage layer that makes `TableBuilder` fast and Arrow export cheap.

## StrColumn

```rust
pub(crate) struct StrColumn {
    data: Vec<u8>,
    offsets: Vec<i32>,
    validity: Vec<bool>,
}
```

This is exactly the Arrow `StringArray` layout (offsets plus bytes plus null bitmap) without per cell `String` allocation.

* `data: Vec<u8>` is one contiguous arena. Every string's bytes are appended sequentially.

* `offsets: Vec<i32>` has `len + 1` entries. `offsets[i] .. offsets[i+1]` is the byte range for value `i`. It is initialized with `[0]` so `push` can compute the next offset as `data.len()`.

* `validity: Vec<bool>` marks null vs present. `true` means present; `false` means null (the buffer contains no bytes for that slot, but offsets still advance by 0).

Operations:

* `with_capacity(cap)` preallocates `offsets` with `cap + 1` and `data` with `cap * 16` (heuristic 16 bytes per string). This matches `TableBuilder::estimated_rows`.

* `push(v: Option<&str>)` extends `data` if `Some`, pushes `data.len()` to `offsets`, and pushes `is_some` to `validity`. No allocation per cell beyond the arena growth.

* `pop` undoes the last push: pops `validity`, pops `offsets`, truncates `data` to the last offset.

* `len` is `validity.len()`.

* `get(i: usize) -> Option<&str>` checks validity, slices `data[offsets[i] .. offsets[i+1]]`, and does `from_utf8` (the slice is known UTF-8 from `validate`, but the check is kept for safety).

* `append(&mut self, other: &StrColumn)` merges another column by base shifting offsets: `base = self.data.len() as i32`, then `self.offsets.extend(other.offsets[1..].iter().map(|o| o + base))`. This is O(n) in offsets, not in bytes.

* `to_arrow(&mut self) -> Result<ArrayRef>` builds an Arrow `StringArray` by wrapping the three buffers with `OffsetBuffer`, `Buffer`, and `NullBuffer`. When all validity are true, `nulls` is `None`. This is a block copy of two buffers, not per cell.

## ColumnBuilder

```rust
pub(crate) enum ColumnBuilder {
    String(StrColumn),
    Int64(NullableColumn<i64>),
    Float64(NullableColumn<f64>),
    Boolean(NullableColumn<bool>),
    Date32(NullableColumn<i32>),
    Timestamp(TimeUnit, NullableColumn<i64>),
    Dictionary { codes: NullableColumn<i32>, dict: Vec<String>, index: HashMap<String, i32> },
}
```

Each variant stores a `NullableColumn<T>` (or `StrColumn` for strings). There is exactly one builder per column, created by `ExecutionPlan::column_type` at first use.

* `String` is `StrColumn`.
* `Int64`, `Float64`, `Boolean` are `NullableColumn<T>` with `lexical::parse` for string inputs.
* `Date32(NullableColumn<i32>)` stores days since epoch. Parsing uses `chrono::NaiveDate::parse_from_str("%Y-%m-%d")`.
* `Timestamp(TimeUnit, NullableColumn<i64>)` stores raw integers in the column's `TimeUnit` (Second, Millisecond, Microsecond, Nanosecond). Parsing tries `"%Y-%m-%dT%H:%M:%S%.f"`, then `" %H:%M:%S%.f"`, then bare `"%Y-%m-%d"` as midnight. Timezone handling is left to adapters that emit `Value::Timestamp` directly.
* `Dictionary { codes, dict, index }` stores `codes: NullableColumn<i32>` plus `dict: Vec<String>` (id to string) and `index: HashMap<String,i32>` (string to id). This is the write path for `FieldType::Dictionary` and for auto dict upgrade.

Variant keys for unification (10 strings):

* `string`, `int64`, `float64`, `boolean`, `date32`, `timestamp[s]`, `timestamp[ms]`, `timestamp[us]`, `timestamp[ns]`, `dictionary`

`variant_key(&self) -> &'static str` returns the key. Timestamp units are distinguished so merging `timestamp[s]` with `timestamp[ms]` is an error rather than silent promotion.

## Push paths

`push_value(&mut self, value: Value<'_>)` is called for every field of every row. It handles typed `Value` variants:

* `Value::Null` becomes `None`.
* `Value::Str(s)` calls `push_str(Some(s))`, which parses according to column type: `lexical::parse` for numbers, `parse::<bool>` for booleans, `parse_date32`/`parse_timestamp` for temporals. Unparseable becomes `None`.
* `Value::Int64(i)` into `Int64` is native, into `Float64` widens `i as f64`, into `String` stringifies, into `Dictionary` encodes via `dict_code`.
* Similarly for `Float64`, `Bool`, `Date32`, `Timestamp`. Cross type mismatches (for example `Bool` into `Int64`) become `None`.

`push(&mut self, value: Option<String>)` and `push_str(&mut self, value: Option<&str>)` are the string entry points. `push_str` avoids allocation for typed columns (it parses and discards the string). Both handle `Dictionary` by calling `dict_code`.

`dict_code(dict: &mut Vec<String>, index: &mut HashMap<String,i32>, v: &str) -> i32` does `if let Some(&code) = index.get(v) { return code }` else `dict.push(v.to_owned())` and insert. Average O(1).

## Auto dictionary

`try_upgrade_to_dict(&mut self, min_rows: usize, max_ratio: f64, max_size: usize)` upgrades a `String` builder to `Dictionary` when cardinality is low. Steps:

1. Only `String` builders; others are no ops.
2. If `len < min_rows` (512 in `TableBuilder::auto_dict_upgrade`), leave as `String`.
3. Count distinct via `FxHashSet<&str>` over `iter().flatten()` (skipping nulls).
4. Compute cap: `ratio_cap = ((len as f64 * max_ratio) as usize).max(16).min(max_size)`. Floor of 16 lets tiny columns upgrade; cap respects `dict_threshold` (default 0.05) and `dict_max_size` (default 256).
5. If distinct > cap, leave as `String`.
6. Otherwise build `dict`, `index`, `codes` from the old `StrColumn` via `dict_code`.

Called after each chunk parse when `plan.auto_dict` is true, and after merge via `TableBuilder::auto_dict_upgrade` which respects `plan.dict_threshold` and `plan.dict_max_size`.

## Merging and promotion

`extend_owned(&mut self, other: ColumnBuilder) -> Result<()>` merges `other` into `self` by consuming `other`. Both must be the same variant (after promotion). Cases:

* `String` via `StrColumn::append` (base shift)
* `Int64`/`Float64`/`Boolean`/`Date32`/`Timestamp` via `Vec::append`
* `Timestamp` checks `unit_a == unit_b` else `Error::Merge`
* `Dictionary` remaps `b`'s dictionary into `a`'s via `dict_code` per value, then translates codes via `remap[idx]`

`unify_variants(a: &str, b: &str) -> Option<&'static str>` reconciles two variant keys:

* same → same
* `int64` plus `float64` → `float64`
* `string` plus `dictionary` → `dictionary`
* otherwise `None` (irreconcilable)

`promote_to_variant(&mut self, target: &'static str) -> Result<()>` mutates in place:

* `Int64` to `Float64` via `std::mem::take` then `map(|o| o.map(|n| n as f64))`
* `String` to `Dictionary` via taking the `StrColumn`, building `dict`/`index`/`codes`
* Same key is a no op
* Any other target returns `Error::Merge`

Used in `merge::extend` and `merge::engines_to_record_batches` before `extend_owned`.

## Arrow export

`arrow_datatype(&self) -> DataType` maps each variant to Arrow type: `Utf8`, `Int64`, `Float64`, `Boolean`, `Date32`, `Timestamp(unit, None)`, `Dictionary(Int32, Utf8)`.

`to_arrow_array(&self) -> Result<ArrayRef>` builds the native array:

* `String` via `StrColumn::to_arrow`
* `Int64`/`Float64`/`Boolean`/`Date32` via `iter().copied().collect::<Array>()`
* `Timestamp(unit, v)` via `collect::<PrimitiveArray<TimestampXType>>` per unit
* `Dictionary` via `Int32Array` keys plus `StringArray` values into `DictionaryArray::<Int32Type>::try_new`

## Typed value view

`TypedValue<'a>` is a borrowed view for filter evaluation:

```rust
pub(crate) enum TypedValue<'a> { Str(&'a str), Int64(i64), Float64(f64), Bool(bool), Date32(i32), Timestamp(i64) }
```

`get_typed_value(&self, index: usize) -> Option<TypedValue<'_>>` borrows directly from storage (dictionary decodes to `Str` via dict lookup). `get_filter_value` formats as `String` for `Equal`/`NotEqual` (dates via `format_date32`, timestamps via `format_timestamp`).

This enum lets `FilterPredicate::Compare` run natively per row with numeric promotion, without allocation.
