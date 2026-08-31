# Storage and export

This page covers `crates/rypipe-core/src/columnar.rs:34` `StrColumn`, `ColumnBuilder`, dictionary, and `crates/rypipe-core/src/arrow_export.rs` plus the `finish` path in `engine.rs:289`.

## Arrow types produced

| `ColumnBuilder` variant | Arrow `DataType` | Array type |
|-------------------------|-----------------|------------|
| `String(StrColumn)` | `Utf8` | `StringArray` (OffsetBuffer plus Buffer plus NullBuffer) |
| `Int64(NullableColumn<i64>)` | `Int64` | `Int64Array` |
| `Float64(NullableColumn<f64>)` | `Float64` | `Float64Array` |
| `Boolean(NullableColumn<bool>)` | `Boolean` | `BooleanArray` |
| `Date32(NullableColumn<i32>)` | `Date32` | `Date32Array` |
| `Timestamp(TimeUnit, NullableColumn<i64>)` | `Timestamp(unit, None)` | `PrimitiveArray<TimestampSecondType>` etc. |
| `Dictionary { codes, dict, index }` | `Dictionary(Int32, Utf8)` | `DictionaryArray<Int32Type>` with `Int32Array` keys and `StringArray` values |

`arrow_datatype(&self) -> DataType` and `to_arrow_array(&self) -> Result<ArrayRef>` implement the mapping. `Timestamp` branches on `TimeUnit` to the correct `PrimitiveArray` type.

## Null handling

* `StrColumn` has `validity: Vec<bool>`. `to_arrow` builds `NullBuffer` only if some validity is false; otherwise `None` (all valid). Offsets still advance by 0 for null entries so `offsets[i]==offsets[i+1]`.

* Numeric and boolean builders are `NullableColumn<T>`. `collect::<Int64Array>()` etc. preserves nulls.

* Missing columns in `engines_to_record_batches` become `null_array(&types[name], e.row_count)` (a `NullArray` of the unified type).

* `Value::Null` and unparseable strings both become `None`.

## String arena

`StrColumn::push(Option<&str>)` appends bytes to `data` and `data.len()` to `offsets`. `pop` truncates `data` to `offsets.last()`. `append` merges another column with base shifted offsets. `get` slices `data[offsets[i]..offsets[i+1]]` and does `from_utf8` (safe because input was validated via `simdutf8`).

Capacity: `with_capacity(cap)` reserves `cap+1` offsets and `cap*16` data bytes. `Pipeline` passes `cap = bytes.len() / 512` (min 64) or `estimated_rows`.

## Numeric and temporal parsing

`push` and `push_str` for typed builders use:

* `lexical::parse::<i64,_>` and `<f64,_>` for `Int64`/`Float64`
* `s.parse::<bool>()` for `Boolean`
* `parse_date32` (`chrono::NaiveDate::parse_from_str("%Y-%m-%d")` minus epoch) for `Date32`
* `parse_timestamp` (tries `"%Y-%m-%dT%H:%M:%S%.f"`, then `" %H:%M:%S%.f"`, then bare date as midnight, then converts via `and_utc` to seconds, millis, micros, or nanos depending on `TimeUnit`) for `Timestamp`

`Value::Int64` into `Float64` widens, into `String` stringifies, into `Dictionary` encodes. Other cross type cases become `None`.

## Dictionary

`Dictionary { codes: NullableColumn<i32>, dict: Vec<String>, index: HashMap<String,i32> }`

* `dict_code` does `if let Some(&code) = index.get(v) { return code }` else `dict.push`, `index.insert`.

* `extend_owned` for `Dictionary` remaps the right dictionary into the left in one pass via `remap: Vec<i32> = b_dict.iter().map(|val| dict_code(a_dict, a_index, val)).collect()` then translates `a_codes.extend(b_codes.iter().map(|c| c.map(|idx| remap[idx as usize])))`.

* `try_upgrade_to_dict` is described in [Columnar](./columnar.md) and [Engine](./engine.md).

## Unification and promotion

`unify_variants` and `promote_to_variant` are the only places that change storage type. `merge.rs:extend` and `engines_to_record_batches` call `unify_variants(skey, okey)` before `extend_owned`; if `None`, they return `Error::Merge("column '{name}' has conflicting types ({skey} vs {okey}); provide explicit field_types")`. Promotions are `Int64 -> Float64` via `take` plus `map as f64`, and `String -> Dictionary` via rebuilding `dict`/`index`/`codes`. Same key is a no op.

## Finish

`TableBuilder::finish` (also `ColumnarSink::finish`) does:

1. `normalize` (truncate `len > row_count` and clear `row_dirty`)
2. Early `new_empty` if `column_order` empty
3. `auto_dict_upgrade` (if `plan.auto_dict`)
4. `sort_columns` by `schema_order`
5. Build `fields` plus `arrays` by iterating `column_order` and `get_column(name)` to call `arrow_datatype` and `to_arrow_array`
6. `Schema::new(fields)` plus `RecordBatch::try_new`

No borrowed bytes outlive `finish`; `StrColumn` owns its data and numeric Vecs are owned.

## Compare filter reapplication

`arrow_export::apply_compare_filter` exists for callers filtering an already built `RecordBatch` (for example merging). It is *not* the per row filter. Per row `FilterPredicate::check` is authoritative; `apply_compare_filter` only reapplies pure `Compare` and `And` of `Compare` via Arrow compute (`compare_columns` casts both to `Float64` if numeric else `Utf8`, then `gt`, `lt`, `gt_eq`, `lt_eq`, `eq`, `neq`, and `and` for `And`, plus `filter_record_batch`). Trees containing `Or`, `Not`, `Equal`, or `NotEqual` are returned unchanged to avoid double null semantics. This matches the comment in `arrow_export.rs:24`.

## Error mapping

`StrColumn::to_arrow` can return `ArrowError` (offsets, null buffer). `to_arrow_array` for dictionary can return `ArrowError` for `DictionaryArray::try_new`. Both surface as `Error::Arrow` and, via `rypipe-python`, as `PyException` with `Arrow error: ...`. `Utf8` from `simdutf8` surfaces as `ParseError` in Python.
