# Columnar storage { #columnar-storage }

This page documents `columnar.rs` in depth. It is the storage layer that makes
`TableBuilder` fast and Arrow export cheap.

## StrColumn { #strcolumn }

```rust
pub(crate) struct StrColumn {
    data: Vec<u8>,
    offsets: Vec<i32>,
    validity: ValidityBitmap,
}
```

This is exactly the Arrow `StringArray` layout (offsets + bytes + null bitmap)
without per-cell `String` allocation.

### Fields { #fields }

- **`data: Vec<u8>`**: One contiguous arena. Every string's bytes are
  appended sequentially. No per-cell allocation.

- **`offsets: Vec<i32>`**: `len + 1` entries. `offsets[i]..offsets[i+1]`
  is the byte range for value `i`. Initialized with `[0]` so `push` can
  compute the next offset as `data.len()`.

- **`validity: ValidityBitmap`**: One bit per row. `true` means present;
  `false` means null (no bytes for that slot, but offsets still advance by 0).

### Operations { #operations }

- **`with_capacity(cap)`**: Preallocates `offsets` with `cap + 1` and
  `data` with `cap * 16` (heuristic 16 bytes per string).

- **`push(Option<&str>)`**: Extends `data` if `Some`, pushes `data.len()`
  to `offsets`, pushes `is_some` to `validity`. No per-cell allocation
  beyond arena growth.

- **`pop`**: Undoes the last push: pops validity, pops offsets, truncates
  data to the last offset.

- **`get(i)`**: Checks validity, slices `data[offsets[i]..offsets[i+1]]`,
  returns `Option<&str>`.

- **`append(&mut self, other)`**: Merges another column by base-shifting
  offsets: `base = self.data.len() as i32`, then copies `other.data` into
  `self.data` and extends offsets. O(n) in both bytes and offsets; the
  base shift only avoids recomputing offsets.

- **`to_arrow()`**: Builds Arrow `StringArray` by wrapping three buffers
  with `OffsetBuffer`, `ScalarBuffer`, `Buffer`, and `NullBuffer`. Block
  copy, not per-cell.

### ValidityBitmap { #validitybitmap }

```rust
struct ValidityBitmap {
    bits: Vec<u8>,
    len: usize,
    null_count: usize,
}
```

One bit per row packed into bytes. Supports `push` (allocates new byte every
8 rows), `pop`, `split_off`, `append`. Converts to Arrow `NullBuffer` via
`into_arrow()`.

## ColumnBuilder { #columnbuilder }

```rust
pub(crate) enum ColumnBuilder {
    String(StrColumn),
    Int64(PrimColumn<i64>),
    Float64(PrimColumn<f64>),
    Boolean(PrimColumn<bool>),
    Date32(PrimColumn<i32>),
    Timestamp(TimeUnit, Option<Box<str>>, PrimColumn<i64>),
    Decimal128(u8, PrimColumn<i128>),
    Dictionary {
        codes: NullableColumn<i32>,
        data: Vec<u8>,
        offsets: Vec<i32>,
        index: FxHashMap<Box<str>, i32>,
    },
}
```

Each variant stores a `PrimColumn<T>` (or `StrColumn` for strings).
`Timestamp` carries the unit and an optional chrono format string (tried
before the built-in ISO layouts when parsing); `Decimal128` carries the
scale, storing `value * 10^scale` as `i128`. Created by
`ColumnBuilder::with_capacity(est, &ty)` in `TableBuilder::ensure_columns`,
where `ty` comes from `ExecutionPlan::column_type`.

### PrimColumn<T> { #primcolumnt }

```rust
pub(crate) struct PrimColumn<T: Copy> {
    data: Vec<T>,
    validity: ValidityBitmap,
}
```

Flat contiguous array. `to_arrow()` moves the buffers out via
`std::mem::take` but converts per element
(`data.into_iter().map(A::Native::from).collect()`), so it allocates a new
`ScalarBuffer` rather than moving the `Vec` wholesale. Boolean specialization
via `to_arrow_bool()`, which maps to bytes and repacks into a
`BooleanBuffer`.

### Push paths { #push-paths }

`push_value(Value<'_>)` is called for every field:

- `Value::Str(s)` → `push_str(Some(s))` → parse according to column type
  (Decimal128 parses to a scaled `i128` via `parse_decimal128`)
- `Value::Int64(i)` into Int64 is native, into Float64 widens
- `Value::Float64(f)` into Float64 is native, into Int64 narrows (`f as i64`)
- Typed values (`Int64`/`Float64`/`Bool`/`Date32`/`Timestamp`) into String
  columns are formatted as text (`"42"`, `"true"`, ISO dates and timestamps);
  into Dictionary columns the same text is dict-encoded
- Other cross-type mismatches become `None`

`dict_code(dict, index, v)` does hash lookup + insert. Average O(1).

### Auto dictionary { #auto-dictionary }

`try_upgrade_to_dict(min_rows, max_ratio, max_size)` upgrades String to
Dictionary when cardinality is low:

1. Only String builders; others are no-ops.
2. If `len < min_rows` (512 default), stay as String.
3. Count distinct via `FxHashSet<&str>` over `iter().flatten()` (skip nulls).
4. Compute cap: `min(max(16, len * max_ratio), max_size)`. Floor of 16 lets
   tiny columns upgrade; cap respects `dict_threshold` (default 0.05) and
   `dict_max_size` (default 256).
5. If distinct > cap, stay as String.
6. Otherwise build dict/index/codes from the old StrColumn via `dict_code`.

Called after each chunk parse when `plan.auto_dict` is true, and after merge
via `TableBuilder::auto_dict_upgrade`.

### Incremental dictionary unification { #incremental-dictionary-unification }

When `auto_dict=True` in parallel mode, chunks may produce different
dictionaries. The incremental path:

1. Per-chunk `auto_dict_upgrade` in parallel
2. Find first divergent dictionary across chunks
3. Build `SeedDict` from first chunk
4. `unify_dictionaries`: global dict + per-chunk remap tables (O(dict_size))
5. `remap_codes`: in-place code remap via `get_unchecked` (serial loop over
   chunk engines; only step 1 runs under rayon)
6. `replace_dict`: swap local dict for unified dict

This avoids the serial merge path while keeping the fast export path.

### Merging and promotion { #merging-and-promotion }

`extend_owned(other)` merges by consuming other. Both must be same variant:

- String via `StrColumn::append` (base shift)
- Numeric via `Vec::append`
- Timestamp checks `unit_a == unit_b` else `Error::Merge`
- Decimal128 checks `scale_a == scale_b` else `Error::Merge`
- Dictionary remaps via `dict_code` per value

`unify_variants(a, b)` reconciles: same→same, int64+float64→float64,
string+dictionary→dictionary, else None.

`promote_to_variant(target)` mutates in place: Int64→Float64, String→Dict.

### TypedValue { #typedvalue }

```rust
pub(crate) enum TypedValue<'a> {
    Str(&'a str), Int64(i64), Float64(f64),
    Bool(bool), Date32(i32), Timestamp(i64),
}
```

Borrowed view for filter evaluation. `get_typed_value` borrows directly from
storage without allocation. The string-filter path tries `get_filter_view`
first, which borrows `&str` with no allocation for String and Dictionary
columns; typed columns return `None` there and fall back to
`get_filter_value`, which formats as `String` (dates via `format_date32`,
timestamps via `format_timestamp`).

Related helpers: `is_type_at` backs the `IsType` predicate, `parses_as`
checks whether a string parses into a declared `FieldType` for strict-types
mode, and `variant_key` returns the storage-variant tag (including timestamp
unit and decimal scale) used to keep schemas consistent across chunks.

## Performance characteristics { #performance-characteristics }

### String push { #string-push }

`push_str` for the String variant does:

1. Compute offset: `data.len()`
2. Extend data: `data.extend_from_slice(s.as_bytes())`
3. Push offset: `offsets.push(data.len() as i32)`
4. Push validity: `validity.push(true)`

Cost: ~10 ns per string (arena append + 3 pushes). No per-cell allocation.

### Numeric push { #numeric-push }

`push_value` for Int64/Float64 does:

1. Match on Value variant
2. `lexical::parse` or direct cast
3. Push to `PrimColumn::data` (Vec push)
4. Push to `validity`

Cost: ~5 ns per numeric value. No parsing overhead for typed values.

### Dictionary push { #dictionary-push }

`dict_code` does:

1. `index.get(v)`: HashMap lookup (~5 ns)
2. If missing: `dict.push(v.to_owned())` + `index.insert` (~20 ns amortized)
3. `codes.push(Some(code))`: NullableColumn push

Cost: ~5 ns per value (amortized O(1) lookup).

### Arrow export { #arrow-export }

`to_arrow_array` uses `std::mem::take` to move internal buffers out, but
zero-copy holds only for `StrColumn` (and the dictionary data/offsets
buffers): `PrimColumn<T>` converts per element
(`data.into_iter().map(A::Native::from).collect()`) into a new allocation,
and booleans are additionally repacked into a bit-packed `BooleanBuffer`.

For `StrColumn`: moves `data`, `offsets`, and `validity` into `StringArray`.
For `PrimColumn<T>`: per-element conversion of `data` into the Arrow native
type; `validity` moves into the null buffer. Decimal128 exports as Arrow
`Decimal128(38, scale)`.
For Dictionary: moves `codes`, `dict` into `DictionaryArray`.

Cost: ~100 ns per column for `StrColumn`/Dictionary (buffer moves + schema
construction); typed columns pay one linear conversion pass.
