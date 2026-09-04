# Columnar storage

This page documents `columnar.rs` in depth. It is the storage layer that makes
`TableBuilder` fast and Arrow export cheap.

## StrColumn

```rust
pub(crate) struct StrColumn {
    data: Vec<u8>,
    offsets: Vec<i32>,
    validity: ValidityBitmap,
}
```

This is exactly the Arrow `StringArray` layout (offsets + bytes + null bitmap)
without per-cell `String` allocation.

### Fields

- **`data: Vec<u8>`**: One contiguous arena. Every string's bytes are
  appended sequentially. No per-cell allocation.

- **`offsets: Vec<i32>`**: `len + 1` entries. `offsets[i]..offsets[i+1]`
  is the byte range for value `i`. Initialized with `[0]` so `push` can
  compute the next offset as `data.len()`.

- **`validity: ValidityBitmap`**: One bit per row. `true` means present;
  `false` means null (no bytes for that slot, but offsets still advance by 0).

### Operations

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
  offsets: `base = self.data.len() as i32`, then extends offsets. O(n) in
  offsets, not in bytes.

- **`to_arrow()`**: Builds Arrow `StringArray` by wrapping three buffers
  with `OffsetBuffer`, `ScalarBuffer`, `Buffer`, and `NullBuffer`. Block
  copy, not per-cell.

### ValidityBitmap

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

## ColumnBuilder

```rust
pub(crate) enum ColumnBuilder {
    String(StrColumn),
    Int64(PrimColumn<i64>),
    Float64(PrimColumn<f64>),
    Boolean(PrimColumn<bool>),
    Date32(PrimColumn<i32>),
    Timestamp(TimeUnit, PrimColumn<i64>),
    Dictionary {
        codes: NullableColumn<i32>,
        data: Vec<u8>,
        offsets: Vec<i32>,
        index: FxHashMap<Box<str>, i32>,
    },
}
```

Each variant stores a `PrimColumn<T>` (or `StrColumn` for strings). Created
by `ExecutionPlan::column_type` at first use.

### PrimColumn<T>

```rust
pub(crate) struct PrimColumn<T: Copy> {
    data: Vec<T>,
    validity: ValidityBitmap,
}
```

Flat contiguous array. Zero-copy Arrow export via `to_arrow()` which moves
`Vec<T>` into `ScalarBuffer`. Boolean specialization via `to_arrow_bool()`.

### Push paths

`push_value(Value<'_>)` is called for every field:
- `Value::Str(s)` → `push_str(Some(s))` → parse according to column type
- `Value::Int64(i)` into Int64 is native, into Float64 widens
- Cross-type mismatches become `None`

`dict_code(dict, index, v)` does hash lookup + insert. Average O(1).

### Auto dictionary

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

### Incremental dictionary unification

When `auto_dict=True` in parallel mode, chunks may produce different
dictionaries. The incremental path:

1. Per-chunk `auto_dict_upgrade` in parallel
2. Find first divergent dictionary across chunks
3. Build `SeedDict` from first chunk
4. `unify_dictionaries`: global dict + per-chunk remap tables (O(dict_size))
5. `remap_codes`: in-place code remap via `get_unchecked` (parallel)
6. `replace_dict`: swap local dict for unified dict

This avoids the serial merge path while keeping the fast export path.

### Merging and promotion

`extend_owned(other)` merges by consuming other. Both must be same variant:
- String via `StrColumn::append` (base shift)
- Numeric via `Vec::append`
- Timestamp checks `unit_a == unit_b` else `Error::Merge`
- Dictionary remaps via `dict_code` per value

`unify_variants(a, b)` reconciles: same→same, int64+float64→float64,
string+dictionary→dictionary, else None.

`promote_to_variant(target)` mutates in place: Int64→Float64, String→Dict.

### TypedValue

```rust
pub(crate) enum TypedValue<'a> {
    Str(&'a str), Int64(i64), Float64(f64),
    Bool(bool), Date32(i32), Timestamp(i64),
}
```

Borrowed view for filter evaluation. `get_typed_value` borrows directly from
storage without allocation. `get_filter_value` formats as `String` for
`Equal`/`NotEqual` predicates (dates via `format_date32`, timestamps via
`format_timestamp`).

## Performance characteristics

### String push

`push_str` for the String variant does:

1. Compute offset: `data.len()`
2. Extend data: `data.extend_from_slice(s.as_bytes())`
3. Push offset: `offsets.push(data.len() as i32)`
4. Push validity: `validity.push(true)`

Cost: ~10 ns per string (arena append + 3 pushes). No per-cell allocation.

### Numeric push

`push_value` for Int64/Float64 does:

1. Match on Value variant
2. `lexical::parse` or direct cast
3. Push to `PrimColumn::data` (Vec push)
4. Push to `validity`

Cost: ~5 ns per numeric value. No parsing overhead for typed values.

### Dictionary push

`dict_code` does:

1. `index.get(v)`: HashMap lookup (~5 ns)
2. If missing: `dict.push(v.to_owned())` + `index.insert` (~20 ns amortized)
3. `codes.push(Some(code))`: NullableColumn push

Cost: ~5 ns per value (amortized O(1) lookup).

### Arrow export

`to_arrow_array` uses `std::mem::take` to move internal buffers into Arrow
arrays. This is zero-copy, no data copying, just pointer moves.

For `StrColumn`: moves `data`, `offsets`, and `validity` into `StringArray`.
For `PrimColumn<T>`: moves `data` and `validity` into `PrimitiveArray`.
For Dictionary: moves `codes`, `dict` into `DictionaryArray`.

Cost: ~100 ns per column (buffer moves + schema construction).
