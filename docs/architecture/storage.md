# Storage and export { #storage-and-export }

This page covers Arrow type mapping, null handling, the string arena,
numeric/temporal parsing, dictionary encoding, unification, and the finish
path. See [Columnar storage](./columnar.md) for the storage types themselves.

## Arrow types produced { #arrow-types-produced }

| ColumnBuilder variant | Arrow DataType | Array type |
|----------------------|---------------|------------|
| String(StrColumn) | Utf8 | StringArray (OffsetBuffer + ScalarBuffer + NullBuffer) |
| Int64(PrimColumn<i64>) | Int64 | Int64Array (ScalarBuffer + NullBuffer) |
| Float64(PrimColumn<f64>) | Float64 | Float64Array |
| Boolean(PrimColumn<bool>) | Boolean | BooleanArray (BooleanBuffer) |
| Date32(PrimColumn<i32>) | Date32 | Date32Array |
| Timestamp(TimeUnit, Option<Box<str>>, PrimColumn<i64>) | Timestamp(unit, None) | PrimitiveArray<TimestampXType> |
| Decimal128(u8, PrimColumn<i128>) | Decimal128(38, scale) | Decimal128Array |
| Dictionary { codes, data, offsets, index } | Dictionary(Int32, Utf8) | DictionaryArray<Int32Type> |

`arrow_datatype()` maps each variant to its Arrow type. `to_arrow_array()`
moves internal buffers into the native array via `std::mem::take` (zero-copy
for `StrColumn` and Dictionary). `PrimColumn<T>` exports collect into a fresh
`ScalarBuffer`, and `to_arrow_bool` repacks into a `BooleanBuffer`, so
primitive and boolean exports copy.

## Null handling { #null-handling }

### StrColumn { #strcolumn }

`ValidityBitmap` stores one bit per row. `to_arrow` builds `NullBuffer`
only if some validity is false; otherwise `None` (all valid). Offsets
advance by 0 for null entries so `offsets[i] == offsets[i+1]`. The null
string occupies zero bytes in the data arena.

### PrimColumn<T> { #primcolumnt }

`ValidityBitmap` + `Vec<T>`. `to_arrow()` preserves nulls via `NullBuffer`.
The boolean specialization `to_arrow_bool()` packs `Vec<bool>` into
`BooleanBuffer` via `ScalarBuffer<u8>`.

### Missing columns { #missing-columns }

In `engines_to_record_batches`, columns present in some chunks but missing
in others become `null_array(&types[name], e.row_count)`: a `NullArray`
of the unified type. This is a block fill, not per-cell.

### Value::Null and unparseable strings { #valuenull-and-unparseable-strings }

Both become `None` in the column builder. `push_str` returns `()`; the column
stores `None` for unparseable strings (e.g., "abc" into Int64 column).
`Value::Null` is explicitly null. Exception: with `plan.strict_types` set,
a non-null string that fails `parses_as` for the declared `field_types` type
is recorded as a strict violation and surfaced as an error by `finish()` (see
the finish path below) instead of becoming `None`.

## String arena { #string-arena }

`StrColumn` stores strings in a contiguous byte arena with offset indexing:

```
data:    [h][e][l][l][o][w][o][r][l][d]     ← two strings
offsets: [0,     5,          10]             ← byte boundaries
```

### push { #push }

`push(Some("hello"))` appends 5 bytes to `data`, pushes `data.len()` (=5)
to `offsets`, pushes `true` to validity. `push(None)` appends nothing to
`data`, so the current offset is repeated, and pushes `false` to validity.
No per-cell allocation beyond arena growth.

### pop { #pop }

Pops validity, pops offsets, truncates `data` to `offsets.last()`. Used
for last-write-wins (duplicate field in same row) and row rejection.

### append { #append }

Merges another column by base-shifting offsets: `base = self.data.len()`.
Then extends `self.offsets` with `other.offsets[1..].map(|o| o + base)` and
copies `other.data` via `extend_from_slice`. O(n) in offsets plus O(n) in
bytes for the arena memcpy.

### Capacity { #capacity }

`with_capacity(cap)` reserves `cap+1` offsets and `cap*16` data bytes.
`Pipeline` passes `cap = (bytes.len() / est).max(64)` where
`est = estimate_bytes_per_row(sample).max(512)`.

## Numeric and temporal parsing { #numeric-and-temporal-parsing }

When a `Value::Str` is pushed to a typed column, the string is parsed:

- **Int64**: `lexical::parse::<i64, _>(s.as_bytes())`: fast, SIMD-optimized
- **Float64**: `lexical::parse::<f64, _>(s.as_bytes())`
- **Boolean**: `s.parse::<bool>()`: accepts only "true"/"false"; other
  spellings become null (the wider "1"/"0"/"yes"/"no" set is used only by
  the `is_type_at` type-detection path)
- **Date32**: `chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")` minus Unix epoch
- **Decimal128**: `parse_decimal128(s, scale)`: sign, integer, and fraction
  parts are combined into a scaled `i128`; extra fraction digits beyond
  `scale` are truncated
- **Timestamp**: if the column declares `format=`, the custom chrono format
  is tried first (as `NaiveDateTime`, then `NaiveDate` at midnight). Then
  the built-ins: `"%Y-%m-%dT%H:%M:%S%.f"`, `"%Y-%m-%d %H:%M:%S%.f"`, and
  bare date as midnight. Converts via `and_utc()` to the target TimeUnit.

Unparseable strings become `None`. Cross-type conversions mostly succeed:
`Value::Int64` into `Float64` widens, into `String` stringifies, into
`Dictionary` encodes; `Float64` into `Int64` truncates; `Bool` converts into
`String`/`Dictionary`; `Date32` and `Timestamp` convert into
`Int64`/`Float64`/`String`/`Dictionary`. Cases with no defined conversion
become `None`.

## Dictionary { #dictionary }

```rust
Dictionary {
    codes: NullableColumn<i32>,
    data: Vec<u8>,
    offsets: Vec<i32>,
    index: FxHashMap<Box<str>, i32>,
}
```

- **`data`**: contiguous byte buffer for all dictionary values (like `StrColumn`).
- **`offsets`**: byte offsets into `data` for each dictionary entry.
- **`index`**: hash lookup from value to code. Average O(1).
- **`dict_code`**: hash lookup in `index`, insert if missing (append to `data`/`offsets`).
- **`extend_owned`**: remaps right dictionary into left via a remap vector.
  For each value in right's data, look up its code in left's index. If
  missing, append to left's data. Then translate right's codes.
- **`try_upgrade_to_dict`**: see [Columnar](./columnar.md).

## Unification and promotion { #unification-and-promotion }

When merging chunks with different storage types:

- `unify_variants("int64", "float64")` → `Some("float64")`
- `unify_variants("string", "dictionary")` → `Some("dictionary")`
- `unify_variants("int64", "string")` → `None` (error)

`promote_to_variant("float64")` on an Int64 column: takes the `Vec<i64>`,
maps each element to `f64`, produces a new `PrimColumn<f64>`.

Used in `merge::extend` and `engines_to_record_batches`.

## Finish path { #finish-path }

`TableBuilder::finish()` does:

1. **Deferred errors**: returns `Err` if `unknown_error` (frozen-schema
   violation) or `strict_error` (`strict_types` violation) was recorded
   during ingestion.
2. **Observer hook**: calls `observer.on_chunk_finished(accepted + rejected,
   accepted, rejected)` when the plan declares an observer.
3. **normalize**: truncate any column with `len > row_count` (partial row
   from truncated chunk), clear `row_dirty`. Idempotent.
4. Early return with `RecordBatch::new_empty` if `column_order` is empty.
5. **auto_dict_upgrade**: if `plan.auto_dict`, iterate columns and upgrade
   String builders with low cardinality to Dictionary. Threshold: 5% distinct
   ratio, max 256 entries, min 512 rows. The distinct cap is floored at 16
   entries (`(len * ratio).max(16)`) so tiny columns can still upgrade.
6. **Schema projection** (when `schema_order` is non-empty): build fields and
   arrays directly from `schema_order`. Columns declared but absent from the
   data are null-filled; non-schema columns are projected out. `sort_columns`
   is not called on this branch.
7. **sort_columns** (only when `schema_order` is empty, where it no-ops):
   reorder `column_order` by `schema_order` rank. Does not reorder `columns`
   or `field_index`; those stay insertion-ordered.
8. **Build fields + arrays**: iterate `column_order`, look up each builder
   via `field_index`, call `arrow_datatype` and `to_arrow_array`.
9. **Schema::new(fields)** + **RecordBatch::try_new**.

No borrowed bytes outlive `finish`. `StrColumn` owns its data and numeric
Vecs are owned. `to_arrow_array` moves string and dictionary buffers into
Arrow arrays via `std::mem::take` (zero-copy); primitive exports collect
into fresh buffers.

## Compare filter reapplication { #compare-filter-reapplication }

`apply_compare_filter` in `arrow_export.rs` re-applies pure `Compare` and
`And` trees using Arrow compute kernels. Other trees (`Or`, `Not`, `Equal`,
`NotEqual`) are returned unchanged because per-row evaluation is
authoritative.

This exists for callers that filter an already-built `RecordBatch` (e.g.,
after merging chunks). It is not the per-row filter. Per-row
`FilterPredicate::check` is authoritative; `apply_compare_filter` only
reapplies pure Compare/And via `compare_columns` (casts to Float64 or Utf8,
then `gt`/`lt`/`gt_eq`/`lt_eq`/`eq`/`neq`/`and`) and `filter_record_batch`.

## Memory layout { #memory-layout }

For a 533 MB XML file with 480K rows and 10 columns:

- **StrColumn (strings)**: ~480K offsets (1.9 MB) + ~200 MB data + ~60 KB validity
- **PrimColumn<i64>**: ~3.8 MB data + ~60 KB validity
- **PrimColumn<f64>**: ~3.8 MB data + ~60 KB validity
- **Dictionary**: ~500 entries × ~20 bytes = ~10 KB dict + ~1.9 MB codes
- **Total per chunk** (16 chunks): ~15 MB per chunk, ~240 MB total
- **Arrow export**: string and dictionary buffers move (zero-copy);
  primitive exports collect into fresh buffers

The arena-based `StrColumn` is the dominant memory consumer. For files with
many unique strings, the arena can grow to 2-3× the raw data size. For
files with low cardinality, dictionary encoding reduces this significantly.
