# Dictionary encoding { #dictionary-encoding }

Arrow dictionaries store string values as integer indices into a separate value table. In `rypipe`, this can reduce memory 5-20x for low-cardinality string columns such as status codes, country codes, or enums. This page explains how dictionaries work in the engine, when they help, and when they force the merge path and hurt throughput.

## How Arrow dictionaries work in rypipe { #how-arrow-dictionaries-work-in-rypipe }

`rypipe-core` stores string columns in a `StrColumn`: a contiguous byte arena plus `i32` offsets and a validity bitmap. When a column is dictionary-encoded, the engine instead builds:

- a `codes: NullableColumn<i32>` array of indices;
- a `data: Vec<u8>` contiguous byte buffer for all dictionary values;
- an `offsets: Vec<i32>` byte offsets into `data` for each entry;
- an `index: FxHashMap<Box<str>, i32>` lookup from value to code.

For a `status` column with rows `["active", "pending", "active", null, "active"]`:

```
row values    "active"   "pending"   "active"     null     "active"
                 │           │           │          │          │
                 ▼           ▼           ▼          ▼          ▼
codes         [    0    ,     1     ,    0    ,   null  ,    0    ]   i32 per row

index          "active"  ──► 0        FxHashMap<Box<str>, i32>
               "pending" ──► 1        built on first sight of each value

offsets        [0, 6, 13]             byte range of each entry in data
data           "activepending"        every value stored once, contiguous
```

Reading row `i` means: take `codes[i]`, slice `data[offsets[code]..offsets[code+1]]`.
Writing a value means: look it up in `index` (one hash probe), or append it
to `data`/`offsets` and insert it into `index` on first sight.

On Arrow export, these become a `DictionaryArray` with `Int32` indices and a `StringArray` dictionary. The layout is exactly what Arrow compute kernels expect, so downstream filters and group-by operations can use the encoded form directly.

## Explicit `dictionary_columns` { #explicit-dictionary_columns }

The safest way to use dictionary encoding is to declare it explicitly:

```python
source = MyAdapter(
    "data.log",
    schema=["id", "ts", "amount", "status"],
    field_types={"id": "int64", "amount": "float64"},
    dictionary_columns=["status"],
)
```

This tells the engine to build a dictionary column for `status` from the first row. There is no inference pass and no runtime heuristic cost.

In Rust:

```rust
let plan = ExecutionPlan::new()
    .dictionary("status");
```

## `auto_dict` heuristics { #auto_dict-heuristics }

`auto_dict=True` asks the engine to guess which string columns should be dictionary-encoded. The heuristic has a small runtime cost: it tracks the number of distinct values and the total row count for each string column. When the ratio of distinct values to rows falls below a threshold, the column is upgraded to dictionary encoding at finish time.

Two knobs tune the upgrade:

- `auto_dict_threshold` (Rust: `dict_threshold`): maximum fraction of rows allowed as distinct values. Default `0.05`.
- `auto_dict_max_size` (Rust: `dict_max_size`): maximum number of distinct values allowed. Default `256`.

```python
source = MyAdapter(
    "data.log",
    auto_dict=True,
    auto_dict_threshold=0.10,   # upgrade columns up to 10% distinct
    auto_dict_max_size=1024,    # and up to 1024 distinct values
)
```

Use `auto_dict=True` when:

- you do not know the schema or cardinality in advance;
- the file is small enough that the tracking cost is negligible;
- downstream operations benefit from dictionary form.

Use `auto_dict=False` when:

- throughput is the top priority;
- columns are high cardinality or already numeric.

## When dictionaries help memory { #when-dictionaries-help-memory }

Dictionary encoding helps most when:

- the column has low cardinality (many repeated values);
- the strings are long relative to the index size;
- the column is used in filters, joins, or group-by operations that can work on integer codes.

Examples:

- HTTP status codes: ~10 distinct values, very short strings.
- Country codes: ~200 distinct values, short strings.
- Product categories: tens to thousands of distinct values, often repeated.

For very short strings (one or two characters), the memory savings are smaller because the string data is already small.

## Dictionaries in parallel mode { #when-dictionaries-force-the-merge-path }

In parallel mode, each chunk builds its own local dictionary. The engine handles this without a full merge:

1. **Per-chunk upgrade**: with `auto_dict`, each chunk upgrades its own low-cardinality columns in parallel.
2. **Unify and remap**: the engine then unifies the chunk dictionaries into one global dictionary (a small serial step over distinct values, not rows) and remaps each chunk's codes. Chunks still export independently, so this stays on the fast path.

The full **merge path** is now reserved for chunks that disagree on column storage types (for example, one chunk typed a column `int64` and another `string`). Merge concatenates builders serially before export and raises a precise `Error::Merge` for irreconcilable mismatches.

If you need both dictionaries and maximum throughput, consider:

- declaring `dictionary_columns` explicitly so the storage type is fixed from the first row;
- giving `auto_dict` tighter thresholds so fewer columns upgrade;
- filtering with keyword-form filters, which run per-row during parse and never force a merge.

## Fast path vs merge path { #fast-path-vs-merge-path }

`ParallelExecutor` has two internal paths:

- **Fast path**: each chunk is exported as its own `RecordBatch` in parallel. This covers the common cases: no dictionaries, explicit `dictionary_columns`, `auto_dict` (via the incremental unify-and-remap step), and compare filters, which are evaluated per-row during parse.
- **Merge path**: chunk builders are merged sequentially before export. Only schema-inconsistent chunks take this path today.

If a merge does happen, peak RSS rises because all chunk builders coexist until the serial merge finishes.

## Summary { #summary }

- Use `dictionary_columns` for known low-cardinality strings; it is predictable and avoids heuristic cost.
- Use `auto_dict=True` when cardinality is unknown; tune with `auto_dict_threshold` (default 0.05) and `auto_dict_max_size` (default 256).
- In parallel mode, dictionaries stay on the fast path via per-chunk upgrade plus a serial unify-and-remap step; only schema-inconsistent chunks take the merge path.
