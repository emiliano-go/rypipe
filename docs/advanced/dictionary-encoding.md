# Dictionary encoding { #dictionary-encoding }

Arrow dictionaries store string values as integer indices into a separate value table. In `rypipe`, this can reduce memory 5-20x for low-cardinality string columns such as status codes, country codes, or enums. This page explains how dictionaries work in the engine, when they help, and when they force the merge path and hurt throughput.

## How Arrow dictionaries work in rypipe { #how-arrow-dictionaries-work-in-rypipe }

`rypipe-core` stores string columns in a `StrColumn`: a contiguous byte arena plus `i32` offsets and a validity bitmap. When a column is dictionary-encoded, the engine instead builds:

- a `codes: NullableColumn<i32>` array of indices;
- a `data: Vec<u8>` contiguous byte buffer for all dictionary values;
- an `offsets: Vec<i32>` byte offsets into `data` for each entry;
- an `index: FxHashMap<Box<str>, i32>` lookup from value to code.

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

Use `auto_dict=True` when:

- you do not know the schema or cardinality in advance;
- the file is small enough that the tracking cost is negligible;
- downstream operations benefit from dictionary form.

Use `auto_dict=False` when:

- throughput is the top priority;
- columns are high cardinality or already numeric;
- you are running parallel mode (see below).

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

## When dictionaries force the merge path { #when-dictionaries-force-the-merge-path }

In parallel mode, dictionary encoding forces the merge path. Each chunk builds its own local dictionary. Before export, the engine must merge all chunk dictionaries into a single global dictionary and remap codes. This has two consequences:

1. **Serial merge**: the merge step is not parallel, so it can become a bottleneck for many small chunks.
2. **Higher peak RSS**: all chunk builders must coexist until the merge finishes, and the global dictionary may be larger than any local one.

If you need both dictionaries and maximum throughput, consider:

- using columnar mode instead of parallel mode;
- declaring `dictionary_columns` explicitly so only those columns pay the merge cost;
- filtering after export instead of forcing a merge with compare filters.

## Fast path vs merge path { #fast-path-vs-merge-path }

`ParallelExecutor` has two internal paths:

- **Fast path**: when `auto_dict` is false, each chunk is exported as its own `RecordBatch` in parallel. No serial merge.
- **Merge path**: when `auto_dict` is enabled or schemas are inconsistent, chunk builders are merged sequentially before export. Peak RSS is higher. Compare filters do not force the merge path.

If you need both a `Compare` filter and maximum throughput, consider filtering after export in Python/Arrow instead.

## Summary { #summary }

- Use `dictionary_columns` for known low-cardinality strings; it is predictable and avoids heuristic cost.
- Use `auto_dict=True` only when cardinality is unknown and the file is small or not parallel.
- Remember that dictionaries force the parallel merge path; weigh memory savings against throughput loss.
