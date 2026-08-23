# Advanced rypipe deep-dive

This guide is for adapter authors and power users who want to squeeze more throughput or lower memory out of rypipe pipelines. It assumes you already know the Python and Rust API basics.

## How pushdown fusion works

`rypipe` splits ingestion into two layers:

1. **Adapter layer**: reads bytes, splits them into records, and emits `Value` rows.
2. **Engine layer**: builds typed Arrow arrays, applies filters, and exports record batches.

When you write a pipeline like this:

```python
source = MyAdapter("data.log")
result = (
    source
    | RenameFields({"old_name": "new_name"})
    | DropFields(["internal_id"])
    | FilterRows(field="status", op="==", value="active")
    | CastTypes({"amount": "float64"})
).to_arrow()
```

the Python `Pipeline` rewrites the stage list into a single `ExecutionPlan`. The engine then applies rename, drop, constant-filter, and cast *while it parses each row*. Rows that fail the filter are never materialized; dropped columns are never allocated; and casts happen once inside Rust instead of twice in Python and Rust.

Fusable stages are:

- `RenameFields`
- `DropFields`
- `CastTypes`
- `FilterRows` with a constant predicate (`field`, `op`, `value`)

Non-fusable stages (for example a Python callable or a stateful window) still work, but they run over the Arrow table after the engine finishes. The pipeline automatically falls back to a row stream when it cannot short-circuit to a table.

## Engine execution modes

Adapters can expose up to three execution strategies. Choose the one that matches your workload:

| Mode | Best for | Memory | Parallelism |
|------|----------|--------|-------------|
| `stream` | Huge files, unknown schema, row-at-a-time consumers | bounded by batch size | single-threaded parse |
| `columnar` | Medium files that fit in RAM, table output | holds full table | single-threaded parse, vectorized builders |
| `parallel` | Large files that fit in RAM, many cores | holds full table | chunked multi-threaded parse |

`auto` should let the adapter pick. A common heuristic is: files under ~8 MiB use columnar; larger files use parallel when memory allows; otherwise stream. Adapters should document their own heuristic because format split boundaries affect chunk safety.

## Memory budgets and chunking

The engine respects a memory budget for bounded/streaming paths. Two knobs control it:

- `memory`: maximum bytes the parser should hold in flight. A string like `"512MiB"` is parsed into bytes.
- `chunks`: number of chunks for parallel mode. More chunks improve load balancing but increase rayon overhead.

Rule of thumb for parallel mode:

```
chunks = 4 * physical_cores
```

Finer chunks even out variable record parse times. Beyond 4-8x core count, synchronization overhead usually wins. Measure with your data; text-heavy formats benefit from fewer chunks because per-chunk setup dominates.

## Schema hints

Providing a `schema` list avoids the discovery pass that some formats need to infer column names. It also stabilizes column order across chunks, which matters for parallel merges. Pass field types with `field_types` so the engine builds the correct Arrow array from the first row instead of inferring and recasting later.

```python
source = MyAdapter(
    "data.log",
    schema=["id", "ts", "amount"],
    field_types={"id": "int64", "ts": "string", "amount": "float64"},
    dictionary_columns=["status"],
)
```

## Dictionary encoding

Set `dictionary_columns` explicitly for low-cardinality string columns. This stores values as integer indices and can reduce memory 5-20x for status codes, country codes, or enums. `auto_dict=True` asks the engine to guess, but the heuristic has a small runtime cost; explicit is faster and predictable.

## mmap vs buffered reads

`use_mmap=True` (the default) maps the file into virtual memory. It is usually fastest for files that fit in RAM or when the OS can cache the file. For cold files larger than RAM, buffered reads may give smoother throughput because the parser avoids page-fault stalls. Benchmark both if your workload is I/O bound.

## Designing a fast adapter

A high-performance adapter does as little work as possible per record:

1. **Split cheaply**: find record boundaries with byte scans; defer full decoding.
2. **Borrow strings**: hand borrowed `&str` slices to the engine when the input is valid UTF-8. The engine copies only when it must cast or encode.
3. **Emit sparse rows**: if a field is missing, skip it entirely instead of emitting `Value::Null` for every row.
4. **Respect plan kwargs**: pass `field_mapping`, `drop_fields`, `filter`, and `field_types` into the Rust parser so fusion actually happens.
5. **Avoid Python in the hot path**: build the Arrow table in Rust and export it once via the C Data Interface.

## Profiling a pipeline

Use the throughput benchmark in `crates/rypipe-core/examples/bench_throughput.rs` as a template. General workflow:

1. Run a release build.
2. Vary `chunks`, `memory`, and `batch_size` and plot throughput vs latency.
3. Use `perf` or `cargo flamegraph` to see whether time is in splitting, decoding, Arrow builders, or the GIL boundary.
4. If Python sinks dominate, sink directly to Arrow/Parquet instead of iterating rows.

## Anti-patterns

- Iterating a pipeline with `for row in pipeline` after fusing a table-shaped source. It works, but it reconstructs dicts from the Arrow table.
- Chaining many non-fusable Python callables. Each one materializes a full batch list.
- Calling `to_pandas()` and then `to_arrow()` repeatedly. Cache the table once and reuse it.
- Ignoring `plan_overrides` in `_read_arrow`. If you ignore them, stages silently fall back to Python execution.

## Summary checklist

- [ ] Provide `schema` and `field_types` when known.
- [ ] Use `dictionary_columns` for low-cardinality strings.
- [ ] Prefer `columnar` for tables that fit in RAM, `parallel` for large cached files, and `stream` for huge or row-oriented consumers.
- [ ] Tune `chunks = 4 * cores` and measure.
- [ ] Implement `_read_arrow(plan_overrides=...)` to keep stages fused.
- [ ] Export Arrow from Rust and sink directly to Parquet when possible.
