# Execution modes

`rypipe` adapters can expose up to three execution strategies. Choosing the right one is usually the biggest single decision for memory and throughput.

| Mode | Best for | Memory | Parallelism | Output |
|------|----------|--------|-------------|--------|
| `stream` | Huge files, unknown schema, row-at-a-time consumers | bounded by batch size | single-threaded parse | iterator / batched record batches |
| `columnar` | Medium files that fit in RAM, table output | holds full table | single-threaded parse, vectorized builders | one `RecordBatch` |
| `parallel` | Large files that fit in RAM, many cores | holds full table | chunked multi-threaded parse | one `RecordBatch` or `Vec<RecordBatch>` |

`auto` lets the adapter pick. A common heuristic is: files under ~8 MiB use columnar; larger files use parallel when memory allows; otherwise stream. Adapters should document their own heuristic because format split boundaries affect chunk safety.

## Stream mode

Stream mode uses `BoundedExecutor`. It keeps a memory budget and parses the file in batches:

1. Opens the file via `InputBuffer`.
2. Estimates `bytes_per_row` from the splitter.
3. Computes `rows_per_batch` from the budget.
4. Splits the file into batches sized to fit the memory budget, capped at 256
   split points as an internal safeguard against pathological chunk counts.
5. Parses each batch into a `TableBuilder`, exports it to a `RecordBatch`, and resets the builder.
6. Returns a `Vec<RecordBatch>`; the caller concatenates or iterates.

Because the input buffer is dropped before the parse phase begins for bounded mode, mmap-backed pages are released before downstream work starts. This keeps peak memory close to the budget even for files much larger than RAM.

Use stream mode when:

- the file does not fit in RAM;
- the consumer is row-oriented or streaming (e.g., writing one row at a time);
- latency per batch matters more than total throughput;
- parallel merge overhead would dominate (very simple parsers).

## Columnar mode

Columnar mode parses the whole file in one thread and builds one `TableBuilder`. It is the simplest path and avoids all chunking and synchronization overhead. The full table stays in memory until export.

Use columnar mode when:

- the file fits comfortably in RAM;
- the parser is fast enough that parallel overhead would not pay off;
- you need one contiguous `RecordBatch` without a merge step;
- `auto_dict` or compare filters force a merge anyway, so parallelism adds overhead.

Columnar mode is often fastest for small files because there is no per-chunk setup and no rayon scheduling.

## Parallel mode

Parallel mode uses `ParallelExecutor`:

1. Calls `Splitter::find_split_points`.
2. Converts points to non-empty `Range<usize>` chunks.
3. Uses `rayon::par_iter` to parse each chunk independently into a `TableBuilder`.
4. Fast path: if `auto_dict` is false and there is no `Compare` filter, each builder is exported as its own `RecordBatch` in parallel. No serial merge happens.
5. Merge path: if `auto_dict` or a `Compare` filter is present, chunk builders are merged sequentially before export.

Use parallel mode when:

- the file fits in RAM or in the OS page cache;
- the parser is CPU-bound (heavy XML, complex field extraction, many columns);
- you can tolerate higher peak memory for shorter wall-clock time;
- `auto_dict` and compare filters are off, so the fast path applies.

## Auto engine selection

The Python `Adapter` layer usually exposes `engine="auto"`. The heuristic is format-specific, but a common default is:

```python
if file_size < 8 * 1024 * 1024:
    engine = "columnar"
elif memory_available > 4 * file_size:
    engine = "parallel"
else:
    engine = "stream"
```

Adapters should expose the engine choice explicitly because the best default depends on split safety, row size variance, and downstream use. A format with expensive per-chunk setup (for example, one that must scan for a global header) may prefer columnar for much larger files than a simple newline-delimited format.

## Trade-offs

| Concern | Prefer | Avoid | Why |
|---------|--------|-------|-----|
| Lowest memory | stream | parallel | Bounded batches keep peak RSS flat. |
| Lowest latency to first batch | stream | parallel | First batch is emitted before the whole file is read. |
| Highest throughput on large files | parallel | columnar | Many cores parse simultaneously. |
| Highest throughput on small files | columnar | parallel | Chunk overhead dominates. |
| Deterministic column order | any with `schema_order` | inference | Chunk merges rely on a common schema. |
| Low cardinality string compression | columnar or parallel with `dictionary_columns` | parallel with `auto_dict` | Auto-dict forces the merge path. |

## GIL behavior

All parse paths release the GIL during the heavy Rust work. The Arrow C Data Interface export re-acquires the GIL briefly. For `read_path_par`, the entire parallel parse runs outside the GIL.

This means parallel mode can saturate CPU from Python without `multiprocessing`, provided the adapter is implemented in Rust and exports Arrow.

## Summary

- Use `stream` for huge files or row consumers.
- Use `columnar` for small-to-medium files and when merge is unavoidable.
- Use `parallel` for large cached files with a CPU-bound parser and no merge-forcing options.
- Expose `engine` explicitly and document the adapter-specific heuristic.
