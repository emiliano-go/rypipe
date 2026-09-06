# Execution modes { #execution-modes }

`rypipe` adapters can expose up to four execution strategies. Choosing the right one is the biggest single decision for memory and throughput.

| Mode | Memory | Parallelism | Speed (1 GB, 5800X) | Best for |
|------|--------|-------------|---------------------|----------|
| `columnar` | Full table in RAM | Single | 714 MB/s | <100 MB, single `Table`, `auto_dict` |
| `parallel` | Full table in RAM | Multi-core | ~3 GB/s | >100 MB, fits RAM, max speed |
| `bounded` (streaming) | Budget + one batch | Single | 625 MB/s 64KB | Huge files, 64KB-256 MB budget |
| `parallel streaming` | Budget + small in-flight set | Multi-core | 1.5-2.5 GB/s (256 MB) | Huge files, ≥256 MB budget, speed + bounded |

`auto` lets the adapter pick. A common heuristic is: files under ~8 MiB use columnar, larger files use parallel when memory allows and budget is large, otherwise bounded/parallel streaming. Adapters should document their own heuristic because format split boundaries affect chunk safety.

## Stream mode { #stream-mode }

Stream mode uses `BoundedExecutor`. It keeps a memory budget and parses the file in batches:

1. Opens the file via `InputBuffer`.
2. Estimates `bytes_per_row` from the splitter.
3. Computes `rows_per_batch` from the budget.
4. Splits the file into batches sized to fit the memory budget, capped at 100,000
   split points as an internal safeguard against pathological chunk counts.
5. Parses each batch into a `TableBuilder`, exports it to a `RecordBatch`, and resets the builder.
6. Returns a `Vec<RecordBatch>`; the caller concatenates or iterates.

Because the input buffer is dropped before the parse phase begins for bounded mode, mmap-backed pages are released before downstream work starts. This keeps peak memory close to the budget even for files much larger than RAM.

Use stream mode when:

- the file does not fit in RAM;
- the consumer is row-oriented or streaming (e.g., writing one row at a time);
- latency per batch matters more than total throughput;
- parallel merge overhead would dominate (very simple parsers).

## Columnar mode { #columnar-mode }

Columnar mode parses the whole file in one thread and builds one `TableBuilder`. It is the simplest path and avoids all chunking and synchronization overhead. The full table stays in memory until export.

Use columnar mode when:

- the file fits comfortably in RAM;
- the parser is fast enough that parallel overhead would not pay off;
- you need one contiguous `RecordBatch` without a merge step;
- `auto_dict` or compare filters force a merge anyway, so parallelism adds overhead.

Columnar mode is often fastest for small files because there is no per-chunk setup and no rayon scheduling.

## Parallel mode { #parallel-mode }

Parallel mode uses `ParallelExecutor`:

1. Calls `Splitter::find_split_points`.
2. Converts points to non-empty `Range<usize>` chunks.
3. Uses `rayon::par_iter` to parse each chunk independently into a `TableBuilder`.
4. Fast path: if `auto_dict` is false and schemas are consistent, each builder is exported as its own `RecordBatch` in parallel. No serial merge happens. Compare filters are applied per-row during parse AND re-applied post-export, so they do not force the merge path.
5. Merge path: if `auto_dict` is enabled or schemas are inconsistent, chunk builders are merged sequentially before export.

Use parallel mode when:

- the file fits in RAM or in the OS page cache;
- the parser is CPU-bound (heavy XML, complex field extraction, many columns);
- you can tolerate higher peak memory for shorter wall-clock time;
- `auto_dict` and compare filters are off, so the fast path applies.

## Parallel streaming mode { #parallel-streaming-mode }

Parallel streaming (`ParallelStreamingExecutor` `crates/rypipe-core/src/parallel_stream.rs`) parses chunks concurrently with bounded memory:

1. `chunk_size = budget / (threads * 2)` (e.g. 256 MB / 16 threads → 8 MB/chunk, 2 chunks/thread → 256 MB peak).
2. `Splitter::find_split_points` creates `num_chunks` ranges.
3. Worker pool (`std::thread` or `rayon` `ThreadPool`) pulls `Range` + `seq` from a queue, parses into `TableBuilder`, sends `(seq, Result<RecordBatch>)` via `sync_channel(max_in_flight)` (backpressure).
4. Coordinator orders by `seq` via `BTreeMap` pending and delivers to `BatchConsumer` in file order.

### In-flight management { #in-flight-management }

The key to bounded memory is the **in-flight cap**: at most `threads × 2` chunks are being parsed concurrently. This works as follows:

```
Main thread                Worker pool              Coordinator
──────────                ───────────              ───────────
                           ┌─ thread 1 ──┐
submit chunk 0 ──────────►│  parse chunk │──► channel ──┐
submit chunk 1 ──────────►│  parse chunk │──► channel    │
                           └──────────────┘              │
                           ┌─ thread 2 ──┐              │
submit chunk 2 ──────────►│  parse chunk │──► channel    ├──► BTreeMap
submit chunk 3 ──────────►│  parse chunk │──► channel    │    pending
                           └──────────────┘              │    (ordered
                                                         │     by seq)
                           ... (max 2 chunks/thread) ... │
                                                         │
                                          deliver in order ◄── file_order
                                          to BatchConsumer
```

- **Backpressure**: the `sync_channel(max_in_flight)` blocks the main thread when the channel is full, preventing more than `threads × 2` chunks from being in flight at once.
- **Ordering**: results arrive out of order (faster chunks finish first). The coordinator uses a `BTreeMap<u64, Result<RecordBatch>>` keyed by sequence number to deliver batches in file order.
- **Memory**: each chunk's `TableBuilder` is dropped after its `RecordBatch` is sent. Peak memory is `budget + threads × 2 × chunk_size`, which stays near the configured budget.
- **Error handling**: if any chunk panics or returns an error, the entire stream is aborted. `catch_unwind` at the worker level prevents a single bad chunk from crashing the process.

Use parallel streaming when:

- the file is large and you want bounded memory, but have ≥256 MB budget and multiple cores;
- you need higher throughput than sequential streaming (`bounded` 625 MB/s) while avoiding full-table materialization (`parallel` 3 GB/s needs full RAM);
- schema is fixed (first chunk defines schema, later chunks `unify_variants` or `Error::Merge`).

Example:

```python
for batch in rypipe.iter_record_batches_parallel("huge.xml", memory="256MB", threads=8, format="crxml"):
    writer.write_batch(batch)
```

Small budgets (`64KB`) are faster single-threaded; use `parallel streaming` for `≥256MB`.

## Auto engine selection { #auto-engine-selection }

rypipe provides a `resolve_engine` function that adapters can use for
`engine="auto"` selection. It considers file size, memory budget, threads,
and schema to pick the optimal mode:

```python
import rypipe

engine = rypipe.resolve_engine(
    file_size=1_000_000_000,      # 1 GB
    memory="64MiB",               # bounded memory
    threads=16,                   # parallel
    schema=["col1", "col2"],      # explicit schema
    has_parallel=True,            # adapter supports parallel
    has_columnar=True,            # adapter supports columnar
)
# Returns: "parallel_streaming"
```

### Heuristic rules { #heuristic-rules }

The algorithm follows these rules in order:

1. **`memory=` provided**: User wants streaming.
   - If `threads > 1`: return `"parallel_streaming"`
   - Else: return `"stream"`

2. **`threads > 1`**: User wants parallel.
   - If file >= 100 MB: return `"parallel_streaming"` (bounded memory)
   - Else: return `"parallel"` (fits in RAM)

3. **`schema=` provided and file >= 100 MB**: Streaming is 11% faster
   (no discovery overhead).
   - Return `"stream"`

4. **Default**:
   - If file < 8 MB and `has_columnar`: return `"columnar"`
   - If file >= 8 MB and `has_parallel`: return `"parallel"`
   - Else: return `"stream"`

### Adapters using resolve_engine { #adapters-using-resolve-engine }

```python
class MySource(Source):
    def __init__(self, path, *, engine="auto", **kwargs):
        self._engine = engine
        self._engine_resolved = None
        super().__init__(path, **kwargs)
    
    def _resolve_engine(self, goal: str) -> str:
        if self._engine != "auto":
            return self._engine
        
        if self._engine_resolved is not None:
            return self._engine_resolved
        
        import rypipe
        self._engine_resolved = rypipe.resolve_engine(
            file_size=self._path.stat().st_size,
            memory=self._memory,
            threads=self._threads,
            schema=self._schema or None,
            has_parallel=_HAS_PARALLEL,
            has_columnar=_HAS_COLUMNAR,
        )
        return self._engine_resolved
```

Adapters should expose the engine choice explicitly because the best default
depends on split safety, row size variance, and downstream use. A format with
expensive per-chunk setup (for example, one that must scan for a global header)
may prefer columnar for much larger files than a simple newline-delimited format.

## Trade-offs { #trade-offs }

| Concern | Prefer | Avoid | Why |
|---------|--------|-------|-----|
| Lowest memory | stream | parallel | Bounded batches keep peak RSS flat. |
| Lowest latency to first batch | stream | parallel | First batch is emitted before the whole file is read. |
| Highest throughput on large files | parallel | columnar | Many cores parse simultaneously. |
| Highest throughput on small files | columnar | parallel | Chunk overhead dominates. |
| Deterministic column order | any with `schema_order` | inference | Chunk merges rely on a common schema. |
| Low cardinality string compression | columnar or parallel with `dictionary_columns` | parallel with `auto_dict` | Auto-dict forces the merge path. |

## GIL behavior { #gil-behavior }

All parse paths release the GIL during the heavy Rust work. The Arrow C Data Interface export re-acquires the GIL briefly. For `read_path_par`, the entire parallel parse runs outside the GIL.

This means parallel mode can saturate CPU from Python without `multiprocessing`, provided the adapter is implemented in Rust and exports Arrow.

## Summary { #summary }

- Use `stream` for huge files or row consumers.
- Use `columnar` for small-to-medium files and when merge is unavoidable.
- Use `parallel` for large cached files with a CPU-bound parser and no merge-forcing options.
- Expose `engine` explicitly and document the adapter-specific heuristic.
