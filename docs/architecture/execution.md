# Execution: Pipeline, Parallel, Bounded, Input { #execution-pipeline-parallel-bounded-input }

This page covers how bytes become batches. The same `Splitter` plus
`RecordParser` plus `ExecutionPlan` are shared across all modes; only the
driver differs.

See [Data flow](./data-flow.md) for diagrams of each mode.

## Pipeline { #pipeline }

```rust
pub struct Pipeline<S, P> {
    splitter: S,
    parser: P,
    plan: Arc<ExecutionPlan>,
}
```

`S: Splitter + Clone` and `P: RecordParser + Clone` so the pipeline can be
reused across files and modes.

### Methods { #methods }

- `new(splitter, parser)`: Creates with default plan.
- `with_plan(plan)`: Replaces the plan (builder pattern).
- `read_bytes(bytes)`: Single-threaded: one `TableBuilder`, one `parse_chunk_generic`.
- `read_bytes_par(bytes, num_chunks)`: Parallel via `ParallelExecutor`.
- `read_bytes_stream(bytes, budget)`: Bounded-memory via `BoundedExecutor`.
- `read_bytes_stream_consumer(bytes, budget, consumer)`: Bounded, calls `consumer` per batch.
- `read_path(path, use_mmap, prefault)`: Opens file, calls `read_bytes`.
- `read_path_par(path, num_chunks, use_mmap, prefault)`: Opens file, calls parallel.
- `read_path_stream(path, budget, prefault)`: Opens file, calls bounded.
- `read_path_stream_consumer(path, budget, prefault, consumer)`: Opens file, calls bounded streaming.
- `read_path_stream_par(path, budget, prefault, opts)`: Parallel streaming; returns a
  `ParallelStreamingBatchIterator` yielding batches as they are produced.

All methods share the same splitter, parser, and plan. The adapter
implements `Splitter` and `RecordParser` once; the engine handles the rest.

## ParallelExecutor { #parallelexecutor }

```rust
pub fn parse<P>(
    bytes: &[u8],
    splitter: &dyn Splitter,
    parser: P,
    plan: Arc<ExecutionPlan>,
    num_chunks: usize,
) -> Result<Vec<RecordBatch>>
where P: RecordParser + Clone + Send + Sync
```

### Steps { #steps }

1. **Split**: `splitter.find_split_points(bytes, num_chunks)` → `split_points_to_ranges`
2. **Parse in parallel**: `rayon::into_par_iter` over ranges, each creating a
   `TableBuilder`, calling `validate` + `parse_chunk_generic`, returning the
   builder. Panics are caught via `catch_unwind`.
3. **Fast path** (schemas consistent): `engines_to_record_batches` exports
   per-chunk batches with unified schema.
4. **auto_dict upgrade** (if `auto_dict` set): per-chunk `auto_dict_upgrade()`
   in parallel, then dictionaries are unified across chunks
   (`dict::unify_dictionaries` + `remap_codes`/`replace_dict`) and the fast
   path is kept when schemas are consistent afterward.
5. **Merge path** (schemas still inconsistent):
   Sequential `extend` loop → single merged batch.

### Fast path vs merge path { #fast-path-vs-merge-path }

The fast path keeps one batch per chunk (chunked columns, no copy). It
unifies schema via `unify_variants` and `promote_to_variant` so all batches
share one `Schema`. Missing columns become `null_array`. `rayon::par_iter`
builds arrays in parallel.

The merge path (`extend` loop) returns a single merged batch and surfaces
irreconcilable type errors with `Error::Merge` naming the column. It is
reached only when schemas remain inconsistent (after any `auto_dict`
upgrade, which otherwise stays on the fast path).

### schemas_consistent { #schemas_consistent }

Checks that all engines agree on column variant keys. Mixed `int64`/`float64`
or `string`/`dictionary` falls to merge path for promotion.

## BoundedExecutor { #boundedexecutor }

```rust
pub struct BoundedExecutor {
    budget: MemoryBudget,
    split_cap: usize,
}
```

`BoundedExecutor::new(budget)` defaults `split_cap` to `MAX_SPLIT_CHUNKS`;
`with_split_cap(cap)` overrides it. `Pipeline` sets the cap from
`plan.max_split_chunks` when that plan field is set.

### MemoryBudget { #memorybudget }

```rust
pub struct MemoryBudget { bytes: usize }
impl MemoryBudget {
    pub fn new(bytes: usize) -> Self { Self { bytes } }
    pub fn bytes(&self) -> usize { self.bytes }
}
```

### plan_chunks { #plan_chunks }

Estimates chunk sizes from budget:

1. `bytes_per_row = splitter.estimate_bytes_per_row(bytes).max(1)`
2. `total_rows_est = bytes.len() / bytes_per_row`
3. `rows_per_batch = (budget.bytes() / bytes_per_row).max(1).min(total_rows_est)`
4. `num_batches = (total_rows_est / rows_per_batch).max(1)`
5. `split_points = splitter.find_split_points(bytes, num_batches.min(self.split_cap))`
6. Convert to ranges

`split_cap` (default `MAX_SPLIT_CHUNKS = 100_000`, overridable via
`plan.max_split_chunks`) caps split points to prevent pathological overhead.

### run_bytes { #run_bytes }

`run_bytes` is a thin wrapper over `run_bytes_stream` with a
`CollectingConsumer`. The streaming loop, for each chunk:

1. Slice `&bytes[chunk.start..chunk.end]`
2. Create per-chunk `TableBuilder`
3. `validate` + `parse_chunk_generic`
4. `extend` into batch engine
5. Flush when `rows_in_batch >= rows_per_batch` or
   `batch_engine.bytes_used() >= budget.bytes()`; the batch is cut via
   `split_off(n)`, with `n` shrunk by a byte-based estimate when the byte
   budget is the trigger
6. Apply `apply_compare_filter` to each flushed batch if `plan.filter` is set
7. After the loop, surface deferred strict-types and unknown-field errors
   (unknown-field as `Error::Merge`) accumulated across chunks

### run (file-based) { #run }

Opens `InputBuffer`. If `Mmap`:

1. `plan_chunks` on the mapped slice
2. Drop the mapping
3. Reopen file, `seek` + `read_exact` per chunk
4. Parse and accumulate

This keeps RSS low: mapping released before parse loop, only one chunk
buffer live at a time.

If `Owned` (compressed input, or the `mmap` feature disabled): delegates to
`run_bytes_stream`. There is no size-based choice; `use_mmap` is set from the
`mmap` feature flag alone.

## InputBuffer { #inputbuffer }

```rust
enum InputBuffer {
    Mmap(MmapHandle),
    Owned(Vec<u8>),
}
```

### MmapHandle { #mmaphandle }

Maps the file. On Unix, does `mmap.advise(WillNeed)` if prefault,
else `Sequential`.

### Compression detection { #compression-detection }

Reads first 4 bytes, matches magic:

- `1f 8b` → gzip (feature `gzip`)
- `28 b5 2f fd` → zstd (feature `zstd`)
- `04 22 4d 18` → lz4 frame (feature `lz4`)

If detected: `Owned(decompress(path, codec)?)` (read to end).
Decompressed bytes are served from memory for all modes.

### open { #open }

```
open(path, use_mmap, prefault):
  detect_compression(path)
  → Some(compressed) → Owned(decompress)
  → None + mmap enabled + use_mmap → Mmap
  → None → Owned(fs::read)
```

### Cargo features { #cargo-features }

- `gzip = ["dep:flate2"]`
- `zstd = ["dep:zstd"]`
- `lz4 = ["dep:lz4_flex"]`
- `compress-all = ["gzip", "zstd", "lz4"]`
- `mmap = ["dep:memmap2"]`

## Merge { #merge }

### extend { #extend }

Merges another `TableBuilder` into self:

1. Propagate a deferred `strict_error` from the other builder, and carry over
   the observer counters `rows_accepted`/`rows_rejected` (per-row observer
   hooks already fired in the chunk's thread)
2. Create missing columns with null backfill
3. For each column in order: check variant equality, promote if needed
   (`int64` → `float64`, `string` → `dictionary`), then `extend_owned`
4. Update `row_count`

### Row observers { #row-observers }

`ExecutionPlan` carries an optional `observer: Arc<dyn RowObserver>` (set via
`with_observer`). The hooks `on_begin_row`, `on_put_field`, `on_row_accepted`,
`on_row_rejected`, and `on_chunk_finished` fire during parse in every driver
above. In parallel mode the per-row hooks fire on the chunk's worker thread;
`extend` then merges the accepted/rejected totals so the final
`on_chunk_finished` sees the full counts.

### engines_to_record_batches { #engines_to_record_batches }

Exports per-chunk builders without serial merge:

1. Surface deferred per-chunk errors first: `unknown_error` as `Error::Merge`,
   then `strict_error`; with `strict_types` this is the fast path's only error
   surface
2. Normalize and retain non-empty builders
3. Unify schema via `unify_variants` + `promote_to_variant`
4. `par_iter` over engines to build arrays per unified order
5. Apply `apply_compare_filter` per batch if filter is present

## Arrow export { #arrow-export }

`apply_compare_filter` re-applies pure `Compare` and `And` trees using
Arrow compute kernels. Other filter trees are returned unchanged because
per-row evaluation is authoritative.

The filter works by:

1. Checking `is_pure_compare_tree` (no Or/Not/Equal/NotEqual)
2. Building a boolean mask via `compare_columns` (cast to Float64 or Utf8,
   then gt/lt/gt_eq/lt_eq/eq/neq); `And` trees are combined in `compare_mask`
3. Applying `filter_record_batch` to produce the filtered batch

See [Storage and export](./storage.md) for Arrow type mapping and null
handling details. See [Engine](./engine.md) for `TableBuilder::finish`
and the zero-copy Arrow export path.
