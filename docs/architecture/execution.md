# Execution: Pipeline, Parallel, Bounded, Input

This page covers how bytes become batches. The same `Splitter` plus
`RecordParser` plus `ExecutionPlan` are shared across all modes; only the
driver differs.

See [Data flow](./data-flow.md) for diagrams of each mode.

## Pipeline

```rust
pub struct Pipeline<S, P> {
    splitter: S,
    parser: P,
    plan: Arc<ExecutionPlan>,
}
```

`S: Splitter + Clone` and `P: RecordParser + Clone` so the pipeline can be
reused across files and modes.

### Methods

- `new(splitter, parser)` — Creates with default plan.
- `with_plan(plan)` — Replaces the plan (builder pattern).
- `read_bytes(bytes)` — Single-threaded: one `TableBuilder`, one `parse_chunk`.
- `read_bytes_par(bytes, num_chunks)` — Parallel via `ParallelExecutor`.
- `read_bytes_stream(bytes, budget)` — Bounded-memory via `BoundedExecutor`.
- `read_path(path, use_mmap, prefault)` — Opens file, calls `read_bytes`.
- `read_path_par(path, num_chunks, use_mmap, prefault)` — Opens file, calls parallel.
- `read_path_stream(path, budget, prefault)` — Opens file, calls bounded.

All six methods share the same splitter, parser, and plan. The adapter
implements `Splitter` and `RecordParser` once; the engine handles the rest.

## ParallelExecutor

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

### Steps

1. **Split**: `splitter.find_split_points(bytes, num_chunks)` → `split_points_to_ranges`
2. **Parse in parallel**: `rayon::into_par_iter` over ranges, each creating a
   `TableBuilder`, calling `validate` + `parse_chunk`, returning the builder.
   Panics are caught via `catch_unwind`.
3. **Fast path** (no auto_dict, schemas consistent):
   `engines_to_record_batches` exports per-chunk batches with unified schema.
4. **Merge path** (auto_dict or inconsistent schemas):
   Sequential `extend` loop → single merged batch.

### Fast path vs merge path

The fast path keeps one batch per chunk (chunked columns, no copy). It
unifies schema via `unify_variants` and `promote_to_variant` so all batches
share one `Schema`. Missing columns become `null_array`. `rayon::par_iter`
builds arrays in parallel.

The merge path (`extend` loop) returns a single merged batch and handles
`auto_dict` visibility (full cardinality) and irreconcilable type errors
with `Error::Merge` naming the column.

### schemas_consistent

Checks that all engines agree on column variant keys. Mixed `int64`/`float64`
or `string`/`dictionary` falls to merge path for promotion.

## BoundedExecutor

```rust
pub struct BoundedExecutor {
    budget: MemoryBudget,
}
```

### MemoryBudget

```rust
pub struct MemoryBudget { bytes: usize }
impl MemoryBudget {
    pub fn new(bytes: usize) -> Self { Self { bytes } }
    pub fn bytes(&self) -> usize { self.bytes }
}
```

### plan_chunks

Estimates chunk sizes from budget:
1. `bytes_per_row = splitter.estimate_bytes_per_row(bytes).max(1)`
2. `total_rows_est = bytes.len() / bytes_per_row`
3. `rows_per_batch = (budget.bytes() / bytes_per_row).max(1).min(total_rows_est)`
4. `num_batches = (total_rows_est / rows_per_batch).max(1)`
5. `split_points = splitter.find_split_points(bytes, num_batches.min(MAX_SPLIT_CHUNKS))`
6. Convert to ranges

`MAX_SPLIT_CHUNKS = 100_000` caps split points to prevent pathological overhead.

### run_bytes

For each chunk:
1. Slice `&bytes[chunk.start..chunk.end]`
2. Create per-chunk `TableBuilder`
3. `validate` + `parse_chunk`
4. `extend` into batch engine
5. Flush when `rows_in_batch >= rows_per_batch`

### run (file-based)

Opens `InputBuffer`. If `Mmap`:
1. `plan_chunks` on the mapped slice
2. Drop the mapping
3. Reopen file, `seek` + `read_exact` per chunk
4. Parse and accumulate

This keeps RSS low: mapping released before parse loop, only one chunk
buffer live at a time.

If `Owned` (compressed or small file): delegates to `run_bytes`.

## InputBuffer

```rust
enum InputBuffer {
    Mmap(MmapHandle),
    Owned(Vec<u8>),
}
```

### MmapHandle

Maps the file. On Unix, does `mmap.advise(WillNeed)` if prefault,
else `Sequential`.

### Compression detection

Reads first 4 bytes, matches magic:
- `1f 8b` → gzip (feature `gzip`)
- `28 b5 2f fd` → zstd (feature `zstd`)
- `04 22 4d 18` → lz4 frame (feature `lz4`)

If detected: `Owned(decompress(path, codec)?)` (read to end).
Decompressed bytes are served from memory for all modes.

### open

```
open(path, use_mmap, prefault):
  detect_compression(path)
  → Some(compressed) → Owned(decompress)
  → None + mmap enabled + use_mmap → Mmap
  → None → Owned(fs::read)
```

### Cargo features

- `gzip = ["dep:flate2"]`
- `zstd = ["dep:zstd"]`
- `lz4 = ["dep:lz4_flex"]`
- `compress-all = ["gzip", "zstd", "lz4"]`
- `mmap = ["dep:memmap2"]`

## Merge

### extend

Merges another `TableBuilder` into self:
1. Create missing columns with null backfill
2. For each column in order: check variant equality, promote if needed
   (`int64` → `float64`, `string` → `dictionary`), then `extend_owned`
3. Update `row_count`

### engines_to_record_batches

Exports per-chunk builders without serial merge:
1. Normalize and retain non-empty builders
2. Unify schema via `unify_variants` + `promote_to_variant`
3. `par_iter` over engines to build arrays per unified order
4. Apply `apply_compare_filter` per batch if filter is present

## Arrow export

`apply_compare_filter` re-applies pure `Compare` and `And` trees using
Arrow compute kernels. Other filter trees are returned unchanged because
per-row evaluation is authoritative.

The filter works by:
1. Checking `is_pure_compare_tree` (no Or/Not/Equal/NotEqual)
2. Building a boolean mask via `compare_columns` (cast to Float64 or Utf8,
   then gt/lt/eq/neq/and)
3. Applying `filter_record_batch` to produce the filtered batch

See [Storage and export](./storage.md) for Arrow type mapping and null
handling details. See [Engine](./engine.md) for `TableBuilder::finish`
and the zero-copy Arrow export path.
