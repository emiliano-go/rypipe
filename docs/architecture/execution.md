# Execution: Pipeline, Parallel, Bounded, Input

`crates/rypipe-core/src/pipeline.rs`, `parallel.rs`, `bounded.rs`, `input.rs` plus `merge.rs` and `plan.rs` decide how bytes become batches.

## Pipeline

```rust
pub struct Pipeline<S, P> { splitter: S, parser: P, plan: Arc<ExecutionPlan> }
```

`S: Splitter + Clone` and `P: RecordParser + Clone` so the pipeline can be reused across files and modes.

* `new(splitter, parser) -> Self` with `ExecutionPlan::new()`
* `with_plan(plan) -> Self` replaces the plan (builder chain)
* `read_bytes(&self, bytes: &[u8]) -> Result<RecordBatch>` creates `TableBuilder::with_plan(bytes.len() / 512, plan)`, calls `validate` then `parse_chunk` on the whole slice, then `finish`. Single thread, single batch.
* `read_bytes_par(&self, bytes: &[u8], num_chunks: usize) -> Result<Vec<RecordBatch>>` delegates to `ParallelExecutor::parse(bytes, &splitter, parser.clone(), plan, num_chunks)` (no file IO)
* `read_bytes_stream(&self, bytes: &[u8], budget: MemoryBudget) -> Result<Vec<RecordBatch>>` delegates to `BoundedExecutor::new(budget).run_bytes(bytes, &splitter, parser.clone(), plan)`
* `read_path(&self, path: impl AsRef<Path>, use_mmap: bool, prefault: bool) -> Result<RecordBatch>` opens `InputBuffer::open(path, use_mmap, prefault)` and calls `read_bytes(input.as_slice())`
* `read_path_par(&self, path, num_chunks, use_mmap, prefault) -> Result<Vec<RecordBatch>>` opens and calls `ParallelExecutor::parse(input.as_slice(), ...)`
* `read_path_stream(&self, path, budget, prefault) -> Result<Vec<RecordBatch>>` delegates to `BoundedExecutor::run(path, &splitter, parser, plan, prefault)`

All six methods share the same `Splitter` plus `RecordParser` plus `ExecutionPlan`. Tests in `pipeline::tests` use a `LineSplitter` and `LineParser` that split on `\n` and parse `key=value` tokens.

## ParallelExecutor

`crates/rypipe-core/src/parallel.rs:16` `pub struct ParallelExecutor;` with one associated function:

```rust
pub fn parse<P>(bytes: &[u8], splitter: &dyn Splitter, parser: P, plan: Arc<ExecutionPlan>, num_chunks: usize) -> Result<Vec<RecordBatch>>
where P: RecordParser + Clone + Send + Sync
```

Steps:

1. `splitter.find_split_points(bytes, num_chunks)` then `split_points_to_ranges(&points, bytes.len())` to get `Vec<Range<usize>>`.

2. `into_par_iter` via `rayon` maps each `Range` to `catch_unwind(AssertUnwindSafe(|| { let mut sink = TableBuilder::with_plan(est, plan.clone()); parser.validate(&bytes[range])?; parser.parse_chunk(&bytes[range], &mut sink)?; Ok(sink) }))` where `est = (range.len() / 512).max(64)`. Panics are caught and turned into `Error::Merge("worker panicked during parallel parse: {msg}")` by downcasting `payload` to `&str` or `String`.

3. `collect::<Result<Vec<TableBuilder>>>()` joins. If `engines` is empty, return `Ok(vec![])`.

4. Fast path: `if !plan.auto_dict && schemas_consistent(&engines) { return engines_to_record_batches(engines, &plan) }`

   `schemas_consistent` builds `base: HashMap<&str, &str>` from `first.field_index` plus `first.columns[idx].variant_key()` and checks every other engine's `field_index` entries have the same `variant_key`. Missing columns are fine (null filled later). This allows `int64` plus `float64` to be considered inconsistent here (so merge path will promote), but `string` plus `dictionary` is also inconsistent and will promote in the fast path via `unify_variants` (not here). Actually `schemas_consistent` requires exact key equality, so `int64` vs `float64` fails and falls to merge path which also promotes; `string` vs `dictionary` also fails but fast path `engines_to_record_batches` handles promotion as well, so the fast path is still taken when `auto_dict` is false? Wait, `schemas_consistent` returning true requires exact match, so mixed `string`/`dictionary` would be false and go to merge path even though `engines_to_record_batches` could handle it. Current code does: fast path only if `!auto_dict && schemas_consistent`. That means `string` plus `dictionary` with `auto_dict` false but different variants will go to merge path (single batch) instead of fast path (multiple batches with unified schema). This is intentional to keep `engines_to_record_batches` as the unified schema path; the merge path also handles it but with single batch. The doc says fast path emits one batch per chunk with unified schema; merge path returns single merged batch. Both handle promotion, but fast path keeps chunked batches.

5. Merge path: `let mut merged = TableBuilder::with_plan(engines.len().max(64) * 512, plan.clone()); for engine in engines { merged.extend(engine)?; } let batch = merged.finish()?; if let Some(filter) = plan.filter { return Ok(vec![apply_compare_filter(batch, filter)?]) }` Note `apply_compare_filter` is only applied here for the merged single batch; fast path applies it per batch inside `engines_to_record_batches`.

All row filters (`Equal`, `NotEqual`, `Compare`, and `And`/`Or`/`Not` trees) are evaluated per row during `finish_row` in both paths, so they never force the merge path.

## BoundedExecutor

`crates/rypipe-core/src/bounded.rs:14` `MemoryBudget` is `bytes: usize` with `new` and `bytes()`.

`BoundedExecutor { budget: MemoryBudget }` has:

* `plan_chunks(&self, bytes: &[u8], splitter: &dyn Splitter) -> (Vec<Range<usize>>, usize, usize)` estimates `bytes_per_row = splitter.estimate_bytes_per_row(bytes).max(1)`, `total_rows_est = bytes.len() / bytes_per_row`, `rows_per_batch = (budget.bytes() / bytes_per_row).max(1).min(total_rows_est.max(1))`, `num_batches = (total_rows_est / rows_per_batch).max(1)`, `split_points = splitter.find_split_points(bytes, num_batches.min(MAX_SPLIT_CHUNKS))` where `MAX_SPLIT_CHUNKS = 100_000`, then `split_points_to_ranges`.

* `run_bytes<P>(&self, bytes: &[u8], splitter: &dyn Splitter, parser: P, plan: ExecutionPlan) -> Result<Vec<RecordBatch>>` where `P: RecordParser + Clone + Send + Sync`. For empty bytes returns `Ok(vec![])`. Otherwise it gets `(chunks, rows_per_batch, bytes_per_row)`, creates `batch_engine = TableBuilder::with_plan(bytes_per_row.max(64), plan)`, then for each `chunk` slices `&bytes[chunk.start..chunk.end]`, creates a per chunk `chunk_engine`, calls `validate` and `parse_chunk`, extends `batch_engine` via `extend`, tracks `rows_in_batch`, flushes when `rows_in_batch >= rows_per_batch` via `batch_engine.finish()` plus `reset`. At the end flushes remainder and calls `apply_plan_filter` (which applies `apply_compare_filter` only for pure `Compare` and `And` trees; other trees are no ops because per row is authoritative).

* `run<P>(&self, path: &Path, splitter: &dyn Splitter, parser: P, plan: Arc<ExecutionPlan>, prefault: bool) -> Result<Vec<RecordBatch>>` opens `InputBuffer::open(path, use_mmap = cfg(feature="mmap"), prefault)`. If the buffer is `Mmap`, it calls `run_mapped` which does `plan_chunks` on the mapped slice, drops the mapping, then reopens the file with `File::open` and for each `chunk` does `seek` plus `read_exact` into a fresh `Vec<u8>`, parses, and accumulates as above. This keeps RSS low for large files: the mapping is released before the parse loop, and only one chunk buffer is live at a time. If the buffer is `Owned` (including transparently decompressed), it delegates to `run_bytes(input.as_slice(), ...)`.

* `run_mapped` is `#[cfg(feature="mmap")]` and takes `input: InputBuffer` by value (so the mapping is dropped after `plan_chunks`). The file is reopened; chunk reads use `SeekFrom::Start(chunk.start)` plus `read_exact`.

`MAX_SPLIT_CHUNKS = 100_000` is the internal safeguard: never request more than 100,000 split points even if `budget` would imply more batches; pathological `bytes_per_row` cannot explode per chunk overhead. Batches may still exceed budget when the required count exceeds the cap (documented).

## InputBuffer

`crates/rypipe-core/src/input.rs:36` `enum InputBuffer { Mmap(MmapHandle), Owned(Vec<u8>) }` where `MmapHandle` wraps `memmap2::Mmap`.

* `MmapHandle::new(file, prefault)` maps the file and on Unix does `mmap.advise(WillNeed)` if `prefault` else `Sequential`.

* `detect_compression(path) -> Option<Compression>` reads the first 4 bytes and matches magic: `gzip` `1f 8b` (2 bytes), `zstd` `28 b5 2f fd`, `lz4` frame `04 22 4d 18`. Each arm is `#[cfg(feature = "gzip"/"zstd"/"lz4")]` so detection only fires when the feature is enabled. No extension check, only magic.

* `decompress(path, codec) -> Result<Vec<u8>>` opens the file again and wraps it in `flate2::read::GzDecoder`, `zstd::stream::read::Decoder`, or `lz4_flex::frame::FrameDecoder` depending on codec and feature, then `read_to_end`.

* `open(path: &Path, use_mmap: bool, prefault: bool) -> Result<Self>` first calls `detect_compression`; if `Some`, returns `Owned(decompress(...)?)` (so all execution modes operate on decompressed bytes). Otherwise, if `#[cfg(feature="mmap")]` and `use_mmap`, returns `Mmap`; else reads via `fs::read` into `Owned`.

* Cargo features: `gzip = ["dep:flate2"]`, `zstd = ["dep:zstd"]`, `lz4 = ["dep:lz4_flex"]`, `compress-all = ["gzip","zstd","lz4"]`, `mmap = ["dep:memmap2"]`. The `zstd` and `lz4` decoders are pure Rust when possible (`flate2` with `rust_backend`).

## Merge

`crates/rypipe-core/src/merge.rs:14` `impl TableBuilder { extend, }` plus `engines_to_record_batches`.

* `extend(&mut self, mut other: TableBuilder) -> Result<()>` merges `other` into `self`. Steps: (1) for each name in `other.column_order.clone()` where `!self.field_index.contains_key(name)`, create a builder `with_capacity(est, &col_type)` where `est = self_rows + other.estimated_rows.max(64)`, backfill `self_rows` nulls, push to `columns`, insert to `field_index`, push false to `row_dirty`, insert into `column_order` at `schema_insert_index`. (2) snapshot `order_snapshot = self.column_order.clone()`, then for each `name` in `order_snapshot` get `self_idx` via `field_index`, take `self_b = &mut columns[self_idx]`, try `other.take_column(name)`; if `Some`, check `variant_key` equality, call `unify_variants` if different (string plus dictionary to dictionary, int64 plus float64 to float64 else `Error::Merge` with column name and hint to provide `field_types`), then `promote_to_variant` on both, then `extend_owned`; else null pad `other_rows` times. Finally `row_count = self_rows + other_rows`.

* `engines_to_record_batches(mut engines: Vec<TableBuilder>, plan: &ExecutionPlan) -> Result<Vec<RecordBatch>>` exports per chunk builders without serial merge. It normalizes and retains `row_count > 0`, builds `order` plus `targets: HashMap<String, &'static str>` via `get_column` and `unify_variants` folding, promotes each builder's columns to the unified variant, builds `types: HashMap<String, DataType>` from first sighting `arrow_datatype`, creates `Schema`, then `par_iter` over engines to build `arrays` per `order` (via `get_column` or `null_array` for missing), `RecordBatch::try_new`, collects via `rayon`, then applies `apply_compare_filter` per batch if `plan.filter` is Some.

## Arrow export

`crates/rypipe-core/src/arrow_export.rs` `null_array`, `apply_compare_filter`, `compare_columns`, `is_numeric`.

* `apply_compare_filter(batch, predicate)` is only for pure `Compare` and `And` trees (checked via `is_pure_compare_tree`). Other trees return `Ok(batch)` unchanged because per row is authoritative. For pure trees, it builds a mask via `compare_mask` (recursing `And` with `and` kernel) and `compare_columns` (casts both to `Float64` if numeric else `Utf8`, then Arrow `gt`, `lt`, `gt_eq`, `lt_eq`, `eq`, `neq`), then `filter_record_batch`.

See also [Engine](./engine.md) for `TableBuilder::finish` and Arrow `to_arrow_array` details per `ColumnBuilder` variant.
