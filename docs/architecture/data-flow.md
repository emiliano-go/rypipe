# Data flow

This page shows how bytes move through the system in each execution mode. All modes share the same `Splitter` plus `RecordParser` plus `ExecutionPlan`; only the driver differs.

Legend for every diagram on this page: `(ADAPTER BOUND)` is code you write in the adapter crate (format specific). `(CORE)` is code in `rypipe` crates (format agnostic, reused).

## Single thread

```
(CORE) Pipeline::read_bytes(bytes)  or  Pipeline::read_path(path) -> InputBuffer::open -> as_slice
    |
    v
(CORE) TableBuilder::with_plan(cap, plan)     cap = bytes.len() / 512  (min 64)
    |
    v
(ADAPTER BOUND) parser.validate(bytes)?                 (CORE) simdutf8 check, called once per chunk; adapter decides what to validate
(ADAPTER BOUND) parser.parse_chunk(bytes, &mut sink)    (CORE) sink is TableBuilder (ColumnarSink)
         loop lines -> (CORE) begin_row (no op) -> (ADAPTER) put_field(s) -> (CORE) push_field_resolved plus dirty -> (CORE) end_row -> (CORE) finish_row per row
    |
    v
(CORE) TableBuilder::finish() -> RecordBatch   normalize, auto_dict_upgrade, sort_columns, to_arrow_array per column
```

One `RecordBatch` is returned. `InputBuffer::Owned` holds the bytes for the duration of the parse; there is no chunking. `ExecutionPlan` is applied per row in `finish_row` (`filter.check`).

## Parallel

```
(CORE) Pipeline::read_bytes_par(bytes, num_chunks)  or  read_path_par(path, num_chunks)
    |
    v
(ADAPTER BOUND) splitter.find_split_points(bytes, num_chunks) -> Vec<usize>  sorted, 0 and len included  (CORE) helper split_points_to_ranges
split_points_to_ranges(&points, len) -> Vec<Range>  (CORE)
    |
    v
(CORE) rayon::into_par_iter over Ranges  (thread pool, work stealing)
    each range -> (CORE) TableBuilder::with_plan(est, plan.clone())
                (ADAPTER BOUND) parser.validate(&bytes[range])?
                (ADAPTER BOUND) parser.parse_chunk(&bytes[range], &mut sink)?  (CORE) sink is TableBuilder
                Ok(sink)
    (CORE) catch_unwind per chunk -> Error::Merge("worker panicked ...") on panic
    collect::<Result<Vec<TableBuilder>>>()
    |
    v
(CORE) if !plan.auto_dict && schemas_consistent(&engines) {
    engines_to_record_batches(engines, &plan)   // fast path (CORE) (parallel Arrow build, unified schema, null_array for missing)
} else {
    merged = TableBuilder::with_plan(engines.len().max(64)*512, plan)  (CORE)
    for e in engines { merged.extend(e)? }      // merge path (CORE) (sequential extend with promotion)
    batch = merged.finish()  (CORE)
    if filter.is_some() { apply_compare_filter(batch, filter) }  (CORE) (pure Compare and And only)
    vec![batch]
}
```

Fast path (`engines_to_record_batches`) keeps one batch per chunk (chunked columns, no copy). It unifies schema via `unify_variants` and `promote_to_variant` so all batches share one `Schema`; missing columns are `null_array`. `rayon::par_iter` builds arrays in parallel. Merge path (`extend` loop) returns a single merged batch and handles `auto_dict` visibility (full cardinality) and irreconcilable type errors with `Error::Merge` naming the column.

Schemas consistent check (`parallel.rs:101`) builds `base: HashMap<&str,&str>` from `first.field_index` and `variant_key`, then verifies every other engine's `field_index` has the same key. Fast path is chosen only when `!auto_dict` and consistent.

## Bounded

```
(CORE) Pipeline::read_bytes_stream(bytes, budget)  or  read_path_stream(path, budget, prefault)
    |
    v
(CORE) BoundedExecutor::plan_chunks(bytes, splitter)   // splitter is (ADAPTER BOUND), the rest is (CORE)
    (ADAPTER BOUND) bytes_per_row = estimate_bytes_per_row(bytes).max(1)  (CORE) arithmetic
    total_rows_est = bytes.len() / bytes_per_row  (CORE)
    rows_per_batch = (budget.bytes() / bytes_per_row).max(1).min(total_rows_est.max(1))  (CORE)
    num_batches = (total_rows_est / rows_per_batch).max(1)  (CORE)
    (ADAPTER BOUND) split_points = splitter.find_split_points(bytes, num_batches.min(256))  (CORE) caps at 256
    chunks = split_points_to_ranges  (CORE)
    |
    v
(CORE) batch_engine = TableBuilder::with_plan(bytes_per_row.max(64), plan)
rows_in_batch = 0
for chunk in &chunks {
    chunk_bytes = &bytes[chunk.start..chunk.end]          // run_bytes path (CORE) slicing
    // or for run(path) with Mmap: (CORE) seek plus read_exact into Vec<u8> (file IO)
    (CORE) chunk_engine = TableBuilder::with_plan(chunk.len()/512, plan.clone())
    (ADAPTER BOUND) parser.validate(chunk_bytes)?; (ADAPTER BOUND) parse_chunk(chunk_bytes, &mut chunk_engine)?  (CORE) sink is TableBuilder
    (CORE) batch_engine.extend(chunk_engine)?          // single hash for new columns, Vec append otherwise
    rows_in_batch += chunk_rows
    if rows_in_batch >= rows_per_batch {
        batches.push(batch_engine.finish()?); batch_engine.reset(); rows_in_batch = 0  (CORE)
    }
}
if batch_engine.num_rows() > 0 { batches.push(batch_engine.finish()?) }  (CORE)
apply_plan_filter(&mut batches, &plan)  // pure Compare/And reapplication only (CORE)
```

`run_bytes` slices directly from `bytes` (used for decompressed buffers and for `Pipeline::read_bytes_stream`). `run` opens `InputBuffer`; if `Mmap` it drops the mapping after `plan_chunks` and reopens the file for `seek` plus `read_exact` per chunk (bounded RSS); if `Owned` it delegates to `run_bytes`. `MAX_SPLIT_CHUNKS = 100_000` caps split points.

## Input buffering  (CORE)

```
(CORE) InputBuffer::open(path, use_mmap, prefault)  (rypipe-core/src/input.rs)
    |
    +-- (CORE) detect_compression(path) reads 4 bytes
    |       1f 8b -> gzip (if feature gzip) (CORE) flate2, 28 b5 2f fd -> zstd (CORE) zstd crate, 04 22 4d 18 -> lz4 frame (CORE) lz4_flex
    |       if Some -> Owned(decompress(path, codec)?)  // read_to_end (CORE)
    |
    +-- else if cfg(mmap) && use_mmap -> (CORE) Mmap(MmapHandle::new(file, prefault)?) with WillNeed or Sequential
    +-- else -> (CORE) Owned(fs::read(path)?)
    |
    v
(CORE) input.as_slice() -> &[u8]  // passed to Pipeline::read_bytes variants or to BoundedExecutor
```

Note: adapter never touches `InputBuffer` directly; `Pipeline` does. Decompression is transparent (`Owned` served from memory) across all modes. `Mmap` is only for uncompressed files when the `mmap` feature is enabled and `use_mmap` is true.

Decompressed bytes are served from memory for all modes. `Pipeline::read_path` and `read_path_par` simply do `input.as_slice()` and call the bytes variants, so compression is transparent.

## Column lifecycle inside a row  (CORE) with (ADAPTER BOUND) events

```
(CORE) begin_row (no op)  // row boundaries tracked by row_count plus row_dirty
  (ADAPTER BOUND) put_field(k, v) -> (CORE) resolve(k) (ExecutionPlan::resolve_field, one hash) -> (CORE) ensure_column_idx (single hash for field_index plus Vec push if new) -> (CORE) set dirty bit row_dirty[idx/64]|=1<<(idx%64) -> (CORE) last_write_wins check (if len > row_count { pop }) -> (CORE) push_value (lexical parse or typed)
  (ADAPTER BOUND) put_field(k, v) duplicate in same row -> (CORE) pop previous value for this row (len > row_count) then push new, dirty stays true
  ... (ADAPTER BOUND) may call put_field_resolved(r, v) after resolve(r) to avoid second hash (see Decoder)
end_row -> (CORE) finish_row
    // bitmask: row_dirty is Vec<u64> with (columns.len()+63)/64 words
    for (i,b) in columns.iter_mut().enumerate() {  (CORE)
        let word = i/64; let bit = i%64;
        if (row_dirty[word]>>bit)&1==0 { b.push(None) }  // null fill only missing (CORE)
        else { row_dirty[word] &= !(1u64<<bit) }          // clear for next row (CORE)
    }
    if (CORE) filter.check(...) false { for b in columns { b.pop() } return }  // per row And/Or/Not with short circuit (CORE)
    row_count += 1  (CORE)
```

`row_dirty` is `Vec<u64>` bitmask with `(columns.len() + 63) / 64` words (kept in sync in `ensure_column_idx` plus `take_column` plus `extend`). See [Engine](./engine.md) and [Optimizations](./optimizations.md) for why this saves 80 percent of `push(None)` calls.

Adapter code never touches `row_dirty`; it only emits `put_field` events. The engine owns the dirty tracking.

`row_dirty` avoids a `while b.len() < target` check for touched columns and avoids pushing `None` for them. See [Engine](./engine.md) and [Optimizations](./optimizations.md).

## Error handling

All drivers propagate `Result` via `?`. Panics in parallel workers are caught with `catch_unwind` and turned into `Error::Merge`. Type mismatches in `extend` and `engines_to_record_batches` become `Error::Merge` with column name and hint to provide `field_types`. UTF-8 failures become `Error::Utf8`, I/O into `Error::Io`, Arrow construction into `Error::Arrow`, plan problems into `Error::Plan`. Python maps these to `ParseError`, `PlanError`, `MergeError`.
