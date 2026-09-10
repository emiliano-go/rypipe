# Data flow { #data-flow }

This page shows how bytes move through the system in each execution mode.
All modes share the same `Splitter` + `RecordParser` + `ExecutionPlan`;
only the driver differs.

See [Execution](./execution.md) for implementation details of each mode.

## Single thread { #single-thread }

```
Pipeline::read_bytes(bytes)
  → TableBuilder::with_plan(cap, plan)
  → parser.validate(bytes)
  → parser.parse_chunk_generic(bytes, &mut sink)
    loop: begin_row → put_field × N → end_row
  → TableBuilder::finish()
    normalize → auto_dict_upgrade → sort_columns → to_arrow_array
  → RecordBatch
```

One `RecordBatch` returned. `read_bytes` works directly on the caller's
slice; `InputBuffer::Owned` holds the bytes for the duration only when the
input came from `read_path`. `ExecutionPlan` applied per row in `finish_row`
(`filter.check`).

## Parallel { #parallel }

```
Pipeline::read_bytes_par(bytes, num_chunks)
  → splitter.find_split_points(bytes, num_chunks)
  → split_points_to_ranges → Vec<Range>
  → rayon::into_par_iter
    each range:
      TableBuilder::with_plan(est, plan.clone())
      parser.validate(&bytes[range])
      parser.parse_chunk_generic(&bytes[range], &mut sink)
      Ok(sink)
  → collect::<Result<Vec<TableBuilder>>>()
  → if !auto_dict && schemas_consistent:
      engines_to_record_batches (fast path)
    else if auto_dict:
      per-chunk auto_dict_upgrade in parallel,
      unify + remap dictionaries,
      engines_to_record_batches (still fast path)
    else (schemas inconsistent):
      merged.extend(each engine) → merged.finish() (merge path)
      apply_compare_filter if plan.filter set
  → Vec<RecordBatch>
```

Fast path: one batch per chunk, unified schema, parallel array build. With
`auto_dict`, chunks are upgraded in parallel and dictionaries unified, so
execution stays on the fast path.
Merge path: single merged batch, sequential extend with promotion, followed
by a final filter pass.

## Bounded memory { #bounded-memory }

```
Pipeline::read_bytes_stream(bytes, budget)
  → BoundedExecutor::plan_chunks(bytes, splitter)
    bytes_per_row = estimate_bytes_per_row(bytes).max(1)
    total_rows_est = bytes.len() / bytes_per_row
    rows_per_batch = (budget.bytes() / bytes_per_row)
      .max(1).min(total_rows_est.max(1))
    num_batches = (total_rows_est / rows_per_batch).max(1)
      capped at split_cap (default MAX_SPLIT_CHUNKS = 100_000,
      overridable via plan.max_split_chunks)
    chunks = splitter.find_split_points(bytes, num_batches)
  → batch_engine = TableBuilder::with_plan(...)
  → for chunk in chunks:
      chunk_engine = TableBuilder::with_plan(...)
      parser.validate(chunk_bytes)
      parser.parse_chunk_generic(chunk_bytes, &mut chunk_engine)
      batch_engine.extend(chunk_engine)
      while rows_in_batch >= rows_per_batch
         or batch_engine.bytes_used() >= budget.bytes():
        n = rows_per_batch.min(num_rows)
          (shrunk by a bytes-based estimate when bytes_used is the trigger)
        to_consume = batch_engine.split_off(n)
        batch = to_consume.finish()
        apply_compare_filter(batch, filter) if plan.filter set
        batches.push(batch)
  → flush remainder (same finish + apply_compare_filter)
  → Vec<RecordBatch>
```

Constant RSS regardless of file size. Mmap path: drop mapping after
plan_chunks, reopen for seek+read per chunk.

## Parallel streaming { #parallel-streaming }

`Pipeline::read_path_stream_par` combines parallel parsing with bounded
memory: it returns a `ParallelStreamingBatchIterator` that yields
`RecordBatch`es as worker threads produce them, instead of collecting a
`Vec`. This mode uses channels for inter-thread communication.

Bounded mode also has consumer variants (`read_bytes_stream_consumer`,
`read_path_stream_consumer`) that deliver each flushed batch to a
`BatchConsumer` callback rather than collecting a `Vec<RecordBatch>`.

## Column lifecycle inside a row { #column-lifecycle-inside-a-row }

```
begin_row
  no filter: no op (row tracked by row_count + row_dirty)
  with plan.filter: reset current_ordinal, clear the RowBuffer, and run the
    adaptive late-predicate check (if predicate_ordinal >= ncols * 4 / 5,
    buffering is deemed not worthwhile and rows fall back to direct push)
  put_field(k, v):
    resolve(k) → ExecutionPlan::resolve_field (one hash)
    ensure_column_idx (single hash for field_index + Vec push if new)
    set dirty bit: row_dirty[idx/64] |= 1u64 << (idx%64)
    last-write-wins: if columns[idx].len() > row_count { pop }
    push_value(v)
  put_field(k, v) duplicate:
    pop previous value, push new, dirty stays true
  put_field_resolved(r, v):
    ensure_column_idx(r) → set dirty → push_value
    (skips resolve hash)
  put_field_at(slot, v):
    set dirty → push_value
    (skips resolve hash + ensure_column_idx)
end_row:
  no filter (or late-predicate fallback) → finish_row:
    for each column:
      if bit not set → push(None)  // null fill only missing
      else → clear bit
    filter.check → if false, pop all, return
    row_count += 1
  with plan.filter (buffered path):
    evaluate_against_null if predicate still undecided
    drain_buffered(pass): flush buffered values on pass, drop them on reject
```

### Observer hooks { #observer-hooks }

When `plan.observer` is set, the sink fires hooks as rows move through:
`on_begin_row` in `begin_row`, `on_put_field` in `put_field`,
`on_row_accepted` / `on_row_rejected` in `finish_row`, and
`on_chunk_finished` in `finish`. With a filtered plan, `on_put_field` may
fire for a row that is later rejected; `on_row_rejected` still reports it.

## Memory management across modes { #memory-management-across-modes }

### Single thread { #single-thread }

- InputBuffer holds the entire file (Owned or Mmap)
- One TableBuilder accumulates all rows
- Peak memory: O(file_size + rows × cols)
- No inter-thread communication

### Parallel { #parallel }

- InputBuffer holds the entire file (shared read-only)
- N TableBuilders (one per chunk) accumulate in parallel
- After parse: fast path exports N batches (no merge), merge path creates one
- Peak memory: O(file_size + N × chunk_rows × cols)
- Thread pool: rayon with work-stealing

### Bounded memory { #bounded-memory }

- InputBuffer holds the entire file (or mmap)
- One TableBuilder accumulates, flushed periodically
- After each flush: batch is exported and dropped.
- Peak memory: O(budget + batch); the budget is a soft target enforced by a
  `bytes_used` trigger, not a hard bound.
- RSS stays roughly constant regardless of file size.

### Key difference: parallel vs bounded { #key-difference-parallel-vs-bounded }

Parallel maximizes throughput by parsing all chunks simultaneously.
Bounded maximizes memory efficiency by processing one batch at a time.
The choice depends on file size vs available RAM:

- File < RAM: use parallel (fastest)
- File > RAM: use bounded (constant RSS)
- File ≈ RAM: use parallel with smaller budget

## Adapter interaction points { #adapter-interaction-points }

The adapter interacts with the engine at these specific points:

1. **`Splitter.find_split_points`**: called once per parse, returns chunk
   boundaries. The engine uses these to create independent byte ranges.
2. **`RecordParser.validate`**: called once per chunk, before parsing.
   Use for upfront checks like UTF-8 validation.
3. **`RecordParser.parse_chunk`**: called once per chunk, feeds
   `ColumnarSink` with `begin_row`/`put_field`/`end_row` events.
4. **`ColumnarSink.begin_row/put_field/end_row`**: called per row per
   field. The engine resolves names, stores values, and tracks dirty bits.
5. **`ColumnarSink.finish`**: called once after all chunks, returns
   Arrow `RecordBatch`. Triggers normalize, auto_dict, sort, export.

All other work (parallelism, memory management, Arrow export, filtering)
is handled by the engine. The adapter never touches `TableBuilder`
internals, `InputBuffer`, or `ExecutionPlan`.

## Performance characteristics { #performance-characteristics }

### Single thread { #single-thread }

- Parse time: O(bytes / row_size) × cost_per_field
- Memory: O(bytes) for InputBuffer + O(rows × cols) for TableBuilder
- No threading overhead, no synchronization
- Best for: small files (< 100 MB), streaming with backpressure

### Parallel { #parallel }

- Parse time: O(bytes / (row_size × threads)) × cost_per_field
- Memory: O(bytes / chunks × cols) per thread + O(rows × cols) for merge
- Threading overhead: rayon work-stealing + per-chunk panic capture
- Best for: large files (>= 100 MB), full-RAM mode
- Scaling: typically 3-5× on 8 cores (limited by parse cost, not I/O)

### Bounded memory { #bounded-memory }

- Parse time: O(bytes / row_size) × cost_per_field (same as single)
- Memory: O(budget + batch); the budget is a soft target enforced by the
  `bytes_used` flush trigger, not a hard bound (batches may exceed it when
  the required batch count hits the split cap)
- RSS: O(budget + per-chunk overhead)
- Best for: files larger than available RAM, streaming pipelines
- Trade-off: sequential processing, no parallelism within a batch

## Row-level event timeline { #row-level-event-timeline }

For a row with fields A, B, C (A missing):

```
begin_row
  put_field("B", 42)     → resolve("B") → ensure_column_idx → set dirty → push_value
  put_field("C", "hello") → resolve("C") → ensure_column_idx → set dirty → push_value
end_row → finish_row:
  column A: dirty bit 0 → push(None)     // null fill
  column B: dirty bit 1 → clear bit      // already has value
  column C: dirty bit 1 → clear bit      // already has value
  filter.check → pass
  row_count += 1
```

## Cross-chunk merge timeline { #cross-chunk-merge-timeline }

For parallel parse with 4 chunks:

```
Chunk 0: parse → TableBuilder { cols: [A,B,C], rows: 120K }
Chunk 1: parse → TableBuilder { cols: [A,B,D], rows: 120K }  // D is new
Chunk 2: parse → TableBuilder { cols: [A,B,C], rows: 120K }
Chunk 3: parse → TableBuilder { cols: [A,B,C], rows: 122K }
```

Fast path (schemas consistent): export each as separate RecordBatch with
unified schema (D null-filled in chunks 0,2,3).

Merge path: extend sequentially:

- merged starts empty
- extend(chunk0): columns `A,B,C`, rows 120K
- extend(chunk1): D is new → backfill 120K nulls, then append 120K values
- extend(chunk2): all columns exist, just append
- extend(chunk3): all columns exist, just append
- finish: normalize, auto_dict, sort, export
