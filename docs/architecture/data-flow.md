# Data flow

This page shows how bytes move through the system in each execution mode.
All modes share the same `Splitter` + `RecordParser` + `ExecutionPlan`;
only the driver differs.

See [Execution](./execution.md) for implementation details of each mode.

## Single thread

```
Pipeline::read_bytes(bytes)
  → InputBuffer::open → as_slice
  → TableBuilder::with_plan(cap, plan)
  → parser.validate(bytes)
  → parser.parse_chunk(bytes, &mut sink)
    loop: begin_row → put_field × N → end_row
  → TableBuilder::finish()
    normalize → auto_dict_upgrade → sort_columns → to_arrow_array
  → RecordBatch
```

One `RecordBatch` returned. `InputBuffer::Owned` holds bytes for the
duration. `ExecutionPlan` applied per row in `finish_row` (`filter.check`).

## Parallel

```
Pipeline::read_bytes_par(bytes, num_chunks)
  → splitter.find_split_points(bytes, num_chunks)
  → split_points_to_ranges → Vec<Range>
  → rayon::into_par_iter
    each range:
      TableBuilder::with_plan(est, plan.clone())
      parser.validate(&bytes[range])
      parser.parse_chunk(&bytes[range], &mut sink)
      Ok(sink)
  → collect::<Result<Vec<TableBuilder>>>()
  → if !auto_dict && schemas_consistent:
      engines_to_record_batches (fast path)
    else:
      merged.extend(each engine) → merged.finish() (merge path)
  → Vec<RecordBatch>
```

Fast path: one batch per chunk, unified schema, parallel array build.
Merge path: single merged batch, sequential extend with promotion.

## Bounded memory

```
Pipeline::read_bytes_stream(bytes, budget)
  → BoundedExecutor::plan_chunks(bytes, splitter)
    bytes_per_row = estimate_bytes_per_row(bytes)
    rows_per_batch = budget.bytes() / bytes_per_row
    num_batches = total_rows / rows_per_batch
    chunks = splitter.find_split_points(bytes, num_batches)
  → batch_engine = TableBuilder::with_plan(...)
  → for chunk in chunks:
      chunk_engine = TableBuilder::with_plan(...)
      parser.validate(chunk_bytes)
      parser.parse_chunk(chunk_bytes, &mut chunk_engine)
      batch_engine.extend(chunk_engine)
      if rows_in_batch >= rows_per_batch:
        batches.push(batch_engine.finish())
        batch_engine.reset()
  → flush remainder
  → apply_plan_filter (Compare/And reapplication only)
  → Vec<RecordBatch>
```

Constant RSS regardless of file size. Mmap path: drop mapping after
plan_chunks, reopen for seek+read per chunk.

## Column lifecycle inside a row

```
begin_row (no op, row tracked by row_count + row_dirty)
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
end_row → finish_row:
  for each column:
    if bit not set → push(None)  // null fill only missing
    else → clear bit
  filter.check → if false, pop all, return
  row_count += 1
```

## Memory management across modes

### Single thread

- InputBuffer holds the entire file (Owned or Mmap)
- One TableBuilder accumulates all rows
- Peak memory: O(file_size + rows × cols)
- No inter-thread communication

### Parallel

- InputBuffer holds the entire file (shared read-only)
- N TableBuilders (one per chunk) accumulate in parallel
- After parse: fast path exports N batches (no merge), merge path creates one
- Peak memory: O(file_size + N × chunk_rows × cols)
- Thread pool: rayon with work-stealing

### Bounded memory

- InputBuffer holds the entire file (or mmap)
- One TableBuilder accumulates, flushed periodically
- After each flush: batch is exported and dropped
- Peak memory: O(budget + per_chunk_overhead)
- RSS stays constant regardless of file size

### Key difference: parallel vs bounded

Parallel maximizes throughput by parsing all chunks simultaneously.
Bounded maximizes memory efficiency by processing one batch at a time.
The choice depends on file size vs available RAM:
- File < RAM: use parallel (fastest)
- File > RAM: use bounded (constant RSS)
- File ≈ RAM: use parallel with smaller budget

## Adapter interaction points

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

## Performance characteristics

### Single thread

- Parse time: O(bytes / row_size) × cost_per_field
- Memory: O(bytes) for InputBuffer + O(rows × cols) for TableBuilder
- No threading overhead, no synchronization
- Best for: small files (< 100 MB), streaming with backpressure

### Parallel

- Parse time: O(bytes / (row_size × threads)) × cost_per_field
- Memory: O(bytes / chunks × cols) per thread + O(rows × cols) for merge
- Threading overhead: rayon work-stealing + channel communication
- Best for: large files (>= 100 MB), full-RAM mode
- Scaling: typically 3-5× on 8 cores (limited by parse cost, not I/O)

### Bounded memory

- Parse time: O(bytes / row_size) × cost_per_field (same as single)
- Memory: bounded by `budget.bytes()` regardless of file size
- RSS: O(budget + per-chunk overhead)
- Best for: files larger than available RAM, streaming pipelines
- Trade-off: sequential processing, no parallelism within a batch

## Row-level event timeline

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

## Cross-chunk merge timeline

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
- extend(chunk0): columns [A,B,C], rows 120K
- extend(chunk1): D is new → backfill 120K nulls, then append 120K values
- extend(chunk2): all columns exist, just append
- extend(chunk3): all columns exist, just append
- finish: normalize, auto_dict, sort, export
