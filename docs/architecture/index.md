# Architecture Overview { #architecture-overview }

`rypipe` is built around one idea: **separate the parts of parsing that depend
on a file format from the parts that do not.** The format-specific side answers
where a row ends and how to extract fields; the format-agnostic side answers how
to store rows as typed columns, how to project them, and how to get them out as
Arrow.

This directory documents the architecture in depth: read this page first, then
dive into the subpages for the component you care about.

## Pages { #pages }

| Page | Lines | Focus |
|------|-------|-------|
| [Engine](./engine.md) | TableBuilder: row handling, dirty tracking, column dispatch |
| [Columnar storage](./columnar.md) | StrColumn, ColumnBuilder variants, dictionary, promotions |
| [Execution plan](./plan.md) | ExecutionPlan, FieldType, FilterPredicate trees |
| [Execution](./execution.md) | Pipeline, Parallel, Bounded, InputBuffer |
| [Decoder API](./decoder.md) | Splitter, RecordParser, ColumnarSink |
| [Data flow](./data-flow.md) | Diagrams for single, parallel, bounded modes |
| [Optimizations](./optimizations.md) | Every optimization and why it matters |
| [Storage and export](./storage.md) | Arrow export, null handling, compare filter |

## Design philosophy { #design-philosophy }

The engine was extracted from a single format (Crystal Reports XML) and
generalized so that the same hot path serves every adapter. The split is
deliberate and strict:

**Adapter-bound** (format specific, lives in adapter crates, you implement it):
where does one row end, how do I extract field names and values, what is the
encoding or entity rule. This code knows the file format and nothing about
column storage.

**Core** (format agnostic, lives in `rypipe` crates, reused by every adapter):
how do I store rows as typed columns, how do I rename, drop, cast, filter,
and reorder, how do I export to Arrow, how do I parallelize while staying
inside a memory budget. This code knows the ingestion framework and nothing about
any file format.

Adapters live in separate packages. `rypipe` ships zero parsers. The boundary
is a trait, not a stringly convention.

## High-level flow { #high-level-flow }

```
input bytes
    |
    v
+---------------------+  (ADAPTER) format specific
|   Splitter          |  find_split_points, estimate_bytes_per_row
+----------+----------+
           | Vec<Range<usize>>  (CORE) split_points_to_ranges
           v
+---------------------+  (ADAPTER) format specific
| RecordParser        |  validate, parse_chunk
|                     |  -> begin_row, put_field, end_row
+----------+----------+
           | Value events  (CORE) Value<'a>
           v
+---------------------+  (CORE) format agnostic
|  TableBuilder       |  ColumnarSink implementation
|  (ColumnarSink)     |  columns: Vec<ColumnBuilder>
|                     |  field_index: HashMap<String,usize>
|                     |  row_dirty: Vec<u64>
|                     |  plan: Arc<ExecutionPlan>
+----------+----------+
           | RecordBatch  (CORE) Arrow
           v
+---------------------+  (CORE) format agnostic
|  Arrow export       |  to_arrow_array, apply_compare_filter
+---------------------+
           | pyarrow.Table / pandas / Polars
           v
      downstream
```

Ownership rule: `Splitter` and `RecordParser` never see `ExecutionPlan` or
`TableBuilder` internals. They emit plain names and `Value` events. The engine
resolves names via `ExecutionPlan::resolve_field` and chooses storage via
`ExecutionPlan::column_type`. This keeps adapters tiny and keeps all pushdown,
typing, and Arrow logic in one place.

## Crate overview { #crate-overview }

### rypipe-core (pure Rust, no pyo3) { #rypipe-core }

| Module | Responsibility |
|--------|---------------|
| `value` | `Value<'a>` enum: Str(Cow), Int64, Float64, Bool, Date32, Timestamp, Null |
| `plan` | ExecutionPlan, FieldType, FilterPredicate trees, CompareOp |
| `columnar` | StrColumn, ColumnBuilder (7 variants), dictionary, promotions |
| `engine` | TableBuilder with ColumnarSink impl, dirty bitmask, predicate-first |
| `decoder` | Splitter, RecordParser, ColumnarSink traits, plan_chunk_count |
| `pipeline` | Pipeline<S,P> wiring splitter + parser to TableBuilder |
| `parallel` | ParallelExecutor: rayon, fast path vs merge path |
| `bounded` | BoundedExecutor, MemoryBudget, mmap-seek path |
| `input` | InputBuffer (Mmap/Owned), compression detection |
| `merge` | TableBuilder::extend, engines_to_record_batches |
| `arrow_export` | null_array, apply_compare_filter |
| `scan` | Portable byte-search primitives (find, find2, find_literal) |
| `bench` | Tier ladder, alloc_baseline, ParProfile (behind bench feature) |
| `schema` | FrozenSchema, DiscoveryOpts, layout_signature caching |

See [Schema](./schema.md) for the detailed architecture of schema handling.
| `dict` | SeedDict, unify_dictionaries, apply_remap |
| `block_masks` | SIMD 64-byte block delimiter scanning |
| `parallel_stream` | ParallelStreamingExecutor, discovery, ordered/unordered delivery |
| `streaming` | StreamingBatchIterator: channel-based pull iterator |
| `consumer` | BatchConsumer, CollectingConsumer, DiscardingConsumer |
| `error` | Error enum (Utf8, Io, Plan, Merge, Arrow), Result type alias |

### rypipe-python (PyO3 bindings) { #rypipe-python }

| Module | Responsibility |
|--------|---------------|
| `lib.rs` | Extension module, exception types |
| `plan_kwargs.rs` | Python kwargs to ExecutionPlan conversion |
| `export.rs` | Arrow to PyArrow via C Data Interface |

## State and ownership { #state-and-ownership }

`TableBuilder` owns three parallel structures:
- `columns: Vec<ColumnBuilder>` (dense storage)
- `field_index: HashMap<String, usize>` (name to Vec index)
- `column_order: Vec<String>` (output order)

A fourth `row_dirty: Vec<u64>` bitmask tracks touched columns. All are kept
in sync via `ensure_column_idx`, `take_column`, and `extend`.

`Pipeline<S,P>` is `Clone` where `S: Splitter + Clone` and `P: RecordParser + Clone`,
so the same pipeline can be reused across files and execution modes.

## Testing strategy { #testing-strategy }

Every optimization has a test that verifies correctness:
- Splitter tests: monotonic points, coverage, comment/CDATA rejection.
- Engine tests: extend, last-write-wins, rename, drop, filter, typed columns.
- Columnar tests: push/pop, split_off, arrow export, dictionary upgrade.
- Integration tests: whole-file parse == N-chunk parse for N in {1, 2, 7, 64}.

## Next steps { #next-steps }

- [Engine](./engine.md) for the hot path (row handling, dirty tracking, predicate-first)
- [Columnar](./columnar.md) for storage internals (StrColumn, ColumnBuilder, dictionary)
- [Execution](./execution.md) for scheduling, parallelism, and bounded memory
- [Decoder API](./decoder.md) for the trait boundary (Splitter, RecordParser, ColumnarSink)
- [Data flow](./data-flow.md) for diagrams of each execution mode
- [Optimizations](./optimizations.md) for every change from the original design
- [Storage and export](./storage.md) for Arrow type mapping and null handling
- [Execution plan](./plan.md) for pushdown plans and filter predicates

## Key invariants { #key-invariants }

1. **Adapter never sees ExecutionPlan.** The boundary is a trait. Adapters
   emit plain names and `Value` events; the engine resolves names and
   chooses storage. This keeps adapters tiny and all pushdown logic in
   one place.

2. **One hash per field in steady state.** `field_index.get(name)` in
   `ensure_column_idx` is the only HashMap probe per field. `resolve_field`
   is called once per field when using `resolve` + `put_field_resolved`.

3. **Row_dirty tracks touched columns.** The bitmask is cleared in
   `finish_row` and kept in sync by `ensure_column_idx`, `take_column`,
   and `extend`. It enables null-fill of only missing columns.

4. **Zero-copy Arrow export.** `to_arrow_array` uses `mem::take` to move
   internal buffers into Arrow arrays. No data copying.

5. **Predicate-first evaluation.** When a filter is active, fields are
   buffered until the predicate resolves. The engine switches to direct
   mode on Pass, discards on Fail.

6. **Layout prediction.** After the first row, `expect_slot` enables
   memcmp-based field resolution, skipping attribute scan + hash lookup.

## Design decisions { #design-decisions }

### Why not HashMap<String, ColumnBuilder>? { #why-not-hashmapstring-columnbuilder }

The original design used a HashMap for column storage. This required two
hash probes per field: one to check if the column exists, one to get a
mutable reference. With dense Vec storage, the second probe becomes a
bounds-checked array access. This saves ~100 ns per row for 10 fields.

### Why a bitmask for null-fill? { #why-a-bitmask-for-null-fill }

The naive approach pushes `None` for every missing column. With 10 columns
where 8 are present, this wastes 80% of pushes. The bitmask tracks which
columns were touched, so `finish_row` only pushes `None` for the 2 missing
columns. The bitmask test is a single word load + bit test, cheaper than
a `Vec` push per column.

### Why predicate-first? { #why-predicate-first }

For selective filters (e.g., 10% pass rate), most rows are rejected early.
Without predicate-first, the engine pushes all fields into columns, then
pops them on rejection. With predicate-first, the engine buffers fields
until the predicate column arrives, evaluates, and either discards the
buffer (Fail) or drains it to columns (Pass). This eliminates 90% of
push/pop cycles for selective filters.

### Why not override find_split_points? { #why-not-override-find_split_points }

The default implementation applies the measured 2 MiB chunk floor that
prevents sub-MB chunk collapse. Two adapters got this wrong: TSV collected
first K newlines (negative scaling), crxml scanned entire file for `<!`
(25% overhead). The default eliminates this bug class.
