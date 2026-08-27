# Architecture Overview

`rypipe` is built around one idea: **separate the parts of parsing that depend on a file format from the parts that do not.** The format specific side answers where a row ends and how to extract fields; the format agnostic side answers how to store rows as typed columns, how to project them, and how to get them out as Arrow.

This directory documents the architecture in depth. If you are new, read this page first; then dive into the subpages for the component you care about.

* [Engine (TableBuilder)](./engine.md): row handling, last write wins, dirty tracking, column dispatch
* [Columnar storage](./columnar.md): `StrColumn`, `ColumnBuilder` variants, dictionary, auto dict, promotions
* [Execution plan](./plan.md): `ExecutionPlan`, `FieldType`, `FilterPredicate` trees
* [Execution (Pipeline, Parallel, Bounded)](./execution.md): `Pipeline`, `ParallelExecutor`, `BoundedExecutor`, `InputBuffer`
* [Decoder API](./decoder.md): `Splitter`, `RecordParser`, `ColumnarSink` with `resolve` and `put_field_resolved`
* [Data flow](./data-flow.md): diagrams for single, parallel, and bounded modes
* [Optimizations](./optimizations.md): every optimization, why it matters, and what it replaces
* [Storage and export](./storage.md): Arrow export, null handling, and compare filter reapplication

## Design philosophy

The engine was extracted from a single format (Crystal Reports XML) and generalized so that the same hot path serves every adapter. The split is deliberate and strict:

**Adapter bound (format specific, lives in adapter crates, you implement it):** where does one row end, how do I extract field names and values, what is the encoding or entity rule. This code knows the file format and nothing about column storage.

**Core (format agnostic, lives in `rypipe` crates, reused by every adapter):** how do I store rows as typed columns, how do I rename, drop, cast, filter, and reorder, how do I export to Arrow, how do I parallelize while staying inside a memory budget. This code knows the columnar engine and nothing about any file format.

Adapters live in separate packages. `rypipe` ships zero parsers. The boundary is a trait, not a stringly convention.

## High level flow

Legend: `(ADAPTER BOUND)` = you write it in the adapter crate; `(CORE)` = lives in `rypipe` and is reused unchanged.

```
input bytes  [ADAPTER or CORE decides the source; InputBuffer below]
    |
    v
+---------------------+  (ADAPTER BOUND) format specific
|   Splitter          |  Trait you implement
|                     |  find_split_points, estimate_bytes_per_row
+----------+----------+
           | Vec<Range<usize>>  (CORE) helper split_points_to_ranges
           v
+---------------------+  (ADAPTER BOUND) format specific
| RecordParser        |  Trait you implement
|                     |  validate, parse_chunk -> begin_row, put_field, end_row
+----------+----------+
           | Value events  (CORE) enum Value<'a>
           | Str, Int64, Float64, Bool, Date32, Timestamp, Null
           v
+---------------------+  (CORE) format agnostic
|  TableBuilder       |  Struct in rypipe-core/src/engine.rs
|  (ColumnarSink)     |  columns: Vec<ColumnBuilder> (CORE)
|                     |  field_index: HashMap<String,usize> (CORE)
|                     |  column_order: Vec<String> (CORE)
|                     |  row_dirty: Vec<bool> (CORE)
|                     |  plan: ExecutionPlan (CORE)
+----------+----------+
           | RecordBatch  (CORE) Arrow
           v
+---------------------+  (CORE) format agnostic
|  Arrow export       |  rypipe-core/src/arrow_export.rs plus engine.rs:finish
|                     |  to_arrow_array, apply_compare_filter, null_array
+---------------------+
           | pyarrow.Table / pandas / Polars  [PYTHON CORE] via rypipe-python C Data Interface
           v
      downstream (join, aggregate, plot) [PYTHON / ADAPTER CONSUMER]
```

Ownership rule: `Splitter` plus `RecordParser` never see `ExecutionPlan` or `TableBuilder` internals. They emit plain names and `Value` events; the engine resolves names (`ExecutionPlan::resolve_field`) and chooses storage (`column_type`). This keeps adapters tiny and keeps all pushdown, typing, and Arrow logic in one place.

## Crate overview

All format specific code is **ADAPTER BOUND** (separate crates, not in this repo). All rows below are **CORE** (format agnostic, in `rypipe`).

### `rypipe-core` (pure Rust, no pyo3, no quick-xml) (CORE)

| Module | File | Side | Responsibility |
|--------|------|------|----------------|
| `value` | `value.rs` | CORE | `Value<'a>` enum: `Str(&str)`, `Int64`, `Float64`, `Bool`, `Date32(i32)`, `Timestamp(i64)`, `Null` |
| `plan` | `plan.rs` | CORE | `ExecutionPlan`, `FieldType` (String, Int64, Float64, Boolean, Dictionary, Date32, Timestamp(unit)), `FilterPredicate` (Equal, NotEqual, Compare, And, Or, Not), `CompareOp` |
| `columnar` | `columnar.rs` | CORE | `StrColumn` (arena plus offsets plus validity), `ColumnBuilder` (7 variants), dictionary `value -> i32` index, `try_upgrade_to_dict`, `unify_variants`, `promote_to_variant` |
| `engine` | `engine.rs` | CORE | `TableBuilder` with `Vec<ColumnBuilder>`, `field_index`, `column_order`, `row_count`, `row_dirty`, `estimated_rows`, `plan`; `ensure_column_idx`, `push_field_resolved`, `push_field`, `finish_row`, `normalize`, `auto_dict_upgrade`, `sort_columns`, `finish` |
| `decoder` | `decoder.rs` | CORE (traits) / ADAPTER BOUND (impls) | Traits `Splitter`, `RecordParser`, `ColumnarSink` (with `resolve` and `put_field_resolved`) plus `split_points_to_ranges`; adapters implement the first two, core implements the third |
| `pipeline` | `pipeline.rs` | CORE | `Pipeline<S,P>` wiring `Splitter` plus `RecordParser` to `TableBuilder` with `read_bytes`, `read_bytes_par`, `read_bytes_stream`, `read_path`, `read_path_par`, `read_path_stream` |
| `parallel` | `parallel.rs` | CORE | `ParallelExecutor::parse` with rayon, fast path vs merge path, `schemas_consistent` |
| `bounded` | `bounded.rs` | CORE | `BoundedExecutor` plus `MemoryBudget`, `run`, `run_bytes`, `run_mapped`, `plan_chunks`, `MAX_SPLIT_CHUNKS = 256` |
| `input` | `input.rs` | CORE | `InputBuffer` (`Mmap` or `Owned`), `MmapHandle`, magic byte detection for `gzip` (`1f 8b`), `zstd` (`28 b5 2f fd`), `lz4` frame (`04 22 4d 18`), transparent decompression |
| `merge` | `merge.rs` | CORE | `TableBuilder::extend`, `engines_to_record_batches` with variant unification and promotion |
| `arrow_export` | `arrow_export.rs` | CORE | `null_array`, `apply_compare_filter` (pure Compare and And only; other trees are no ops), `compare_columns`, `is_numeric` |
| `error` | `error.rs` | CORE | `Error` enum (`Utf8`, `Plan(String)`, `Merge(String)`, `Io`, `Arrow`) and `Result` |
| `lib` | `lib.rs` | CORE | Reexports |

**Not in `rypipe-core`:** `XmlSplitter`, `CsvSplitter`, `JsonSplitter`, `CrystalXmlDecoder` etc. are **ADAPTER BOUND** and live in separate packages (see `docs/crxml-adapter.md` and `docs/writing-adapters.md`). They depend on `rypipe-core` but `rypipe-core` never imports them.

### `rypipe-python` (PyO3 bindings) (CORE)

| Module | File | Side | Responsibility |
|--------|------|------|----------------|
| `lib.rs` | `crates/rypipe-python/src/lib.rs` | CORE | `_rypipe` extension, exception types (`ParseError`, `XmlError`, `PlanError`, `MergeError`), `py_err_from_rypipe` |
| `plan_kwargs.rs` | `plan_kwargs.rs` | CORE | `execution_plan_from_kwargs` converting Python kwargs to `ExecutionPlan` with nested filter trees (`and`, `or`, `not`) |
| `export.rs` | `export.rs` | CORE | `record_batch_to_pyarrow`, `record_batches_to_pyarrow_batches`, `record_batches_to_pyarrow_table` |

### Python package `rypipe` (CORE)

| Module | File | Side | Responsibility |
|--------|------|------|----------------|
| `source` | `rypipe/source.py` | CORE | `Source` abstract base, `Adapter` convenience base (adapters subclass it; this file itself is core) |
| `pipeline` | `rypipe/pipeline.py` | CORE | `Pipeline` stage chaining (`|`), `plan_split` fusion, `to_arrow` short circuit |
| `fusion` | `rypipe/fusion.py` | CORE | `plan_split` (multi filter `and` combine), `_try_columnar_fusion`, `fused_iter` |
| `batchpipe` | `rypipe/batchpipe.py` | CORE | `Batch`, `Operator`, `ArrowSource`, `FusedTransforms`, `LambdaOp`, `build_chain`, `iter_dicts`, `collect_table` |
| `stages` | `rypipe/stages/*.py` | CORE | `RenameFields`, `DropFields`, `CastTypes`, `FilterRows`, `FilterRowsAny`, `FilterRowsAll`, `FilterRowsNot` |
| `sinks` | `rypipe/sinks.py` | CORE | `collect`, `to_arrow`, `to_dataframe`, `to_polars`, `to_parquet`, `to_csv` |

**ADAPTER BOUND** on the Python side: `my_adapter.MySource`, `my_adapter.MyAdapter`, `CrystalXmlSource` etc. They subclass `Source`/`Adapter` and are not in `rypipe`.

## How the boundary is enforced

`RecordParser` never sees `ExecutionPlan`. It emits plain field names and `Value` events. `TableBuilder` (through `ColumnarSink::put_field` or `put_field_resolved`) resolves those names via `ExecutionPlan::resolve_field` (rename then drop) and chooses storage via `ExecutionPlan::column_type`. This keeps adapters tiny and keeps all pushdown logic in one place.

`ColumnarSink::wants` and the newer pair `resolve` plus `put_field_resolved` let adapters skip dropped fields with one hash lookup instead of two. See [Decoder API](./decoder.md) and [Optimizations](./optimizations.md).

## State and ownership

`TableBuilder` owns three parallel structures: `columns: Vec<ColumnBuilder>` (dense storage), `field_index: HashMap<String, usize>` (name to Vec index), and `column_order: Vec<String>` (output order). A fourth vector `row_dirty: Vec<bool>` tracks which columns were touched in the current row (see [Engine](./engine.md) and [Optimizations](./optimizations.md)). All are cleared together in `reset` and kept in sync in `take_column` (swap_remove with index patching) and `extend`.

`Pipeline<S,P>` is `Clone` where `S: Splitter + Clone` and `P: RecordParser + Clone`, so the same pipeline can be reused across files and execution modes.

## Next steps

Start with [Engine](./engine.md) if you want the hot path, [Columnar](./columnar.md) if you want storage, or [Execution](./execution.md) if you want scheduling. [Optimizations](./optimizations.md) explains every change from the original `HashMap`-based design and why it matters for all adapters, not just one format.
