---
title: Changelog
description: Release notes for rypipe, newest first. Tracks features, fixes, and breaking changes across all published versions.
---

# Changelog

All notable changes to rypipe, newest first. Versions follow semantic versioning and are tagged in the repository.

## [0.3.1] - 2026-09-11

### Fixed

- **Flaky schema cache test.** Removed global counter assertions from `schema_cache_hits_misses_and_plan_changes` that were affected by parallel test interference.

### Changed

- **Publishing CI: test blocker.** Both PyPI and crates.io publishing workflows now run tests as a required gate before publishing.

## [0.3.0] - 2026-09-11

### Added

- **Expression API with `col()` for fusable filter predicates.** `col("amount") > 100` builds a predicate that fuses into the Rust parse loop, just like keyword-form filters. Supports `&` (and), `|` (or), `~` (not) composition, and methods: `isin`, `not_in`, `startswith`, `endswith`, `contains`, `matches`, `between`, `is_null`, `is_not_null`, `is_type`.
- **`FilterRowsAny`, `FilterRowsAll`, `FilterRowsNot` combinators.** Build OR, AND, and NOT logic over keyword-form filters. Combinators are fusable and nestable: `FilterRowsAny(A, FilterRowsAll(B, FilterRowsNot(C)))` expresses `A or (B and not C)`.
- **`RowObserver` trait and Python `observer` hooks.** Per-row instrumentation fired during parsing. The Rust `RowObserver` trait has methods `on_begin_row`, `on_put_field`, `on_row_accepted`, `on_row_rejected`, `on_chunk_finished`. Python passes `observer={"on_row_rejected": fn, ...}` in plan kwargs, wrapping callables in a `PyObserver`.
- **`strict_types` mode.** `strict_types=True` on `Source.__init__` or `ExecutionPlan` aborts the read when a non-null value fails to parse into its declared type, instead of silently nulling. Genuine nulls remain null.
- **`max_split_chunks` parameter.** Caps the number of bounded-streaming chunks. Defaults to 100,000. Exposed as `max_split_chunks` on `Source.__init__` and `ExecutionPlan`.
- **`MemoryBudget` struct and `BatchConsumer` trait.** `MemoryBudget::new(bytes)` sets a streaming budget. `BatchConsumer` is a callback trait for streaming reads without batch collection: `Pipeline::read_bytes_stream_consumer(bytes, budget, consumer)`.
- **`parse_decimal128` support.** `FieldType::Decimal128` and `CastTypes` now parse decimal values.
- **`rypipe-test` crate.** Property-based testing helpers for adapter development: proptest strategies (`arb_field_name`, `arb_field_value`, `arb_record`, `arb_records`, `arb_malformed_utf8`, `arb_nested_quotes`), fixtures (`MALFORMED_UTF8`, `NESTED_QUOTES`, `EMPTY_VALUES`), and test helpers (`parse_test_bytes`, `assert_batches_equal`, `KeyValueParser`, `NewlineSplitter`).
- **cargo-generate template for adapter scaffolding.** `cargo generate emiliano-go/rypipe template` creates a ready-to-build adapter package.
- **`chunks=` kwarg on `CrystalXMLSource`.** Passes through to `max_split_chunks` for parallel reads.
- **`memory=` kwarg on all sink functions.** `collect(pipeline, memory="64MiB")`, `to_pandas(pipeline, memory=...)`, `to_polars(pipeline, memory=...)`, `to_parquet(pipeline, path, memory=...)` all support bounded-memory streaming.
- **`discover_schema()` function.** Scans a file once and returns column names after applying `field_mapping`, `drop_fields`, etc. Avoids per-file discovery overhead.
- **`Pipeline.to_pandas()`, `to_polars()`, `to_parquet()` methods.** Materialize pipeline results directly without importing sink functions.
- **Binary memory units.** `iter_record_batches` and `Source.to_pandas`/`to_polars`/`to_parquet` accept `KiB`, `MiB`, `GiB`, `TiB` in addition to `KB`, `MB`, `GB`, `TB`.
- **`starts_with`, `ends_with`, `contains` filter operators.** Fusable keyword-form operators for string prefix, suffix, and substring matching.
- **`schema()` respects projection.** When `schema=["Name", "Amount"]` is passed, `Source.schema()` returns `["Name", "Amount"]` instead of the full column list.
- **`Splitter::find_split_points` default method.** Finds safe split points via nominal offsets, `next_record_start`, skip-region rejection, dedup, and sort. Adapters rarely need to override.
- **`FrozenSchema::from_partial_plan` and `is_exact`.** `from_partial_plan` builds a schema from a partial column list. `is_exact` reports whether the schema matches exactly.
- **Schema cache functions.** `insert_schema_cache`, `clear_schema_cache`, `schema_cache_stats`, `dynamic_window_count`, `dynamic_window_size`, `layout_signature` for schema discovery optimization.
- **Timestamp format variants.** `FieldType::Timestamp(TimeUnit)` supports `Second`, `Millisecond`, `Microsecond`, `Nanosecond`, plus custom chrono formats: `timestamp[ms,format=%Y%m%d %H:%M]`.
- **`Predicate` composable trait.** Python objects with `_to_spec()` returning a spec dict can be passed as `FilterRows(predicate=obj)`.

### Fixed

- **`ArithmeticCompare.cmp_value` type.** Was documented as `f64`, corrected to `String`.
- **`Length.value` type.** Was documented as `i64`, corrected to `String`.
- **Compound filter specs fall back to Python with full plan override.** `FilterRowsAny`/`All`/`Not` and unsupported ops (`is_null`, `is_type`, `regex`, comparisons) now correctly apply rename, drop, and cast transformations in the Python fallback path.
- **`iter_record_batches` memory passthrough.** Memory strings are parsed to bytes before passing to the Rust extension, which only accepts integer byte counts.
- **`schema()` projection.** `Source.schema()` now returns `self._schema` when set, respecting the projection.

### Changed

- **`to_dataframe()` removed.** Use `to_pandas()` instead. The alias existed for backward compatibility with crxml 2.1.
- **Lambda compiler sunset.** The in-tree lambda bytecode compiler is removed. Use `col()` expressions or keyword-form filters instead; for arbitrary logic, use plain callables (Python fallback, not fusable).
- **`ColumnarSink` trait expanded.** 10 new methods: `put_row`, `resolve_raw`, `resolve_and_put_raw`, `put_field_resolved`, `put_field_at`, `resolve_and_put`, `needs_value`, `needs_resolve`, `wanted_mask`, `expect_slot`.
- **`Pipeline` expanded.** 3 new methods: `read_bytes_stream_consumer`, `read_path_stream_consumer`, `read_path_stream_par`.
- **`ExecutionPlan` gained fields.** `strict_types: bool`, `max_split_chunks: Option<usize>`, `observer: Option<Arc<dyn RowObserver>>`.

## [0.2.2] - 2026-09-04

### Added

- **Contiguous dictionary storage.** Dictionary-encoded columns now use contiguous storage for better cache locality and compression.

### Fixed

- **Pipeline examples use `CrystalXMLSource`** instead of `rypipe.read` in documentation.
- **Hello World adapter example** tested and working.
- **Prose text cleanup.** Replaced ` : ` with `, ` across 4 files.
- **Documentation fixes.** Removed redundant Pages table, fixed missing blank lines before numbered lists, updated performance numbers in why-python.

## [0.2.1] - 2026-09-03

### Fixed

- **`wanted_mask` projection short-circuit.** Projection short-circuit fired incorrectly when `field_map` or `drop_fields` were active.
- **Cross-platform `is_x86_feature_detected!` guard.** Added `cfg` guard for non-x86 platforms.
- **Dead code on `RowBuffer::new`.** Allowed `dead_code` to fix clippy error.
- **`bench_tier` return type.** Corrected from `(f64, usize)` to `(f64, f64, usize)`.
- **`_headers` file for correct Content-Type** on `sitemap.xml`.
- **Writing Adapters nav** points to directory, not missing `.md` file.

### Changed

- **Documentation expanded.** Added tagline, "Why rypipe", "What rypipe is not" sections. Fixed 6 audit findings.

## [0.2.0] - 2026-09-02

### Added

- **SIMD scan module with runtime dispatch.** `scan/` module with AVX2/SSE NEON detection; the engine picks the fastest path at runtime.
- **Tier ladder in core.** Performance tiers (`S5` through `S10`) behind `bench` feature for benchmarking individual optimizations.
- **`Splitter` default method + `SkipRegionFinder` + chunk floor.** `find_split_points` has a complete default implementation. `SkipRegionFinder` defines byte ranges that must not be split on. `plan_chunk_count` enforces a minimum chunk size.
- **`dict.rs` incremental dictionary unification.** Builds dictionary-encoded columns incrementally across chunks.
- **Parallel streaming + `BatchConsumer` + `StreamingBatchIterator`.** True parallel bounded-memory streaming via `read_path_stream_par`.
- **`put_field_at` + `expect_slot`/`record_slot`/`layout_broken` on `ColumnarSink`.** Slot-based fast path for adapters that verify field identity via `expect_slot` + memcmp.
- **`PrimColumn<T>` + validity bitmap.** Replaces `Vec<Option<T>>` with packed storage and a bitpack validity array.
- **`BlockMasks` engine asset.** Precomputed bitmasks for fast column membership testing.
- **Frozen schema + parallel discovery.** `FrozenSchema` resolves column order and types once; parallel discovery scans multiple windows concurrently.
- **64KB streaming via `BatchConsumer` + `StreamingBatchIterator`.** True streaming at 64KB memory budget with dynamic `bytes_used` and `split_off`.
- **`Cow`-based `Value::Str`.** `Value::Str(Cow<'a, str>)` borrows from input bytes, closing filtered-path use-after-free.
- **Advanced docs section.** 2000+ lines covering architecture, fusion, streaming, profiling, parallelism, memory, I/O tuning, schema, stage protocol, source patterns, adapter design, case studies.

### Fixed

- **`plan_chunk_count` hardcoded chunks.** Was hardcoded to 1; now uses `max_chunks`.
- **`in_skip_region` overhead.** Uses `memchr::memmem::find` directly instead of `Finder::new`.
- **Clippy warnings.** Resolved all `clippy -D warnings` for CI.
- **`starts_with` out-of-bounds panic.** Fixed bounds check.
- **Parallel stream channel types.** Corrected channel type parameters.
- **Reorder buffer overflow fallback.** Falls back to unordered delivery when the buffer overflows.
- **`reset_child_ordinal`** syncs adapter/engine ordinal counters.

### Changed

- **`engine.rs` (2392 lines) split into `engine/` module.** Modular structure for maintainability.
- **Arrow bumped to 59.2.0**, pyo3 to 0.29.
- **Documentation rewritten.** Architecture (2000 lines) and writing-adapters (7 pages) fully rewritten.

## [0.1.1] - 2026-08-23

### Fixed

- **crates.io metadata and trusted publishing.** Added `LICENSE`, `README`, and `description` to `Cargo.toml` for crates.io publishing.

## [0.1.0] - 2026-08-23

### Added

- **Format-agnostic columnar engine.** Core `rypipe-core` crate with `Splitter`, `RecordParser`, `ColumnarSink` traits, `Pipeline` high-level API, `ExecutionPlan` builder, and Arrow export.
- **Crystal Reports XML adapter (crxml).** Reference adapter demonstrating the full trait implementation.
- **PyO3 bindings (`rypipe-python`).** `execution_plan_from_kwargs`, `record_batches_to_pyarrow_table`, `record_batch_to_pyarrow`, exception types (`ParseError`, `XmlError`, `PlanError`, `MergeError`).
- **Public `rypipe` package.** `Source`, `Pipeline`, stages (`RenameFields`, `DropFields`, `CastTypes`, `FilterRows`), sinks (`collect`, `to_arrow`, `to_pandas`, `to_polars`, `to_parquet`, `to_csv`), `read()`, `read_par()`, `read_stream()`, `read_batches()`, `iter_record_batches()`, `register_adapter()`, `resolve_engine()`.
- **`Adapter` base class.** One-method source creation: subclass `Adapter`, implement `read()`, get caching and pipelines for free.
- **abi3 wheel builds.** Stable ABI wheels for Python 3.10+.
- **aarch64 manylinux cross-compilation** with zig.
- **Zensical docs site.** Tutorial, building-adapters guide, architecture, advanced topics, reference pages.
- **Benchmark harness.** Throughput benchmarks for the columnar engine.
