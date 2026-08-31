# Optimizations

This page lists every optimization that makes `rypipe` fast for all adapters, not just one format. Each entry gives the file and line, what changed, why it matters, and what it replaces.

## 1. Vec plus map column storage (engine.rs:16, plan.rs:280, merge.rs:30, parallel.rs:101)

**Before:** `columns: HashMap<String, ColumnBuilder>` with `HashMap::get_mut` per field. `push_field` did `ensure_column` (`contains_key` plus `insert`) then `get_mut` (second hash) : 2 hashes per field in steady state.

**After:** `columns: Vec<ColumnBuilder>` plus `field_index: HashMap<String, usize>` and `column_order: Vec<String>`. `ensure_column_idx` does `field_index.get(name)` (one hash) and returns `idx`; `push_field_resolved` then does `&mut columns[idx]` (array index). Hot path is one hash plus one indexed load. `take_column` keeps the three vectors in sync via `swap_remove` with index patching.

**Why generic:** every `RecordParser` calls `put_field` for every field of every row. The `Pipeline` hot loop is `validate` plus `parse_chunk` plus `push_value` plus this dispatch. Fewer hashes helps TSV at 5 fields per row as much as XML at 20 fields.

## 2. Single lookup push (engine.rs:202)

**Before:** `ensure_column` plus `get_mut` : double hash as above, plus `schema_insert_index` inside `ensure_column` scanned `column_order` even when the column already existed.

**After:** `ensure_column_idx` is the single lookup. `push_field_resolved` marks `row_dirty[idx] = true` and handles last write wins with one `len > row_count` check. `schema_insert_index` is only called when the column is new. The old `ensure_column` (void) is removed; `push_field` delegates to `push_field_resolved` after one `resolve_field`.

**Why not `entry` API:** `HashMap::entry` would also be one hash, but `Vacant` insertion still needs `schema_insert_index` which borrows `self` mutably while `entry` holds a borrow. The `get` plus `insert` split avoids the borrow checker fight and keeps the fast path as a pure `get` without allocating the key.

## 3. Dirty bitmask for finish_row (engine.rs:563)

**Before:** `for b in &mut columns { while b.len() < target { b.push(None) } }` iterated all columns and pushed `None` for missing ones, checking `len < target` for every column.

**After:** `row_dirty: Vec<u64>` is a bitmask with `(columns.len() + 63) / 64` words. `push_field_resolved` sets the bit for column `i` via `row_dirty[i / 64] |= 1u64 << (i % 64)` on first touch of the row. `finish_row` checks each bit: if clear, pushes `None`; if set, clears the bit. Only missing columns get a push; touched columns are just cleared. `reset` and `normalize` clear the mask, `take_column` keeps it in sync.

**Impact:** for 10 columns where 8 are present, this saves 80% of the `push(None)` calls and the associated `len` loads. The loop iterates over bitmask words and checks each bit; a bit test plus branch is cheaper than a `Vec` push.

## 4. Resolve plus put_field_resolved (decoder.rs:65, engine.rs:542)

**Before:** adapters did `if sink.wants(k) { /* decode */ sink.put_field(k, v) }` which called `ExecutionPlan::resolve_field` twice: once in `wants` (hash `field_map` then `drop_fields`) and once in `put_field`. When `field_map` or `drop_fields` is non empty, this is two hashes per field. `push_field` also did `owned = n.to_owned()` to hold the resolved name.

**After:** `ColumnarSink` has `resolve<'a>(&'a self, name: &'a str) -> Option<&'a str>` and `put_field_resolved(&mut self, resolved, value)`. `TableBuilder` implements `resolve` as `self.plan.resolve_field(name)` (borrows from `field_map` when renamed) and `put_field_resolved` as `push_field_resolved` (no re hash). Adapters can do `if let Some(r) = sink.resolve(k) { /* expensive decode */ sink.put_field_resolved(r, v) }` with one hash total. `wants` now delegates to `resolve` so old code still gets the single hash path.

**Why not `wants_resolved`:** the pair `resolve` plus `put_field_resolved` is the minimal extension that keeps the trait backward compatible (defaults delegate to `put_field`, which re resolves). Adapters choose the fast path only when extraction is expensive; stringly adapters like `LineParser` keep the simple `wants` plus `put_field` path.

## 5. Filter check with Vec plus index (plan.rs:280)

**Before:** `FilterPredicate::check(&self, columns: &HashMap<String, ColumnBuilder>, ...)` did `columns.get(resolve(field, plan))` which hashed the resolved name per filter field per row.

**After:** `check(&self, columns: &[ColumnBuilder], field_index: &HashMap<String, usize>, ...)` does `get_column(columns, field_index, resolve(field, plan))` which is `field_index.get(name).map(|&i| &columns[i])` : one hash per leaf, shared with the column dispatch hash, and no clone of `String` keys. `get_value` and `Compare` arms use the same helper.

**Why generic:** every row that has a filter pays this per row. `Equal`/`NotEqual` plus `Compare` plus `And`/`Or`/`Not` trees all use it. Vectorizing `Equal`/`NotEqual` after `finish` would be possible (see `arrow_export`), but per row is authoritative for short circuiting and missing field semantics, so the Vec path is the right intermediate.

## 6. Unified schema with promotion (columnar.rs:1043, merge.rs:30)

**Before:** mixed `int64` plus `float64` or `string` plus `dictionary` across chunks was an error or silent first sighting.

**After:** `unify_variants` plus `promote_to_variant` reconcile `int64` plus `float64` to `float64` and `string` plus `dictionary` to `dictionary` before `extend_owned` or `engines_to_record_batches`. Irreconcilable types return `Error::Merge` naming the column and hinting to provide `field_types`. This lets `ParallelExecutor` fast path keep chunked batches with one unified schema instead of forcing the merge path.

## 7. InputBuffer magic and decompression (input.rs:33, Cargo.toml:15)

**Before:** only `Mmap` or `fs::read`, no compression.

**After:** `detect_compression` reads 4 bytes, matches `1f 8b` (gzip), `28 b5 2f fd` (zstd), `04 22 4d 18` (lz4 frame) when the corresponding Cargo feature is enabled (`gzip`, `zstd`, `lz4`, `compress-all`). `open` then returns `Owned(decompress(...))` via `flate2`, `zstd`, or `lz4_flex`. Bytes APIs slice directly; `BoundedExecutor::run` uses `run_bytes` for `Owned` and `run_mapped` (seek plus read) for `Mmap`.

## 8. Bounded executor chunking (bounded.rs:30, pipeline.rs:65)

**Before:** `Pipeline` had only `read_bytes` (single) and `read_path` variants; bounded mode required a file path and did `File::open` plus `seek`.

**After:** `Pipeline::read_bytes_par` and `read_bytes_stream` plus `BoundedExecutor::run_bytes` slice directly from `&[u8]` with no file IO. `plan_chunks` computes `bytes_per_row`, `total_rows_est`, `rows_per_batch`, `num_batches`, caps at `MAX_SPLIT_CHUNKS = 100_000`, then `split_points_to_ranges`. `run_bytes` is used for decompressed buffers and for adapters that hold `BytesIO` data.

## 9. Arrow export and filter reapplication (arrow_export.rs:29, engine.rs:335)

* `StrColumn` arena plus offsets plus validity is block copied to `StringArray` (two buffers).
* Numeric builders are `NullableColumn<T>` with `collect`.
* `apply_compare_filter` only reapplies pure `Compare` and `And` of `Compare` via Arrow compute; other trees are no ops because per row is authoritative. This avoids double null semantics and keeps the fast path correct.

## 10. Small but cumulative

* `estimated_rows` capacity hint (`bytes.len() / 512`, min 64) in `TableBuilder::with_plan` reduces reallocations.
* `lexical::parse` for numbers instead of `str::parse` (faster).
* `simdutf8::basic::from_utf8` for validate.
* `FxHashMap` and `FxHashSet` for field names (short strings).
* `with_capacity` for `StrColumn` arena (`cap * 16` bytes heuristic).
* `ColumnarSink` defaults keep the trait object safe (`&mut dyn ColumnarSink` in `parse_chunk`).

Each of these is 0.5 to 2%, but together they move single thread from ~600 MB/s on the original `HashMap` plus double lookup plus `while len < target` baseline to the current 700 MB/s, and filtered cases by 8 to 12% via `resolve` plus `put_field_resolved`. None of them require `unsafe`; all are behind safe API changes. When safe leaves more than a few percent on the table, the remaining `unsafe` candidates are gated behind `#[cfg(feature="unsafe-fast")]` and require `miri` plus `cargo test --release` proof, per the plan in the repo root.
