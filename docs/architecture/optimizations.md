# Optimizations { #optimizations }

Every optimization in `rypipe-core`, why it matters, and what it replaces.
These changes are not format-specific; they benefit every adapter equally.

## 1. Dense column storage (2C-S1) { #1-dense-column-storage }

**Before:** `HashMap<String, ColumnBuilder>` required two hash probes per
field: one in `ensure_column` (create if missing), one in `get_mut` (push
value). For 10 fields per row, this was 20 HashMap operations per row.

**After:** `Vec<ColumnBuilder>` + `field_index: HashMap<String, usize>`.
One hash probe (name → index via `field_index.get`), then `columns[idx]`
is a bounds-checked array access. The `push_field_resolved` hot path does
one hash, one bit set, one conditional pop, and one push_value.

**Impact:** Eliminates the second hash probe for every field. For 10 fields
per row at ~10 ns per hash, this saves ~100 ns per row. On a 533 MB file
with ~480K rows, that's ~48 ms saved.

## 2. Dirty bitmask null-fill (2C-S2) { #2-dirty-bitmask-null-fill }

**Before:** For each row, loop over all columns:
```rust
for b in &mut columns {
    while b.len() < target { b.push(None); }
}
```
This pushed `None` for every missing column, even when most columns were
present. For 10 columns where 8 are present, 2 null pushes per row.

**After:** `row_dirty: Vec<u64>` bitmask. In `finish_row`:
```rust
for (i, b) in columns.iter_mut().enumerate() {
    let word = i / 64;
    let bit = i % 64;
    if (row_dirty[word] >> bit) & 1 == 0 {
        b.push(None);  // Only missing columns get null-filled
    }
}
self.row_dirty.fill(0);  // Clear all bits for next row
```

**Impact:** For 10 columns where 8 are present, saves 80% of null-fill pushes.
The bitmask word load plus bit test is cheaper than a `Vec` push per column.
Measured: 34% reduction in `finish_row` cost.

## 3. Predicate-first deferred materialization { #3-predicate-first-deferred-materialization }

**Before:** Parse all fields into columns, then evaluate filter. If rejected,
pop all columns (expensive for wide tables with many fields).

**After:** Buffer `(slot, Value<'static>)` pairs in `RowBuffer`. Evaluate
predicate as soon as the predicate column arrives. On Fail, discard buffer
(no pops). On Pass, switch to direct mode and drain buffer to columns.

**Impact:** For selective filters (e.g., 10% selectivity), eliminates 90%
of column push/pop cycles. The adaptive strategy disables buffering when
the predicate column is late (> 4/5 of columns), falling back to direct
push + pop-on-reject.

## 4. resolve + put_field_resolved (single hash) { #4-resolve-put_field_resolved }

**Before:** `wants(name)` + `put_field(name, v)` = two hash probes (one
in `wants` via `resolve_field`, one in `put_field` via `ensure_column_idx`).

**After:** `resolve(name)` + `put_field_resolved(resolved, v)` = one hash
probe. `resolve` does the rename/drop lookup; `put_field_resolved` does
the column lookup with the already-resolved name.

**Impact:** Saves one HashMap lookup per field. For 10 fields per row,
saves ~100 ns per row.

## 5. expect_slot layout prediction { #5-expect_slot-layout-prediction }

**Before:** Every field: `find_attr_value` (memchr scan) + `decode_attr`
(UTF-8 + entity unescape) + `resolve` (HashMap lookup) + `put_field`.

**After:** After first row, `expect_slot(ordinal)` returns `(slot,
raw_name_bytes)`. Adapter does memcmp (8-16 bytes, single SIMD compare),
then `put_field_at(slot, value)`.

**Impact:** Skips attribute scan, UTF-8 decode, hash lookup. ~25 ns → ~8 ns
per field. ~17% on the hot path. Works for formats with stable field order
(CSV, XML, JSONL).

## 6. row_satisfied projection short-circuit { #6-row_satisfied-projection-short-circuit }

**Before:** Scan all fields even when only 3 of 11 are wanted.

**After:** `row_satisfied()` returns true when all wanted columns have
values. Scanner byte-jumps to row close via `find_row_close`.

**Impact:** For projections, skips scanning 60-80% of fields. Measured:
+123% on drop_half parallel (533 MB: 3,394 → 7,571 MB/s).

## 7. wanted_mask bitmask projection { #7-wanted_mask-bitmask-projection }

**Before:** `sink.wants(name)` virtual call per field (vtable dispatch).

**After:** `(wanted_mask >> slot) & 1`: single bit test, no vtable dispatch.

**Impact:** Eliminates virtual dispatch overhead in the hot inner loop.
Combined with row_satisfied, enables full projection optimization.

## 8. Ordinal threading { #8-ordinal-threading }

**Before:** `parse_row` doesn't track field ordinals.

**After:** Ordinal counter threads through `parse_row` → `scan_child` →
`field_element`. Enables `expect_slot` (layout prediction) and
`row_satisfied` (projection short-circuit).

**Impact:** Enables optimizations 5 and 6. ~3% overhead for the counter
increment, but the savings from 5 and 6 far outweigh it.

## 9. Precomputed close finder (F1) { #9-precomputed-close-finder }

**Before:** `find_row_close` allocates `Vec<u8>` + `memmem::Finder` per
rejected or satisfied row. With `row_satisfied`, this ran on every row
in projection workloads.

**After:** Precomputed once in `scan_chunk`, passed to `parse_row` as a
reference. Zero allocation per row.

**Impact:** Eliminates per-row allocation. For 480K rows, saves 480K
Vec allocations + Finder constructions. Measured: +10% single-thread,
+9% parallel.

## 10. Scan primitives (S5) { #10-scan-primitives }

**Before:** Raw `memchr` calls without fast path.

**After:** `scan::find(hay, from, b)` checks `hay[from] == b` first (O(1)),
then delegates to memchr. `scan::find2` for dual-byte searches with the
same fast path.

**Impact:** 15% on the `next_lt` hot path (byte-at-position check avoids
AVX2 prologue on 2/3 of calls).

## 11. Engine-provided Splitter default (S1) { #11-engine-provided-splitter-default }

**Before:** Adapters implement `find_split_points` from scratch. Two adapters
got it wrong: TSV collected first K newlines (negative scaling), crxml
scanned entire file for `<!` (25% overhead).

**After:** Default `find_split_points` uses `next_record_start` + rayon +
skip-region rejection + dedup + chunk floor (2 MiB minimum).

**Impact:** Eliminates the bug class. +13-32% on projection workloads.

## 12. Incremental dictionary unification { #12-incremental-dictionary-unification }

**Before:** `auto_dict=True` forces serial merge path (no fast path).
All chunks must be merged before dict upgrade.

**After:** Per-chunk upgrade in parallel, then `unify_dictionaries` +
`remap_codes` (O(dict_size), not O(rows)).

**Impact:** auto_dict parallel gap: 45% → 16%.

## Summary table { #summary-table }

| # | Optimization | Measured gain | Where |
|---|-------------|---------------|-------|
| 1 | Dense column storage | Eliminates 1 hash probe/field | engine.rs |
| 2 | Dirty bitmask null-fill | 80% fewer null pushes | engine.rs |
| 3 | Predicate-first | 90% fewer push/pop cycles | engine.rs |
| 4 | Single hash resolve | 100 ns/row saved | engine.rs |
| 5 | expect_slot layout | 17% on hot path | scanner.rs |
| 6 | row_satisfied | +123% on drop_half | scanner.rs |
| 7 | wanted_mask | Eliminates vtable dispatch | scanner.rs |
| 8 | Ordinal threading | Enables 5 and 6 | scanner.rs |
| 9 | Precomputed close finder | Eliminates per-row alloc | scanner.rs |
| 10 | Scan primitives | 15% on next_lt | scan/mod.rs |
| 11 | Splitter default | Eliminates bug class | decoder.rs |
| 12 | Incremental dicts | 45% → 16% gap | parallel.rs |
