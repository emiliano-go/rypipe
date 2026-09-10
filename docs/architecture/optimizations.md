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
one hash, a frozen-schema check, a strict-types check, an observer
notification, first-row ordinal tracking, one bit set, one conditional
pop, and one push_value.

**Example:** the adapter-visible hot path, one hash probe per field:

```rust
use rypipe_core::{ColumnarSink, TableBuilder, Value};

fn feed_row(sink: &mut TableBuilder) {
    sink.begin_row();
    sink.put_field("id", Value::Int64(7));          // 1 hash: field_index
    sink.put_field("name", Value::Str("acme".into()));
    sink.end_row();                                 // finish_row: dirty-bitmask pass
}
```

**Impact:** Eliminates the second hash probe for every field. For 10 fields
per row at ~10 ns per hash, this saves ~100 ns per row. On a 533 MB file
with ~480K rows, that is ~48 ms saved.

## 2. Dirty bitmask null-fill (2C-S2) { #2-dirty-bitmask-null-fill }

**Before:** For each row, loop over all columns:
```rust
for b in &mut columns {
    while b.len() < target { b.push(None); }
}
```
This pushed `None` for every missing column, even when most columns were
present. For 10 columns where 8 are present, 2 null pushes per row.

**After:** `row_dirty: Vec<u64>` bitmask. In `finish_row`, a dense-row
`is_full` check (every dirty bit set) skips the per-column loop entirely;
otherwise only missing columns get null-filled.
**Example:**

```rust
if is_full {
    self.row_dirty.fill(0);  // Dense row: no loop at all
} else {
    for (i, b) in columns.iter_mut().enumerate() {
        let word = i / 64;
        let bit = i % 64;
        if (row_dirty[word] >> bit) & 1 == 0 {
            b.push(None);  // Only missing columns get null-filled
        }
    }
    self.row_dirty.fill(0);  // Clear all bits for next row
}
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

**Example:** selective filter via crxml (rejected rows are discarded before
any column push):

```python
import crxml

src = crxml.CrystalXMLSource("report.xml", row_tag="Row", engine="parallel")
df = src | crxml.FilterRows(field="status", op="==", value="active") | crxml.to_pandas()
```

**Impact:** For selective filters (e.g., 10% selectivity), eliminates 90%
of column push/pop cycles. The adaptive strategy disables buffering when
the predicate column's slot is at or beyond 4/5 of the column count
(`ordinal >= ncols * 4 / 5`), falling back to direct
push + pop-on-reject.

## 4. resolve + put_field_resolved (single hash) { #4-resolve-put_field_resolved }

**Before:** `wants(name)` + `put_field(name, v)` = two hash probes (one
in `wants` via `resolve_field`, one in `put_field` via `ensure_column_idx`).

**After:** `resolve(name)` + `put_field_resolved(resolved, v)` = one hash
probe. `resolve` does the rename/drop lookup; `put_field_resolved` does
the column lookup with the already-resolved name.

**Example:** adapter call pattern (one hash probe instead of two):

```rust
// Instead of: if sink.wants(name) { sink.put_field(name, value); }
if let Some(resolved) = sink.resolve(name) {
    let owned = resolved.to_owned(); // end the borrow on sink
    sink.put_field_resolved(&owned, value);
}
```

**Impact:** Saves one HashMap lookup per field. For 10 fields per row,
saves ~100 ns per row.

## 5. expect_slot layout prediction { #5-expect_slot-layout-prediction }

**Before:** Every field: `find_attr_value` (memchr scan) + `decode_attr`
(UTF-8 + entity unescape) + `resolve` (HashMap lookup) + `put_field`.

**After:** After first row, `expect_slot(ordinal)` returns `(slot,
raw_name_bytes)`. Adapter does memcmp (8-16 bytes, single SIMD compare),
then `put_field_at(slot, value)`.

**Example:** per-field fast path after row 1:

```rust
if let Some((slot, expected_name)) = sink.expect_slot(ordinal) {
    if raw_name_bytes == expected_name { // one memcmp, no scan/decode/hash
        sink.put_field_at(slot, value);
        continue;
    }
}
// memcmp miss: fall back to the generic attribute-scan path
```

**Impact:** Skips attribute scan, UTF-8 decode, hash lookup. ~25 ns → ~8 ns
per field. ~17% on the hot path. Works for formats with stable field order
(CSV, XML, JSONL).

## 6. row_satisfied projection short-circuit { #6-row_satisfied-projection-short-circuit }

**Before:** Scan all fields even when only 3 of 11 are wanted.

**After:** `row_satisfied()` returns true when all wanted columns have
values. Scanner byte-jumps to row close via `find_close_after` with the
precomputed close finder (see section 9).

**Example:** projection via crxml (scanner byte-jumps to the row close once
all wanted fields arrived):

```python
import crxml

src = crxml.CrystalXMLSource("report.xml", row_tag="Row", engine="parallel")
df = src | crxml.DropFields(["notes", "audit_trail", "meta_json"]) | crxml.to_pandas()
```

**Impact:** For projections, skips scanning 60-80% of fields. Measured:
+123% on drop_half parallel (533 MB: 3,394 → 7,571 MB/s).

## 7. wanted_mask bitmask projection { #7-wanted_mask-bitmask-projection }

**Before:** `sink.wants(name)` virtual call per field (vtable dispatch).

**After:** `(wanted_mask >> slot) & 1`: single bit test, no vtable dispatch.

**Example:** per-field membership test in the parse loop:

```rust
let mask = sink.wanted_mask(); // precomputed once, 0 when no projection
if mask != 0 && (mask >> slot) & 1 == 0 {
    continue; // not wanted: skip extraction entirely
}
```

**Impact:** Eliminates virtual dispatch overhead in the hot inner loop.
Combined with row_satisfied, enables full projection optimization.

## 8. Ordinal threading { #8-ordinal-threading }

**Before:** `parse_row` doesn't track field ordinals.

**After:** Ordinal counter threads through `parse_row` → `scan_child` →
`field_element`. Enables `expect_slot` (layout prediction) and
`row_satisfied` (projection short-circuit).

**Example:** the counter threads through the parse recursion:

```rust
// the caller advances `ordinal` per field; it threads parse_row ->
// scan_child -> field_element so each field knows its position
fn field_element<S: ColumnarSink>(sink: &mut S, ordinal: u32,
                                  raw_name: &[u8], value: Value<'_>) {
    if let Some((slot, expected)) = sink.expect_slot(ordinal) {
        if raw_name == expected {
            sink.put_field_at(slot, value); // layout fast path
            return;
        }
    }
    // generic path: scan + decode + resolve + put_field
}
```

**Impact:** Enables optimizations 5 and 6. ~3% overhead for the counter
increment, but the savings from 5 and 6 far outweigh it.

## 9. Precomputed close finder (F1) { #9-precomputed-close-finder }

**Before:** `find_row_close` allocates `Vec<u8>` + `memmem::Finder` per
rejected or satisfied row. With `row_satisfied`, this ran on every row
in projection workloads.

**After:** Precomputed once in `scan_chunk`, passed to `parse_row` as a
reference. Zero allocation per row.

**Example:** precomputed once in scan_chunk, reused per row:

```rust
use rypipe_core::scan::find_literal;

let close_finder = memchr::memmem::Finder::new(b"</Row>"); // once per chunk
// per satisfied or rejected row: zero allocation
if let Some(close) = find_literal(bytes, pos, &close_finder) {
    pos = close + b"</Row>".len();
}
```

**Impact:** Eliminates per-row allocation. For 480K rows, saves 480K
Vec allocations + Finder constructions. Measured: +10% single-thread,
+9% parallel.

## 10. Scan primitives (S5) { #10-scan-primitives }

**Before:** Raw `memchr` calls without fast path.

**After:** `scan::find(hay, from, b)` checks `hay[from] == b` first (O(1)),
then delegates to memchr. `scan::find2` for dual-byte searches with the
same fast path.

**Example:** the `next_lt` hot path:

```rust
use rypipe_core::scan::{find, find2};

if let Some(lt) = find(bytes, pos, b'<') {
    // bytes[pos] == b'<': O(1) hit, AVX2 prologue skipped
}
let (i, hit) = find2(bytes, pos, b'<', b'"')?; // dual-byte, same fast path
```

**Impact:** 15% on the `next_lt` hot path (byte-at-position check avoids
AVX2 prologue on 2/3 of calls).

## 11. Engine-provided Splitter default (S1) { #11-engine-provided-splitter-default }

**Before:** Adapters implement `find_split_points` from scratch, which is a
recurring source of performance bugs: sampling too little data causes
sub-MB chunk collapse, sampling too much scans far more of the file than
needed.

**After:** Default `find_split_points` uses `next_record_start` + rayon +
skip-region rejection + dedup + chunk floor (2 MiB minimum).

**Example:** a minimal Splitter gets the tuned default for free:

```rust
use rypipe_core::Splitter;

struct RowTagSplitter;

impl Splitter for RowTagSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        memchr::memmem::find(&bytes[from..], b"<Row>").map(|p| from + p)
    }
    fn estimate_bytes_per_row(&self, _sample: &[u8]) -> usize {
        2048 // coarse hint, only used for chunk sizing
    }
}
// find_split_points comes free: rayon + skip-region rejection + 2 MiB floor.
let points = RowTagSplitter.find_split_points(bytes, 16);
```

**Impact:** Eliminates the bug class: +13-32% on projection workloads.

## 12. Incremental dictionary unification { #12-incremental-dictionary-unification }

**Before:** `auto_dict=True` forces serial merge path (no fast path).
All chunks must be merged before dict upgrade.

**After:** Per-chunk upgrade in parallel, then `unify_dictionaries` +
`remap_codes` (O(dict_size), not O(rows)).

**Example:** the user-facing trigger:

```python
import crxml

src = crxml.CrystalXMLSource(
    "report.xml", row_tag="Row", engine="parallel", auto_dict=True
)
df = src.to_pandas()  # low-cardinality strings become dictionary columns
```

**Impact:** auto_dict parallel gap: 45% to 16%.

## Summary table { #summary-table }

| # | Optimization | Measured gain | Where |
|---|-------------|---------------|-------|
| 1 | Dense column storage | Eliminates 1 hash probe/field | engine/table_builder.rs |
| 2 | Dirty bitmask null-fill | 80% fewer null pushes | engine/table_builder.rs |
| 3 | Predicate-first | 90% fewer push/pop cycles | engine/table_builder.rs |
| 4 | Single hash resolve | 100 ns/row saved | engine/table_builder.rs |
| 5 | expect_slot layout | 17% on hot path | decoder.rs, scanner.rs |
| 6 | row_satisfied | +123% on drop_half | decoder.rs, scanner.rs |
| 7 | wanted_mask | Eliminates vtable dispatch | decoder.rs |
| 8 | Ordinal threading | Enables 5 and 6 | decoder.rs, scanner.rs |
| 9 | Precomputed close finder | Eliminates per-row alloc | scanner.rs |
| 10 | Scan primitives | 15% on next_lt | scan/mod.rs |
| 11 | Splitter default | Eliminates bug class | decoder.rs |
| 12 | Incremental dicts | 45% → 16% gap | parallel.rs |
