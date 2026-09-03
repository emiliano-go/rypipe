# The ColumnarSink Trait

`ColumnarSink` is the bridge between your parser and the engine. The parser
calls `begin_row`/`put_field`/`end_row` for each record; the sink accumulates
values into typed Arrow columns.

See [Decoder API](../architecture/decoder.md) for how the
engine implements this trait internally.

## Method reference (21 methods)

### Required (4)

| Method | Signature | Purpose |
|--------|-----------|---------|
| `begin_row` | `fn begin_row(&mut self)` | Start a new row. Clears per-row state. |
| `put_field` | `fn put_field(&mut self, name: &str, value: Value<'_>)` | Push a field value. `name` is resolved via `resolve_field`. |
| `end_row` | `fn end_row(&mut self)` | End the row. Null-fills missing columns, evaluates filter. |
| `finish` | `fn finish(&mut self) -> Result<RecordBatch>` | Finalize into Arrow. Called once after all rows. |

### Field resolution (4)

| Method | Default | Purpose |
|--------|---------|---------|
| `wants` | `true` | Check if field should be kept (not dropped). |
| `resolve` | identity | Map raw name to output column name, or `None` if dropped. |
| `put_field_resolved` | delegates to `put_field` | Push with pre-resolved name (skips rename lookup). |
| `resolve_and_put` | resolve then put_field_resolved | Combined resolve + push (single hash probe). |

### Tier control (3)

| Method | Default | Purpose |
|--------|---------|---------|
| `needs_value` | `true` | `false` = locate-only mode (skip text extraction). |
| `needs_resolve` | `true` | `false` = traverse-only mode (skip resolve). |
| `row_rejected` | `false` | `true` = filter rejected this row; scanner byte-jumps to row close. |

### Projection (3)

| Method | Default | Purpose |
|--------|---------|---------|
| `row_satisfied` | `false` | `true` = all wanted columns have values; scanner byte-jumps to row close. |
| `wanted_mask` | `0` | Bitmask of wanted columns. `(mask >> slot) & 1` replaces per-field `wants()`. |
| `reset_child_ordinal` | no-op | Reset ordinal counter after row-tag attributes. |

### Layout prediction (4)

| Method | Default | Purpose |
|--------|---------|---------|
| `expect_slot` | `None` | `(slot, raw_name_bytes)` for ordinal. Skip attribute scan + hash on match. |
| `put_field_at` | no-op | Push directly to slot index (no name resolution). |
| `record_slot` | no-op | Cache slot resolution for subsequent rows. |
| `layout_broken` | no-op | Invalidate cached layout on mismatch. |

### Batch (1)

| Method | Default | Purpose |
|--------|---------|---------|
| `put_row` | iterates `put_field` | Push a complete row in one call. |

## Fast paths in order

The engine provides four push methods, from fastest to slowest:

### 1. `put_field_at(slot, value)`: fastest

Direct slot push. No name resolution, no hash lookup. Used by the
`expect_slot` path after the layout is learned.

```
expect_slot(ordinal) → Some((slot, expected))
  memcmp(raw, expected) == 0  →  put_field_at(slot, value)
```

**Cost:** ~5 ns per field (column write + dirty bit set).

### 2. `put_field_resolved(name, value)`: fast

Skips the rename lookup. Used when you've already called `resolve()`.

```
resolve(name) → Some(resolved)
  put_field_resolved(resolved, value)
```

**Cost:** ~10 ns per field (single HashMap lookup + column write).

### 3. `resolve_and_put(name, value)`: medium

Single resolve + push. Default implementation.

```
resolve(name) → Some(resolved)
  put_field_resolved(resolved, value)
```

**Cost:** ~15 ns per field (HashMap lookup + column write).

### 4. `put_field(name, value)`: slowest

Full resolve + push. The engine calls `resolve_field(name)` which checks
rename map, then drop set, then returns the output name.

**Cost:** ~20 ns per field (two HashMap lookups + column write).

## The projection fast path

When a projection selects 3 of 11 columns and all 3 arrive by field 4, the
scanner can byte-jump to the row close tag, skipping fields 5-11.

```
sink.row_satisfied()  →  true  →  scanner calls find_row_close()
```

This is implemented in the scanner as:

```rust
// After each child element:
if sink.row_satisfied() {
    let after = find_row_close(bytes, cur, row_tag, regions);
    sink.end_row();
    return Flow::At(after);
}
```

**Composes with `row_rejected()`** (filter rejection). A row can be
satisfied OR rejected; both short-circuit the scanner.

**`wanted_mask()`** provides the bitmask for this:

```rust
fn wanted_mask(&self) -> u64 {
    // Bitmask of columns in the output schema
    let mut mask = 0u64;
    for name in &self.plan.schema_order {
        if let Some(&idx) = self.field_index.get(name) {
            if idx < 64 { mask |= 1u64 << idx; }
        }
    }
    mask
}
```

The adapter checks `(mask >> slot) & 1 == 1` instead of calling `wants()`
per field.

## The layout prediction fast path

After the first row, the engine knows which slot each ordinal maps to.
`expect_slot(ordinal)` returns `(slot, raw_name_bytes)`. The adapter compares
raw bytes via memcmp instead of running the full attribute scan → UTF-8 decode
→ hash → lookup pipeline.

```
expect_slot(ordinal)  →  Some((slot, expected))
  memcmp(raw, expected) == 0  →  put_field_at(slot, value)  // skip decode + resolve
  memcmp(raw, expected) != 0  →  layout_broken(ordinal)     // invalidate, fall back
```

**Cost comparison:**
- Generic path: ~25 ns (find_attr_value + decode_attr + resolve + put_field)
- Fast path: ~8 ns (memcmp + put_field_at)

**When it helps:** Formats with stable field order across rows (CSV columns,
XML attributes, JSONL keys). The first row learns the layout; subsequent rows
skip the expensive resolution.

**When it doesn't help:** Formats where field order changes between rows, or
where field names contain entities that need decoding before comparison.

## The predicate-first fast path

When a filter is active, the engine buffers fields until the predicate resolves.
If the predicate passes mid-row, the engine switches to direct mode and drains
the buffer.

```
begin_row → [put_field × N] → end_row
                │
                ▼
         check predicate slot
         ├── Pass → direct mode (push remaining fields directly)
         ├── Fail → discard buffer, skip row
         └── Undecided → continue buffering
```

The scanner checks `sink.row_rejected()` after each child element:

```rust
if sink.row_rejected() {
    let after = find_row_close(bytes, cur, row_tag, regions);
    sink.end_row();
    return Flow::At(after);
}
```

**Adaptive strategy:** If the predicate column appears late (> 4/5 of columns),
buffering is a net loss. The engine switches to direct push + pop-on-reject.

## Example: minimal sink

```rust
struct CountingSink {
    rows: usize,
    fields: usize,
}

impl ColumnarSink for CountingSink {
    fn begin_row(&mut self) {}
    fn put_field(&mut self, _name: &str, _value: Value<'_>) { self.fields += 1; }
    fn end_row(&mut self) { self.rows += 1; }
    fn finish(&mut self) -> Result<RecordBatch> {
        Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
    }
}
```

## Example: profiling sink (locate-only)

```rust
struct LocateOnlySink {
    row_count: usize,
    field_count: usize,
    plan: ExecutionPlan,
}

impl ColumnarSink for LocateOnlySink {
    fn begin_row(&mut self) {}
    fn put_field(&mut self, name: &str, _value: Value<'_>) {
        self.field_count += 1;
        // Only resolve, don't store
        let _ = self.plan.resolve_field(name);
    }
    fn end_row(&mut self) { self.row_count += 1; }
    fn wants(&self, _name: &str) -> bool { true }
    fn needs_value(&self) -> bool { false }  // Skip text extraction
    fn resolve<'a>(&'a self, name: &'a str) -> Option<&'a str> {
        self.plan.resolve_field(name)
    }
    fn finish(&mut self) -> Result<RecordBatch> {
        Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
    }
}
```

## Thread safety

`ColumnarSink` is `Send` but not `Sync`. Each chunk gets its own sink
instance via `begin_row`/`end_row` lifecycle. The engine creates one
`TableBuilder` per chunk and merges them after all chunks complete.
