# The ColumnarSink Trait { #the-columnarsink-trait }

`ColumnarSink` is the bridge between your parser and the engine. The parser
calls `begin_row`/`put_field`/`end_row` for each record; the sink accumulates
values into typed Arrow columns.

See [Decoder API](../architecture/decoder.md) for how the
engine implements this trait internally.

## Method reference { #method-reference }

### Required { #required }

| Method | Signature | Purpose |
|--------|-----------|---------|
| `begin_row` | `fn begin_row(&mut self)` | Start a new row. Clears per-row state. |
| `put_field` | `fn put_field(&mut self, name: &str, value: Value<'_>)` | Push a field value. `name` is resolved via `resolve_field`. |
| `end_row` | `fn end_row(&mut self)` | End the row. Null-fills missing columns, evaluates filter. |
| `finish` | `fn finish(&mut self) -> Result<RecordBatch>` | Finalize into Arrow. Called once after all rows. |

### Field resolution { #field-resolution }

| Method | Default | Purpose |
|--------|---------|---------|
| `wants` | `true` | Check if field should be kept (not dropped). |
| `resolve` | identity | Map raw name to output column name, or `None` if dropped. |
| `put_field_resolved` | delegates to `put_field` | Push with pre-resolved name (skips rename lookup). |
| `resolve_and_put` | resolve then put_field_resolved | Combined resolve + push (single hash probe). |

### Tier control { #tier-control }

| Method | Default | Purpose |
|--------|---------|---------|
| `needs_value` | `true` | `false` = locate-only mode (skip text extraction). |
| `needs_resolve` | `true` | `false` = traverse-only mode (skip resolve). |
| `row_rejected` | `false` | `true` = filter rejected this row; scanner byte-jumps to row close. |

### Projection { #projection }

| Method | Default | Purpose |
|--------|---------|---------|
| `row_satisfied` | `false` | `true` = all wanted columns have values; scanner byte-jumps to row close. |
| `wanted_mask` | `0` | Bitmask of wanted columns. `(mask >> slot) & 1` replaces per-field `wants()`. |
| `reset_child_ordinal` | no-op | Reset ordinal counter after row-tag attributes. |

### Layout prediction { #layout-prediction }

| Method | Default | Purpose |
|--------|---------|---------|
| `expect_slot` | `None` | `(slot, raw_name_bytes)` for ordinal. Skip attribute scan + hash on match. |
| `put_field_at` | no-op | Push directly to slot index (no name resolution). |
| `record_slot` | no-op | Cache slot resolution for subsequent rows. |
| `layout_broken` | no-op | Invalidate cached layout on mismatch. |

### Batch { #batch }

| Method | Default | Purpose |
|--------|---------|---------|
| `put_row` | iterates `put_field` | Push a complete row in one call. |

The default just loops over `put_field`, so there is nothing to gain unless
you override it. Override only if your parser naturally produces a whole
row at once (a decoded record struct, a fixed-width slot array) and can
push it without per-field branching; otherwise the per-field methods give
the engine more chances to short-circuit (`wants`, `row_rejected`).

## Fast paths in order { #fast-paths-in-order }

The engine provides four push methods, from fastest to slowest. The speed
differences come from skipped lookups, and each skip moves a responsibility
onto your parser: the faster the method, the more preconditions you must
guarantee yourself. The slower methods exist because they are simpler and
robust by construction; a few nanoseconds per field only matter on very hot
paths, so start with `put_field` and move down this list only where
profiling justifies it.

### 1. `put_field_at(slot, value)`: fastest { #1-put_field_at-fastest }

Direct slot push. No name resolution, no hash lookup. Used by the
`expect_slot` path after the layout is learned.

```
expect_slot(ordinal) → Some((slot, expected))
  memcmp(raw, expected) == 0  →  put_field_at(slot, value)
```

**Cost:** ~5 ns per field (column write + dirty bit set).

**Use when:** your format has a fixed field order and the parser sits on a
hot path (millions of rows).

**Constraint:** slot numbers are only meaningful for one specific layout.
You must guard every push with the `expect_slot` memcmp and call
`layout_broken(ordinal)` the moment a row deviates (reordered, missing, or
extra fields), or you silently write values into the wrong columns. See
[Layout prediction](#layout-prediction).

### 2. `put_field_resolved(name, value)`: fast { #2-put_field_resolved-fast }

Skips the rename lookup. Used when you've already called `resolve()`.

```
resolve(name) → Some(resolved)
  put_field_resolved(resolved, value)
```

**Cost:** ~10 ns per field (single HashMap lookup + column write).

**Use when:** extraction is expensive (entity decoding, base64, date
parsing), so you want to check `resolve()` once, skip unwanted fields, and
push without paying the rename/drop lookup a second time.

**Constraint:** the `resolved` name is only valid within the current row
state; do not cache it across rows or schema changes. And if extraction is
cheap, this gains nothing over `resolve_and_put`, since you paid for
`resolve()` anyway.

### 3. `resolve_and_put(name, value)`: medium { #3-resolve_and_put-medium }

Single resolve + push. Default implementation.

```
resolve(name) → Some(resolved)
  put_field_resolved(resolved, value)
```

**Cost:** ~15 ns per field (HashMap lookup + column write).

**Use when:** you want the `resolve()` semantics (rename + drop honored,
unwanted fields skipped) in one call without managing the resolved name
yourself. This is the default for a reason: it is what `put_field`
degenerates to after resolution.

**Constraint:** still one hash lookup per field; on a fixed-layout hot
loop, method 1 or 2 avoids it.

### 4. `put_field(name, value)`: slowest { #4-put_field-slowest }

Full resolve + push. The engine calls `resolve_field(name)` which checks
rename map, then drop set, then returns the output name.

**Cost:** ~20 ns per field (two HashMap lookups + column write).

**Use when:** almost everywhere. No bookkeeping, no layout assumptions, no
validity windows: pass the raw field name and the value and the engine does
the right thing. At ~20 ns per field, a 1M-row, 10-column file spends about
0.2 s of total parse time here, usually a small fraction of actual parsing
work.

**Constraint:** none, which is the point.

## The projection fast path { #the-projection-fast-path }

**Projection** is the relational operation of selecting a subset of columns
from each row and discarding the rest. In rypipe it comes from three sources:
`schema_order` (the output contains exactly the listed columns),
`.drop(...)` / `DropFields` (named columns removed), and filter columns
(parsed for predicate evaluation, then projected out of the output).

**Projection pushdown** means moving that selection to the earliest possible
point in the pipeline: the byte scanner itself. The theoretical win is that
an unwanted column costs nothing at every stage it never reaches: no byte
scanning, no UTF-8 decoding, no hash lookup, no allocation, no Arrow builder
append. The engine exposes this as a chain of increasingly coarse signals:

1. `wants(name)` / `resolve()`: per-field; skip this field's value.
2. `wanted_mask()`: per-row; a bitmask answering `wants()` for all slots at
   once.
3. `row_satisfied()`: per-row short-circuit; once every wanted column in
   the row has a value, the remaining fields can only be unwanted, so the
   scanner may skip them without even reading their names.

The third signal is what makes projection a scanner optimization rather than
just a storage optimization. When a projection selects 3 of 11 columns and
all 3 arrive by field 4, the scanner can byte-jump to the row close tag,
skipping fields 5-11.

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

## The layout prediction fast path { #the-layout-prediction-fast-path }

The generic per-field path does four pieces of work before a value lands in
a column:

```
raw name bytes → UTF-8/entity decode → hash lookup → resolve (rename/drop) → slot
```

The observation behind this fast path: in most row-oriented formats the
**layout is stable**, meaning field number 3 of every row is the same
column. Once one row has been parsed generically, the mapping
`ordinal → (slot, name)` is known, and verifying it costs a single memcmp
instead of the whole pipeline. (An *ordinal* is the field's position within
the row, 0-based: first field is 0, second is 1, and so on.)

The learning side is automatic: every generic `put_field` call records
`(slot, name_bytes)` for its ordinal when no entry exists yet, so the first
row (and any re-learn after an invalidation) costs nothing extra. Your
adapter only opts into *using* the mapping. For each field:

1. Ask `sink.expect_slot(ordinal)`. `None` means "nothing learned yet":
   take the generic path, which learns the entry as a side effect.
2. `Some((slot, expected))`: memcmp the field's raw name bytes against
   `expected`.
3. Match: push with `sink.put_field_at(slot, value)`. No decode, no hash,
   no resolve.
4. Mismatch: call `sink.layout_broken(ordinal)` to drop the stale entry,
   then fall through to the generic path, which re-learns the ordinal from
   this row's actual name.

A complete adapter loop:

```rust
fn parse_row(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    sink.begin_row();
    let mut ordinal = 0u32;
    for (raw_name, raw_value) in self.scan_fields(bytes) {
        // Fast path: the engine cached a slot for this ordinal and the
        // raw name bytes match. Skip decode + resolve entirely.
        if let Some((slot, expected)) = sink.expect_slot(ordinal) {
            if raw_name == expected {
                sink.put_field_at(slot, Value::Str(Cow::Borrowed(raw_value)));
                ordinal += 1;
                continue;
            }
            // Layout changed (reordered, missing, or extra field).
            // Invalidate so the generic path below re-learns the ordinal.
            sink.layout_broken(ordinal);
        }
        // Generic path: decode + resolve + push. Also (re)learns the
        // ordinal → (slot, name) mapping as a side effect.
        let name = decode_name(raw_name);
        if sink.wants(&name) {
            sink.put_field(&name, Value::Str(Cow::Borrowed(raw_value)));
        }
        ordinal += 1;
    }
    sink.end_row();
    Ok(())
}
```

Three rules keep this sound:

1. **Always memcmp before `put_field_at`.** `put_field_at` writes blindly to
   a slot; without the comparison, a shifted layout puts values in the
   wrong columns silently.
2. **Always `layout_broken(ordinal)` on mismatch.** Otherwise the stale
   entry either mismatches forever (you pay `expect_slot` + memcmp per row
   for nothing) or, worse, matches a *different* field that happens to carry
   the old name at this ordinal and writes it to the wrong slot.
3. **Never cache slot numbers yourself.** Slots are only valid through
   `expect_slot` for the current layout; a drop, rename, or schema change
   invalidates them.

One subtlety: the engine stores the **resolved** (post-rename) name bytes,
so the memcmp only hits when the raw bytes are already the final name. A
field whose name contains an entity (`&amp;`) or escapes never matches in
raw form and always falls through to the generic path. That is correct,
just not fast.

**Cost comparison:**

- Generic path: ~25 ns (find_attr_value + decode_attr + resolve + put_field)
- Fast path: ~8 ns (memcmp + put_field_at)

**When it helps:** formats with stable field order across rows (CSV columns,
XML attributes, JSONL keys). The first row learns the layout; subsequent
rows skip the expensive resolution.

**When it doesn't help:** formats where field order changes between rows
(every memcmp fails, so you pay for the attempt and the generic path), or
where field names contain entities that need decoding before comparison.

## The predicate-first fast path { #the-predicate-first-fast-path }

A filter creates a dilemma for the engine: a rejected row's values are
wasted work, but you cannot know whether a row passes until its predicate
column arrives, and that column may sit anywhere in the row. The two naive
strategies both lose:

- **Push everything, delete on reject:** values are materialized into Arrow
  builders before the verdict; a rejected row pays full column writes plus
  a per-column pop to remove them.
- **Buffer the whole row, decide at `end_row`:** rejected rows cost nothing
  in the columns, but every value of every *passing* row pays a buffer copy
  first, even when the filter was decidable after field 2.

Predicate-first is the middle ground: buffer only **until the predicate
resolves**, then commit to one of the two strategies for the rest of the
row. Fields arrive as `(slot, value)` pairs in a per-row buffer, and after
each field the engine re-evaluates the predicate using the values seen so
far:

```
begin_row → [buffered put_field × N] → end_row
                  │
                  ▼  after each field
           predicate state
           ├── Pass      → drain buffer into columns, switch to direct
           │               mode: remaining fields push unbuffered
           ├── Fail      → row_rejected() = true; end_row discards the
           │               buffer (zero Arrow writes for this row)
           └── Undecided → keep buffering
```

A **Pass mid-row** means the buffer drains once and the rest of the row
costs the same as an unfiltered parse. A **Fail mid-row** means nothing was
ever written to a column, and the scanner can stop reading the row
immediately. A rejected row therefore costs only its buffered fields up to
the predicate column; a passing row pays the buffer only up to the
predicate column too.

Your adapter's part is one check per field: after pushing, ask
`sink.row_rejected()` and byte-jump out of the row when the filter has
already decided against it:

```rust
sink.begin_row();
for (raw_name, raw_value) in self.scan_fields(bytes) {
    let name = decode_name(raw_name);
    if sink.wants(&name) {
        sink.put_field(&name, Value::Str(Cow::Borrowed(raw_value)));
    }
    // Filter decided against this row mid-row: skip to the row close,
    // end_row throws the buffered fields away.
    if sink.row_rejected() {
        let after = find_row_close(bytes, cur, row_tag, regions);
        sink.end_row();
        return Ok(Flow::At(after));
    }
}
sink.end_row();
```

Two things worth knowing when combining this with other features:

- `wants()` returns `true` for predicate columns even when they are
  projected out of the output, because the engine needs their values to
  evaluate the filter. They are parsed, used, and dropped at `finish()`.
- Buffering is why `Cow::Borrowed` values must borrow from the chunk, not
  from a temporary: the engine may hold a value past the point where your
  per-field temporary is gone (see
  [Parser: Cow::Borrowed](parser.md#1-use-cowborrowed-for-non-entity-text)).

**Adaptive strategy:** buffering only pays when the predicate resolves
early. The engine records the predicate column's ordinal on the first row;
if it appears late (beyond 4/5 of the columns), buffering whole rows is a
net loss, so from row 2 onward the engine switches to direct push +
pop-on-reject: values go straight into the columns, and a row that fails at
`end_row` has its values popped back out. The adapter code above is
unchanged; `row_rejected()` simply stays `false` mid-row in that mode.

## Example: minimal sink { #example-minimal-sink }

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

## Example: profiling sink (locate-only) { #example-profiling-sink }

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

## Thread safety { #thread-safety }

`ColumnarSink` is `Send` but not `Sync`. Each chunk gets its own sink
instance via `begin_row`/`end_row` lifecycle. The engine creates one
`TableBuilder` per chunk and merges them after all chunks complete.

!!! note

    The `begin_row`/`end_row` lifecycle means you can safely use non-atomic
    mutable state in your sink (e.g., counters, buffers). The engine never
    shares a sink instance across threads: it clones or creates per-chunk.


!!! warning

    If you implement a custom `ColumnarSink` and share it across threads via
    `&dyn ColumnarSink`, you will hit data races. Always let the engine manage
    sink instances: one per chunk, never shared.


## Build and test { #build-and-test }

Verify the projection contract: with a plan that drops every field except
`name`, a correct `wants()` implementation lets the parser skip all other
fields and the finished batch has exactly one column:

```console
$ cargo test sink_wants
running 1 test
test tests::sink_wants_skips_dropped_fields ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 9 filtered out; finished in 0.00s
```

The test builds a plan with `.drop("age").drop("active")`, parses a
two-row sample, and asserts `batch.num_columns() == 1` and the remaining
column is `name`. If your sink ignores `wants()`, all three columns
appear and this test fails.

## What the end user sees { #what-the-end-user-sees }

The sink and its fast paths are engine internals. The user calls
`to_arrow()` once and receives a finished `pyarrow.Table`; the per-field
method hierarchy (`put_field_at`, `put_field_resolved`, ...) only shows up
as throughput:

```python
from rypipe_log import LogSource

# begin_row/put_field/end_row have already run; this is a plain Arrow table.
table = LogSource("sample.log").to_arrow()
print(table.schema)
```
