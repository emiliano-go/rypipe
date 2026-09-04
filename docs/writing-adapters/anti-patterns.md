# Anti-Patterns { #anti-patterns }

Common mistakes in rypipe adapters. Avoiding these is the difference
between a working adapter and a fast one.

## 1. Not checking `wants()` { #wants }

**The mistake:** Scanning every field, even dropped ones.

```rust
// Bad: always scan, even for dropped fields
for field in self.parse_fields(line) {
    let value = self.extract_value(field);
    sink.put_field(field.name, Value::Str(Cow::Borrowed(value)));
}
```

Dropped columns still get scanned, decoded, and emitted — wasted work.

**The fix:** Check `sink.wants(field.name)` before extracting and emitting.

**Impact:** +66% on `drop_all` workloads.

## 2. Allocating in the hot path { #allocation }

**The mistake:** Creating `String` objects for every field value.

```rust
// Bad: allocates a String for every field
sink.put_field(&field.name.to_string(), Value::Str(Cow::Owned(field.value.to_string())));
```

Heap allocation is ~100 ns each. For 10 million fields that's 1s of
pure allocation overhead.

**The fix:** Borrow from input — `Cow::Borrowed(field.value)`.

**Impact:** 10-20% on allocation-heavy workloads.

## 3. Not declaring schema { #schema }

**The mistake:** Letting the engine discover column names at runtime.

```python
# Bad: engine must scan the file to find column names
src = MySource("data.log")
table = src.to_arrow()
```

Discovery pass doubles I/O and all values land as strings.

**The fix:** Pass `schema=["id", "name", "amount"]` and `field_types={"id": "int64"}`.

**Impact:** +80% with projection, +11% without.

## 4. Ignoring `plan_overrides` in `Source` { #plan-overrides }

**The mistake:** Not forwarding fused plan kwargs in `_read_arrow`.

```python
# Bad: fused stages fall back to Python
class MySource(Source):
    def _read_arrow(self, **kwargs):
        return my_rust_read(str(self._path))  # ignores kwargs!
```

Fused stages (rename, filter) fall back to Python over a full table —
10-50x slower.

**The fix:**

```python
class MySource(Source):
    def _read_arrow(self, *, plan_overrides=None, **kwargs):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return my_rust_read(str(self._path), **plan)
```

**Impact:** 10-50x for filtered/rename workloads.

## 5. Parsing values twice { #double-parse }

**The mistake:** Parsing for validation, then emitting the raw string.

```rust
// Bad: engine must parse the string again to build typed arrays
let value: i64 = field.value.parse().map_err(|e| ...)?;
sink.put_field("id", Value::Str(Cow::Borrowed(field.value)));
```

**The fix:** Parse once, emit the typed value — `Value::Int64(value)`.

**Impact:** 10-20% for numeric-heavy workloads.

## 6. Bad `estimate_bytes_per_row` { #estimate }

**The mistake:** Returning a fixed value regardless of the data.

```rust
// Bad: wrong for most formats
fn estimate_bytes_per_row(&self, _sample: &[u8]) -> usize { 100 }
```

Bad estimates create unbalanced chunks and hurt parallel efficiency.

**The fix:** Count newlines in the sample and divide.

**Impact:** 5-15% on parallel workloads.

## 7. Not implementing `validate()` { #validate }

**The mistake:** Leaving `validate()` as a no-op.

```rust
fn validate(&self, _bytes: &[u8]) -> Result<()> { Ok(()) }
```

Invalid UTF-8 causes panics in `from_utf8` during parse. Validation
catches it early with a useful error.

**The fix:**

```rust
fn validate(&self, bytes: &[u8]) -> Result<()> {
    simdutf8::basic::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Utf8(e))?;
    Ok(())
}
```

## 8. Not implementing `skip_regions` { #skip-regions }

**The mistake:** Letting the splitter chunk inside comments or CDATA.

```rust
fn skip_regions(&self) -> Option<&dyn SkipRegionFinder> { None }
```

The splitter sees record-start tokens inside comments and creates
false-positive split points, breaking the parse.

**The fix:** Implement `SkipRegionFinder` with openers (`<!--`, `<![CDATA[`)
and their corresponding closers (`-->`, `]]>`).

## 9. Wrong position in `next_record_start` { #record-start }

**The mistake:** Returning the delimiter position instead of after it.

```rust
// Bad: returns position OF newline, not after it
fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
    memchr::memchr(b'\n', &bytes[from..])
}
```

Overlapping chunks or missing data at chunk boundaries.

**The fix:** `.map(|r| from + r + 1)` — return position after delimiter.

## 10. Forgetting `end_row` { #end-row }

**The mistake:** Calling `begin_row` without a matching `end_row`.

```rust
sink.begin_row();
sink.put_field("name", Value::Str(Cow::Borrowed("Alice")));
// Forgot sink.end_row();
```

Values from multiple logical rows accumulate into one physical row,
producing wrong results silently.

**The fix:** Always pair `begin_row()` / `end_row()`.

## Summary { #summary }

| Anti-pattern | Impact | Fix |
|---|---|---|
| Not checking `wants()` | -66% drop workloads | `sink.wants()` first |
| Allocating in hot path | -10-20% | `Cow::Borrowed` |
| Not declaring schema | -80% projection | `schema` + `field_types` |
| Ignoring `plan_overrides` | -10-50x fusion | Forward kwargs |
| Parsing twice | -10-20% numeric | Parse once, emit typed |
| Bad `estimate_bytes_per_row` | -5-15% parallel | Count delimiters |
| Not implementing `validate()` | Crash on bad UTF-8 | Validate early |
| Not implementing `skip_regions` | Split inside comments | `SkipRegionFinder` |
| Wrong `next_record_start` | Overlapping chunks | Return pos after delim |
| Forgetting `end_row` | Silent wrong results | Pair begin/end |

## See also { #see-also }

- [Techniques](./techniques.md): Performance optimizations
- [Schema](./schema.md): The biggest performance lever
- [Splitter](./splitter.md): `Splitter` trait reference
- [Parser](./parser.md): `RecordParser` trait reference
