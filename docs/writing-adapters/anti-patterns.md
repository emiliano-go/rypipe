# Anti-Patterns

This page documents common mistakes in rypipe adapters. Avoiding these
patterns is the difference between a working adapter and a fast one.

## Anti-pattern 1: Not checking `wants()`

**The mistake:** Scanning and decoding every field, even dropped ones.

```rust
// Bad
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    for line in text.lines() {
        sink.begin_row();
        for field in self.parse_fields(line) {
            // Always scan, even for dropped fields
            let value = self.extract_value(field);
            sink.put_field(field.name, Value::Str(Cow::Borrowed(value)));
        }
        sink.end_row();
    }
    Ok(())
}
```

**Why it hurts:** When the user drops columns, your parser wastes CPU
scanning, decoding, and emitting values that the engine discards.

**The fix:** Check `wants()` first:

```rust
// Good
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    for line in text.lines() {
        sink.begin_row();
        for field in self.parse_fields(line) {
            if sink.wants(field.name) {
                let value = self.extract_value(field);
                sink.put_field(field.name, Value::Str(Cow::Borrowed(value)));
            }
        }
        sink.end_row();
    }
    Ok(())
}
```

**Impact:** +66% on `drop_all` workloads.

## Anti-pattern 2: Allocating in the hot path

**The mistake:** Creating `String` objects for every field value.

```rust
// Bad: allocates a String for every field
let name = field.name.to_string();
sink.put_field(&name, Value::Str(Cow::Owned(field.value.to_string())));
```

**Why it hurts:** Heap allocation is expensive (~100 ns per allocation).
For a file with 10 million fields, that's 1 second of pure allocation.

**The fix:** Borrow from the input:

```rust
// Good: zero allocation
sink.put_field(field.name, Value::Str(Cow::Borrowed(field.value)));
```

**Impact:** 10-20% on allocation-heavy workloads.

## Anti-pattern 3: Not declaring schema

**The mistake:** Letting the engine discover column names and types.

```python
# Bad: engine must scan the file to find column names
src = MySource("data.log")
table = src.to_arrow()
```

**Why it hurts:**

- Discovery pass doubles I/O (full scan to find field names)
- Each chunk may have different column order (merge path)
- All values stored as strings (no typed arrays)

**The fix:** Declare schema upfront:

```python
# Good: skip discovery, enable typed arrays
src = MySource("data.log", schema=["id", "name", "amount"],
               field_types={"id": "int64", "amount": "float64"})
table = src.to_arrow()
```

**Impact:** +80% with projection, +11% without.

## Anti-pattern 4: Ignoring `plan_overrides` in `Source`

**The mistake:** Not forwarding fused plan kwargs in `_read_arrow`.

```python
# Bad: fused stages fall back to Python
class MySource(Source):
    def _read_arrow(self, **kwargs):
        return my_rust_read(str(self._path))  # ignores kwargs!
```

**Why it hurts:** When the user writes `src | RenameFields(...)`, the
rename is fused into the Rust parse loop. If you ignore `plan_overrides`,
the rename happens in Python over a full table (10-50x slower).

**The fix:** Forward `plan_overrides`:

```python
# Good: fusion stays active
class MySource(Source):
    def _read_arrow(self, *, plan_overrides=None, **kwargs):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return my_rust_read(str(self._path), **plan)
```

**Impact:** 10-50x for filtered/rename workloads.

## Anti-pattern 5: Using `Cow::Owned` unnecessarily

**The mistake:** Converting borrowed slices to owned Strings.

```rust
// Bad: allocates
sink.put_field("name", Value::Str(Cow::Owned(field.value.to_string())));

// Good: borrows
sink.put_field("name", Value::Str(Cow::Borrowed(field.value)));
```

**When `Cow::Owned` is correct:**

- You must modify the value (unescape entities, normalize whitespace)
- The value is constructed, not sliced from input

**When `Cow::Borrowed` is correct:**

- The value is a slice of the input bytes
- No modification needed

## Anti-pattern 6: Parsing values twice

**The mistake:** Parsing for validation, then parsing again for the value.

```rust
// Bad: parse twice
let value: i64 = field.value.parse().map_err(|e| ...)?;
sink.put_field("id", Value::Str(Cow::Borrowed(field.value)));

// Good: parse once, emit typed
let value: i64 = field.value.parse().map_err(|e| ...)?;
sink.put_field("id", Value::Int64(value));
```

**Why it hurts:** Double parsing wastes CPU. The engine must parse the
string again to build the typed array.

**Impact:** 10-20% for numeric-heavy workloads.

## Anti-pattern 7: Not implementing `estimate_bytes_per_row`

**The mistake:** Returning a fixed value regardless of the data.

```rust
// Bad
fn estimate_bytes_per_row(&self, _sample: &[u8]) -> usize {
    100  // wrong for most formats
}
```

**Why it hurts:** The engine uses this to size chunks. A bad estimate
creates unbalanced chunks (some too small, some too large), hurting
parallel efficiency.

**The fix:** Count delimiters in the sample:

```rust
// Good
fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
    let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
    (sample.len() / n).max(1)
}
```

**Impact:** 5-15% on parallel workloads.

## Anti-pattern 8: Forgetting to register the adapter

**The mistake:** Implementing the adapter but not calling
`register_adapter`.

```python
# Bad: users get "no adapter registered" error
import _rypipe_log  # no registration!

# Good: register at import time
import rypipe
import _rypipe_log

class LogAdapter:
    def read(self, path, **kwargs):
        return _rypipe_log.read_log(path, **kwargs)

rypipe.register_adapter("log", LogAdapter(), extensions=[".log"])
```

**Why it hurts:** Users cannot use `rypipe.read("file.log")`. They must
pass the adapter explicitly.

## Anti-pattern 9: Returning wrong type from `read()`

**The mistake:** Returning a list of dicts instead of a `pyarrow.Table`.

```python
# Bad: returns list of dicts
class MyAdapter:
    def read(self, path, **kwargs):
        return [{"name": "Alice"}, {"name": "Bob"}]

# Good: returns pyarrow.Table
class MyAdapter:
    def read(self, path, **kwargs):
        import pyarrow as pa
        return pa.table({"name": ["Alice", "Bob"]})
```

**Why it hurts:** `rypipe.read()` expects a `pyarrow.Table`. Returning
dicts causes a type error.

## Anti-pattern 10: Not handling empty input

**The mistake:** Crashing on empty files.

```rust
// Bad: panics on empty input
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let text = std::str::from_utf8(bytes).unwrap();
    for line in text.lines() {
        // ...
    }
    Ok(())
}

// Good: handles empty input gracefully
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    let text = std::str::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
    for line in text.lines() {
        // ...
    }
    Ok(())
}
```

## Anti-pattern 11: Using `unwrap()` in production code

**The mistake:** Using `unwrap()` instead of proper error handling.

```rust
// Bad: panics on invalid input
let value: i64 = field.value.parse().unwrap();

// Good: returns an error
let value: i64 = field.value.parse()
    .map_err(|e| rypipe_core::Error::Plan(format!("invalid integer: {e}")))?;
```

**Why it hurts:** Panics crash the entire parse. The user gets an
unhelpful "thread panicked" message instead of a useful error.

## Anti-pattern 12: Not implementing `validate()`

**The mistake:** Leaving `validate()` empty.

```rust
// Bad: no validation
fn validate(&self, _bytes: &[u8]) -> Result<()> {
    Ok(())
}

// Good: validate UTF-8
fn validate(&self, bytes: &[u8]) -> Result<()> {
    simdutf8::basic::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Utf8(e))?;
    Ok(())
}
```

**Why it hurts:** Invalid UTF-8 causes panics in `std::str::from_utf8`
during parsing. Validation catches this early with a useful error message.

## Anti-pattern 13: Using `std::str::from_utf8` without validation

**The mistake:** Converting bytes to `&str` without checking validity.

```rust
// Bad: panics on invalid UTF-8
let text = std::str::from_utf8(bytes).unwrap();

// Good: use the validated bytes
let text = std::str::from_utf8(bytes)
    .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
```

**Why it hurts:** `from_utf8` panics on invalid UTF-8. Use the `Result`
version or rely on `validate()`.

## Anti-pattern 14: Not using `scan::find`

**The mistake:** Using raw `memchr` for byte searching.

```rust
// Bad: no O(1) fast path
let pos = memchr::memchr(b'<', bytes);

// Good: uses scan::find with O(1) fast path
let pos = rypipe_core::scan::find(bytes, b'<');
```

**Why it hurts:** `scan::find` checks for single-byte patterns first
(no SIMD setup cost), then falls back to `memchr` for multi-byte.

## Anti-pattern 15: Creating too many small chunks

**The mistake:** Letting the engine create tiny chunks.

```rust
// Bad: estimate returns very small values
fn estimate_bytes_per_row(&self, _sample: &[u8]) -> usize {
    10  // creates thousands of chunks
}
```

**Why it hurts:** Each chunk has overhead (thread scheduling, memory
allocation). Too many small chunks wastes more time on overhead than
on parsing.

**The fix:** Return accurate estimates. The engine has a 2 MiB minimum
chunk size, but accurate estimates help it balance chunks better.

## Summary

| Anti-pattern | Impact | Fix |
|--------------|--------|-----|
| Not checking `wants()` | -66% on drop workloads | Check `sink.wants()` first |
| Allocating in hot path | -10-20% | Use `Cow::Borrowed` |
| Not declaring schema | -80% with projection | Declare `schema_order` + `field_types` |
| Ignoring `plan_overrides` | -10-50x for fusion | Forward `plan_overrides` |
| Using `Cow::Owned` | -10-20% | Borrow when possible |
| Parsing twice | -10-20% | Parse once, emit typed |
| Bad `estimate_bytes_per_row` | -5-15% parallel | Count delimiters in sample |
| Forgetting registration | Users confused | Call `register_adapter` |
| Wrong return type | Type error | Return `pyarrow.Table` |
| Not handling empty input | Crash | Check `bytes.is_empty()` |
| Using `unwrap()` | Crash | Return `Result` |
| Not implementing `validate()` | Crash on bad UTF-8 | Validate in `validate()` |
| Not using `scan::find` | -5% scanning | Use `scan::find` |
| Too many small chunks | -5-15% overhead | Accurate `estimate_bytes_per_row` |

## Detailed examples

### Example: Fixing a slow parser

**Before (slow):**

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let text = std::str::from_utf8(bytes).unwrap(); // Anti-pattern: unwrap
    for line in text.lines() {
        sink.begin_row();
        for part in line.split(',') {
            let (k, v) = part.split_once('=').unwrap(); // Anti-pattern: unwrap
            let name = k.to_string(); // Anti-pattern: allocation
            let value = v.to_string(); // Anti-pattern: allocation
            sink.put_field(&name, Value::Str(Cow::Owned(value))); // Anti-pattern: Cow::Owned
        }
        sink.end_row();
    }
    Ok(())
}
```

**After (fast):**

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    if bytes.is_empty() { return Ok(()); } // Handle empty input

    let text = std::str::from_utf8(bytes)
        .map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

    for line in text.lines() {
        if line.is_empty() { continue; }
        sink.begin_row();
        for part in line.split(',') {
            if let Some((k, v)) = part.split_once('=') {
                if sink.wants(k) { // Check wants first
                    sink.put_field(k, Value::Str(Cow::Borrowed(v))); // Borrow
                }
            }
        }
        sink.end_row();
    }
    Ok(())
}
```

### Example: Fixing a slow Source

**Before (slow):**

```python
class MySource(Source):
    def _read_arrow(self, **kwargs):
        # Anti-pattern: ignores plan_overrides
        return _my_rust.read_file(str(self._path))
```

**After (fast):**

```python
class MySource(Source):
    def _read_arrow(self, *, plan_overrides=None, **kwargs):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return _my_rust.read_file(str(self._path), **plan)
```

## Anti-pattern 16: Not using typed values

**The mistake:** Emitting all values as strings even when types are known.

```rust
// Bad: all strings
sink.put_field("id", Value::Str(Cow::Borrowed("123")));
sink.put_field("amount", Value::Str(Cow::Borrowed("45.67")));
sink.put_field("active", Value::Str(Cow::Borrowed("true")));
```

**Why it hurts:** The engine must parse strings into typed arrays later.
Double work.

**The fix:** Emit typed values directly:

```rust
// Good: typed values
sink.put_field("id", Value::Int64(123));
sink.put_field("amount", Value::Float64(45.67));
sink.put_field("active", Value::Bool(true));
```

## Anti-pattern 17: Not implementing `skip_regions`

**The mistake:** Letting the splitter chunk inside comments or CDATA.

```rust
// Bad: no skip regions — splitter may chunk inside comments
fn skip_regions(&self) -> Option<&dyn SkipRegionFinder> {
    None
}
```

**Why it hurts:** The splitter may chunk inside a comment, causing parse
errors. For example:

```
<!-- This is a comment with <Row> tags -->
<Row><Field Name="x"><Value>1</Value></Field></Row>
```

Without skip regions, the splitter sees `<Row>` inside the comment and
creates a false-positive split point.

**The fix:** Implement `SkipRegionFinder`:

```rust
struct XmlSkipRegions;

impl SkipRegionFinder for XmlSkipRegions {
    fn openers(&self) -> &[&'static [u8]] {
        &[b"<!--", b"<![CDATA["]
    }

    fn closer_for(&self, opener: &[u8]) -> &'static [u8] {
        if opener == b"<!--" { b"-->" } else { b"]]>" }
    }
}

// Wire into your splitter:
impl Splitter for MyXmlSplitter {
    fn skip_regions(&self) -> Option<&dyn SkipRegionFinder> {
        Some(&XmlSkipRegions)
    }
}
```

## Anti-pattern 18: Not handling edge cases in `next_record_start`

**The mistake:** Returning incorrect positions for edge cases.

```rust
// Bad: returns position of newline, not after it
fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
    memchr::memchr(b'\n', &bytes[from..])
}
```

**Why it hurts:** The engine expects the position *after* the delimiter,
not at it. Returning the wrong position causes overlapping chunks or
missing data.

**The fix:** Return the position after the delimiter:

```rust
// Good: returns position after newline
fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
    memchr::memchr(b'\n', &bytes[from..]).map(|r| from + r + 1)
}
```

## Anti-pattern 19: Using `unwrap_or` instead of proper error handling

**The mistake:** Using `unwrap_or` to hide errors.

```rust
// Bad: silently swallows errors
let value: i64 = field.value.parse().unwrap_or(0);
sink.put_field("id", Value::Int64(value));
```

**Why it hurts:** Invalid input is silently converted to 0. The user
gets wrong data without knowing.

**The fix:** Return an error or skip the row:

```rust
// Good: return error
let value: i64 = field.value.parse()
    .map_err(|e| rypipe_core::Error::Plan(format!("invalid integer: {e}")))?;
sink.put_field("id", Value::Int64(value));

// Or: skip the field
if let Ok(value) = field.value.parse::<i64>() {
    sink.put_field("id", Value::Int64(value));
}
```

## Anti-pattern 20: Forgetting `end_row` after `begin_row`

**The mistake:** Calling `begin_row` without matching `end_row`.

```rust
// Bad: missing end_row
sink.begin_row();
sink.put_field("name", Value::Str(Cow::Borrowed("Alice")));
// Forgot sink.end_row();
```

**Why it hurts:** The engine accumulates values across multiple logical
rows into one physical row, producing wrong results.

**The fix:** Always pair `begin_row` and `end_row`:

```rust
// Good: matched begin/end
sink.begin_row();
sink.put_field("name", Value::Str(Cow::Borrowed("Alice")));
sink.end_row();
```

## See also

- [Techniques](./techniques.md): Performance optimizations
- [Schema](./schema.md): The biggest performance lever
- [Splitter](./splitter.md): `Splitter` trait reference
- [Parser](./parser.md): `RecordParser` trait reference
