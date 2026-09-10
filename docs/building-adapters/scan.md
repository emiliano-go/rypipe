# Scan Primitives { #scan-primitives }

The `rypipe_core::scan` module provides portable byte-search primitives that
adapters should use instead of raw `memchr` calls. Each function has a
documented cost model.

## Functions { #functions }

### `find(hay, from, b) -> Option<usize>` { #find-optionusize }

Find byte `b` at or after position `from`.

```rust
pub fn find(hay: &[u8], from: usize, b: u8) -> Option<usize>
```

**Cost:** O(1) when `hay[from] == b` (the byte-at-position fast path).
Otherwise delegates to `memchr` (AVX2/SSE2/scalar).

**Use for:** Single-byte searches. The 15% win comes from the fast path
checking the current position before calling memchr.

### `find2(hay, from, a, b) -> Option<(usize, u8)>` { #find2-option }

Find either byte `a` or `b` at or after position `from`.
Returns `(position, matched_byte)`.

```rust
pub fn find2(hay: &[u8], from: usize, a: u8, b: u8) -> Option<(usize, u8)>
```

**Use for:** Dual-byte searches (e.g., finding `<` or `&` in XML text,
or `,` or `"` in CSV).

### `starts_with(hay, at, lit) -> bool` { #starts_with-bool }

Check if bytes at position `at` start with a given literal.

```rust
pub fn starts_with<const N: usize>(hay: &[u8], at: usize, lit: &[u8; N]) -> bool
```

**Use for:** Prefix checks on tags, keywords, or delimiters.

### `find_literal(hay, at, finder) -> Option<usize>` { #find_literal-optionusize }

Find a multi-byte literal using `memmem::Finder`.

```rust
pub fn find_literal(hay: &[u8], at: usize, finder: &memmem::Finder) -> Option<usize>
```

**Use for:** Container close tags where the body contains false candidates
(e.g., `</Field>` enclosing `<Field>` children).

### `utf8_after_chunk_validation(b) -> &str` { #utf8_after_chunk_validation-str }

Unsafe: convert SIMD-validated bytes to `&str` without re-scanning.

```rust
pub unsafe fn utf8_after_chunk_validation(b: &[u8]) -> &str
```

**Use for:** After `simdutf8::basic::from_utf8` has validated the chunk.

## The leaf-vs-container rule { #the-leaf-vs-container-rule }

**Candidate-plus-verify beats multi-byte search only when the delimiter has
no false candidates before it.**

- Leaf close tags (`</Value>`) never contain `<` inside → use `find`.
- Container tags (`</Field>` with child `<Field>` elements) contain `<` →
  use `find_literal`.

## Negative results { #negative-results }

- Scalar loops lose to memchr's AVX2 at every size tested (memchr switches
  at 16B SSE2 / 32B AVX2).
- `Finder` construction hoisting is worth ~0.4pp because construction was
  never the cost.

## Build and test { #build-and-test }

The scan helpers are pure functions, so test them directly against known
byte strings:

```console
$ cargo test scan_helpers
running 1 test
test tests::scan_helpers_find_bytes ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 9 filtered out; finished in 0.00s
```

The test asserts `find2(b"a=b,c=d", 0, b'=', b',') == Some((1, b'='))`,
`starts_with(b"a=b,c=d", 3, b",c")`, and that `find2` returns `None` when
neither byte is present. Note the helpers live in `rypipe_core::scan`,
not at the crate root.

## What the end user sees { #what-the-end-user-sees }

The scan primitives are invisible by design. Whether your parser uses
`scan::find` or `find_literal`, the user-facing call is the same plain
read; the primitives only show up as speed:

```python
from rypipe_log import LogSource

# No scan-related options exist; SIMD byte-search is purely internal.
table = LogSource("sample.log").to_arrow()
```
