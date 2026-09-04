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
