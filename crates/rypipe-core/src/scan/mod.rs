//! Portable scan primitives for byte searching.
//!
//! Consolidates the common byte-search patterns used by adapters (XML, TSV,
//! CSV, JSON). Each function has a documented cost model and the rule that
//! separates `find` from `find_literal` is stated explicitly.
//!
//! # The leaf-vs-container rule
//!
//! **Candidate-plus-verify beats multi-byte search only when the delimiter
//! has no false candidates before it.**
//!
//! - Leaf close tags (`</Value>`, `</Field>`) never contain the delimiter
//!   (`<`) inside their body → `find` (byte-at-position + memchr fallback)
//!   is correct and faster.
//! - Container tags (`</Field>` enclosing child `<Field>` elements) DO
//!   contain `<` inside → `find_literal` (memmem) is required to avoid
//!   false candidates.
//!
//! # Negative results (documented so nobody retries)
//!
//! - Scalar loops lose to memchr's AVX2 at every size tested (memchr
//!   switches at 16B SSE2 / 32B AVX2).
//! - `Finder` construction hoisting is worth ~0.4pp because construction
//!   was never the cost.

use std::sync::OnceLock;

// ---------------------------------------------------------------------------
// S6: Runtime SIMD dispatch
// ---------------------------------------------------------------------------

static HAS_AVX2: OnceLock<bool> = OnceLock::new();

/// Returns true if the current CPU supports AVX2.
#[cfg(target_arch = "x86_64")]
#[inline]
pub fn avx2() -> bool {
    *HAS_AVX2.get_or_init(|| is_x86_feature_detected!("avx2"))
}

// ---------------------------------------------------------------------------
// S5: Core scan primitives
// ---------------------------------------------------------------------------

/// Find byte `b` at or after position `from`.
///
/// **Cost:** O(1) when `bytes[from] == b` (the 15% fast path from crxml's
/// hot loop). Otherwise delegates to `memchr` (AVX2/SSE2/scalar).
#[inline]
pub fn find(hay: &[u8], from: usize, b: u8) -> Option<usize> {
    if from < hay.len() && hay[from] == b {
        return Some(from);
    }
    memchr::memchr(b, &hay[from..]).map(|p| from + p)
}

/// Find either byte `a` or `b` at or after position `from`.
/// Returns `(position, matched_byte)`.
#[inline]
pub fn find2(hay: &[u8], from: usize, a: u8, b: u8) -> Option<(usize, u8)> {
    if from < hay.len() {
        if hay[from] == a {
            return Some((from, a));
        }
        if hay[from] == b {
            return Some((from, b));
        }
    }
    memchr::memchr2(a, b, &hay[from..]).map(|p| (from + p, hay[from + p]))
}

/// Check if bytes at position `at` start with the given literal.
#[inline]
pub fn starts_with<const N: usize>(hay: &[u8], at: usize, lit: &[u8; N]) -> bool {
    at + N <= hay.len() && hay[at..at + N] == *lit
}

/// Find a multi-byte literal using `memmem::Finder`.
///
/// Use this for container close tags where the body contains false candidates
/// (e.g. `</Field>` enclosing `<Field>` children). For leaf tags where the
/// body never contains `<`, use `find` instead.
#[inline]
pub fn find_literal(hay: &[u8], at: usize, finder: &memchr::memmem::Finder) -> Option<usize> {
    finder.find(&hay[at..]).map(|p| at + p)
}

/// Validate that `b` is valid UTF-8 after chunk-level SIMD validation.
///
/// # Safety
///
/// The caller must have already validated `b` with `simdutf8::basic::from_utf8`
/// (or equivalent SIMD validator) at the chunk level. This function performs
/// the final Rust-level `str` conversion without re-scanning.
///
/// # Panics
///
/// Panics in debug mode if the bytes are not valid UTF-8.
#[inline]
pub unsafe fn utf8_after_chunk_validation(b: &[u8]) -> &str {
    debug_assert!(
        simdutf8::basic::from_utf8(b).is_ok(),
        "utf8_after_chunk_validation: bytes failed SIMD validation"
    );
    // SAFETY: caller guarantees SIMD-validated UTF-8.
    std::str::from_utf8_unchecked(b)
}

// ---------------------------------------------------------------------------
// S10c: Generic delimiter scanning for TSV/CSV
// ---------------------------------------------------------------------------

/// Find the field delimiter and quote/escape byte in one SIMD pass.
///
/// For CSV/TSV parsing, this replaces two separate `memchr` calls with a
/// single `memchr2` scan. Returns `(position, matched_byte)` where
/// `matched_byte` is either the delimiter or the quote.
///
/// Example: `find_delimiter_or_quote(data, 0, b',', b'"')` finds the first
/// comma or double-quote in the data.
#[inline]
pub fn find_delimiter_or_quote(
    hay: &[u8],
    from: usize,
    delimiter: u8,
    quote: u8,
) -> Option<(usize, u8)> {
    find2(hay, from, delimiter, quote)
}

/// Scan a delimited field, respecting quoted regions.
///
/// Returns the end position (byte after the delimiter or end of input).
/// When a quote is encountered, scans forward to the matching close quote
/// before resuming delimiter search. This is the portable half of crxml's
/// fused scan for CSV/TSV.
pub fn scan_delimited_field(bytes: &[u8], start: usize, delimiter: u8, quote: u8) -> usize {
    if start >= bytes.len() {
        return bytes.len();
    }
    match find2(bytes, start, delimiter, quote) {
        Some((rel, b)) if b == quote => {
            // Skip quoted region: find closing quote.
            let mut pos = rel + 1;
            while let Some(q_rel) = memchr::memchr(quote, &bytes[pos..]) {
                // Check for escaped quote (double quote).
                if pos + q_rel + 1 < bytes.len() && bytes[pos + q_rel + 1] == quote {
                    pos += q_rel + 2; // skip escaped quote
                } else {
                    return pos + q_rel + 1; // past closing quote
                }
            }
            bytes.len() // unterminated quote
        }
        Some((rel, _)) => rel + 1, // past delimiter
        None => bytes.len(),       // no more delimiters
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find() {
        let data = b"hello world";
        assert_eq!(find(data, 0, b'w'), Some(6));
        assert_eq!(find(data, 6, b'w'), Some(6));
        assert_eq!(find(data, 7, b'w'), None);
        assert_eq!(find(data, 0, b'x'), None);
    }

    #[test]
    fn test_find_fast_path() {
        // When bytes[from] == b, returns immediately without memchr.
        let data = b"xxxxx";
        assert_eq!(find(data, 3, b'x'), Some(3));
    }

    #[test]
    fn test_find2() {
        let data = b"a,b,c";
        assert_eq!(find2(data, 0, b'a', b'b'), Some((0, b'a')));
        assert_eq!(find2(data, 0, b'b', b'a'), Some((0, b'a')));
        assert_eq!(find2(data, 1, b'b', b'c'), Some((2, b'b')));
    }

    #[test]
    fn test_starts_with() {
        let data = b"<Field>";
        assert!(starts_with(data, 0, b"<Field>"));
        assert!(!starts_with(data, 0, b"<Text>"));
        assert!(!starts_with(data, 10, b"<Fiel"));
    }

    #[test]
    fn test_find_literal() {
        let finder = memchr::memmem::Finder::new(b"</Field>");
        let data = b"<Field><Value>1</Value></Field>";
        assert_eq!(find_literal(data, 0, &finder), Some(23));
    }

    #[test]
    fn test_utf8_after_chunk_validation() {
        let valid = "hello world";
        unsafe {
            assert_eq!(utf8_after_chunk_validation(valid.as_bytes()), valid);
        }
    }
}
