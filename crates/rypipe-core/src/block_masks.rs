//! BlockMasks: one AVX2/SSE2 load per 64-byte block, cached bitmasks.
//!
//! 36.5% of parse is memchr on ~50-100 byte spans per field (5-7 separate
//! searches). Computing delimiter positions once per 64-byte block and
//! answering queries via bit operations removes per-search prologue.
//!
//! Two things make this beat a sequence of `memchr` calls:
//!
//! 1. **Single-pass multi-delimiter masks.** Every delimiter in the set is
//!    masked from the *same* 64-byte load (one load, N `cmpeq`), instead of
//!    one full load per delimiter (simdxml's structural classifier shape).
//! 2. **Quote-aware scanning.** `next_unquoted` derives an "inside a quoted
//!    region" bitmask from the quote bitmask with a SWAR prefix-XOR
//!    (simdjson/simdxml trick), so delimiters inside attribute values are
//!    skipped in the same pass with no per-byte state machine.

use std::sync::OnceLock;

const MAX_DELIMS: usize = 8;

static HAS_AVX2: OnceLock<bool> = OnceLock::new();

#[inline]
fn has_avx2() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        *HAS_AVX2.get_or_init(|| is_x86_feature_detected!("avx2"))
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
}

/// Inclusive prefix-XOR (scan) of a 64-bit mask.
///
/// Bit `i` of the result is the parity of bits `0..=i`. Applied to a quote
/// bitmask this yields the "inside a quoted region" bitmask: a delimiter bit
/// ANDed with the complement of this is outside quotes.
#[inline]
pub fn prefix_xor(mut m: u64) -> u64 {
    m ^= m << 1;
    m ^= m << 2;
    m ^= m << 4;
    m ^= m << 8;
    m ^= m << 16;
    m ^= m << 32;
    m
}

/// 64-byte block masks.
///
/// Single-delimiter queries ([`next`](Self::next)) compute only the mask they
/// need (one load, one `cmpeq`). Multi-delimiter entry points
/// ([`next_any`](Self::next_any), [`next_unquoted`](Self::next_unquoted))
/// compute every delimiter from the *same* load in one pass.
pub struct BlockMasks<'a> {
    buf: &'a [u8],
    base: usize,
    loaded: bool,
    /// Bit `i` set => `masks[i]` valid for the current block.
    computed: u8,
    masks: [u64; MAX_DELIMS],
    /// Active quote character for `next_unquoted`: 0 = outside a quote,
    /// otherwise the quote byte (one of `quotes`) whose region we are inside.
    /// Only a matching quote byte closes it; the other quote byte is literal.
    in_quote: u8,
    /// Absolute buffer position through which `in_quote` has been advanced.
    /// A call with `from == quote_pos` continues; anything else resets.
    quote_pos: usize,
    delims: &'static [u8],
    /// Delimiters treated as quote characters by `next_unquoted` (⊆ delims).
    quotes: &'static [u8],
}

impl<'a> BlockMasks<'a> {
    /// Create for `buf` with `delims` (static, ≤8, e.g. crxml: [b'<',b'>',b'"',b'\'',b'=']).
    pub fn new(buf: &'a [u8], delims: &'static [u8]) -> Self {
        Self::build(buf, delims, &[])
    }

    /// Like [`new`](Self::new) but enables [`next_unquoted`](Self::next_unquoted)
    /// with the given quote characters. Each quote char must be in `delims`.
    pub fn with_quotes(buf: &'a [u8], delims: &'static [u8], quotes: &'static [u8]) -> Self {
        Self::build(buf, delims, quotes)
    }

    fn build(buf: &'a [u8], delims: &'static [u8], quotes: &'static [u8]) -> Self {
        assert!(delims.len() <= MAX_DELIMS, "MAX_DELIMS=8");
        for &q in quotes {
            assert!(delims.contains(&q), "quote char must be in delims");
        }
        Self {
            buf,
            base: usize::MAX, // sentinel: first seek will set
            loaded: false,
            computed: 0,
            masks: [0; MAX_DELIMS],
            in_quote: 0,
            quote_pos: 0,
            delims,
            quotes,
        }
    }

    #[inline]
    fn delim_index(&self, d: u8) -> Option<usize> {
        self.delims.iter().position(|&x| x == d)
    }

    /// Bitmask of "all current delimiters computed".
    #[inline]
    fn full_mask(&self) -> u8 {
        let n = self.delims.len();
        if n >= 8 {
            u8::MAX
        } else {
            (1u8 << n) - 1
        }
    }

    /// Fill `out[..delims.len()]` with masks for the 64-byte block at `base`.
    #[inline]
    fn fill(&self, base: usize, delims: &[u8], out: &mut [u64]) {
        let n = delims.len();
        if n == 0 {
            return;
        }
        if base + 64 <= self.buf.len() {
            // SAFETY: the guard proves 64 readable bytes start at `base`.
            unsafe { masks64(self.buf.as_ptr().add(base), delims, out, n) };
        } else {
            // Tail: copy remainder into stack array, compute, mask off beyond len.
            let rem = self.buf.len().saturating_sub(base);
            if rem == 0 {
                return;
            }
            let mut tmp = [0u8; 64];
            tmp[..rem].copy_from_slice(&self.buf[base..base + rem]);
            // SAFETY: `tmp` is a 64-byte array (the remainder, zero-padded).
            unsafe { masks64(tmp.as_ptr(), delims, out, n) };
            let keep = (1u64 << rem) - 1;
            for m in out.iter_mut().take(n) {
                *m &= keep;
            }
        }
    }

    /// Load (or reload) the block at `base`; masks become stale.
    #[inline]
    fn load_at(&mut self, base: usize) {
        self.base = base;
        self.loaded = true;
        self.computed = 0;
    }

    /// Select the block containing `from`, reloading only if it changed.
    #[inline]
    fn select_block(&mut self, from: usize) {
        let new_base = from & !63;
        if new_base != self.base || !self.loaded {
            self.load_at(new_base);
        }
    }

    #[inline]
    fn advance_block(&mut self) -> bool {
        let nb = self.base.wrapping_add(64);
        if nb >= self.buf.len() {
            return false;
        }
        self.load_at(nb);
        true
    }

    /// Mask for `d`, computed alone (single load, one compare) if not cached.
    #[inline]
    fn mask_for(&mut self, d: u8) -> u64 {
        let idx = self.delim_index(d).expect("delim not in set");
        if (self.computed >> idx) & 1 == 0 {
            self.masks[idx] = self.compute_one(d);
            self.computed |= 1 << idx;
        }
        self.masks[idx]
    }

    /// Compute a single delimiter's mask for the current block (tail-safe).
    #[inline]
    fn compute_one(&self, d: u8) -> u64 {
        let base = self.base;
        if base + 64 <= self.buf.len() {
            // SAFETY: the guard proves 64 readable bytes start at `base`.
            unsafe { mask64(self.buf.as_ptr().add(base), d) }
        } else {
            let rem = self.buf.len().saturating_sub(base);
            if rem == 0 {
                return 0;
            }
            let mut tmp = [0u8; 64];
            tmp[..rem].copy_from_slice(&self.buf[base..base + rem]);
            // SAFETY: `tmp` is a 64-byte array; the mask is trimmed to `rem`.
            (unsafe { mask64(tmp.as_ptr(), d) }) & ((1u64 << rem) - 1)
        }
    }

    /// Compute every delimiter mask for the current block in one pass.
    #[inline]
    fn ensure_all(&mut self) {
        if self.computed != self.full_mask() {
            let mut out = [0u64; MAX_DELIMS];
            self.fill(self.base, self.delims, &mut out);
            self.masks = out;
            self.computed = self.full_mask();
        }
    }

    /// Bit positions `>= pos` as a mask (`0` once `pos >= 64`).
    #[inline]
    fn from_mask(pos: usize) -> u64 {
        if pos >= 64 {
            0
        } else {
            !0u64 << pos
        }
    }

    /// Next occurrence of `d` at or after `from`.
    #[inline]
    pub fn next(&mut self, from: usize, d: u8) -> Option<usize> {
        if from >= self.buf.len() {
            return None;
        }
        let mut cur = from;
        loop {
            if cur < self.base || cur >= self.base + 64 {
                self.select_block(cur);
            }
            let offset = cur - self.base;
            // mask off bits before offset
            let m = self.mask_for(d) & (!0u64 << offset);
            if m != 0 {
                return Some(self.base + m.trailing_zeros() as usize);
            }
            if !self.advance_block() {
                return None;
            }
            cur = self.base;
        }
    }

    /// Next occurrence of any of `ds` at or after `from`, returns (pos, delim).
    #[inline]
    pub fn next_any(&mut self, from: usize, ds: &[u8]) -> Option<(usize, u8)> {
        if from >= self.buf.len() {
            return None;
        }
        let mut cur = from;
        loop {
            if cur < self.base || cur >= self.base + 64 {
                self.select_block(cur);
            }
            self.ensure_all();
            let offset = cur - self.base;
            let mut combined = 0u64;
            for &d in ds {
                combined |= self.masks[self.delim_index(d).expect("delim not in set")];
            }
            let masked = combined & (!0u64 << offset);
            if masked != 0 {
                let pos = self.base + masked.trailing_zeros() as usize;
                // Identify which delim matches at pos
                for &d in ds {
                    let m = self.masks[self.delim_index(d).expect("delim not in set")];
                    if (m >> (pos - self.base)) & 1 == 1 {
                        return Some((pos, d));
                    }
                }
                // Should not reach: combined had bit but no individual? Fallback
                return Some((pos, ds[0]));
            }
            if !self.advance_block() {
                return None;
            }
            cur = self.base;
        }
    }

    /// Next occurrence of `d` at or after `from` that lies **outside** any
    /// quoted region (per the quote chars given to [`with_quotes`](Self::with_quotes)).
    ///
    /// Quote regions follow XML attribute-value rules: a value opened by `"` is
    /// closed only by the next `"`, and vice versa for `'`. A quote byte of the
    /// other type inside a value is literal.
    ///
    /// This is a **forward cursor**: quote state is accumulated as the scan
    /// advances, so each call's `from` must be ≥ the previously returned
    /// position (the natural `from = last_found + 1` loop). Querying a position
    /// behind the cursor resets the quote state, which is only correct if that
    /// earlier prefix has no unbalanced quote.
    #[inline]
    pub fn next_unquoted(&mut self, from: usize, d: u8) -> Option<usize> {
        if self.quotes.is_empty() {
            return self.next(from, d);
        }
        if from >= self.buf.len() {
            return None;
        }
        let di = self.delim_index(d).expect("delim not in set");
        let mut cur = from;
        // Continue only when `from` is exactly where the cursor left off;
        // any other position is a fresh scan (unknown quote state).
        if !self.loaded || from != self.quote_pos {
            self.in_quote = 0;
            self.quote_pos = from;
            self.load_at(from & !63);
        }
        loop {
            self.ensure_all();
            if let Some(p) = self.scan_block_unquoted(cur - self.base, di) {
                self.quote_pos = self.base + p + 1;
                return Some(self.base + p);
            }
            if !self.advance_block() {
                self.quote_pos = self.base.wrapping_add(64);
                return None;
            }
            cur = self.base;
            self.quote_pos = cur;
        }
    }

    /// Within the loaded block, from byte `offset`, return the offset of the
    /// next `delims[di]` that is outside quotes, updating `in_quote`. `None` if
    /// the block ends first.
    fn scan_block_unquoted(&mut self, offset: usize, di: usize) -> Option<usize> {
        let mut pos = offset;
        if self.in_quote == 0 {
            // Fast path: if only one quote type remains, "inside" is a plain
            // prefix-XOR of that quote's mask (simdjson/simdxml trick).
            let q_single = self.single_quote_remaining(pos);
            if let Some(qi) = q_single {
                let qmask = self.masks[qi] & Self::from_mask(pos);
                let inside = prefix_xor(qmask);
                let hits = self.masks[di] & Self::from_mask(pos) & !inside;
                if hits != 0 {
                    // We return outside, so `in_quote` stays 0 for the cursor.
                    return Some(hits.trailing_zeros() as usize);
                }
                // No delimiter before block end; carry the parity to the next block.
                if qmask.count_ones() & 1 == 1 {
                    self.in_quote = self.delims[qi];
                }
                return None;
            }
        }
        // General path: walk quote events (only the active type closes).
        loop {
            if self.in_quote == 0 {
                let mut qbits = 0u64;
                for &q in self.quotes {
                    qbits |= self.masks[self.delim_index(q).expect("quote in delims")];
                }
                let g = self.masks[di] & Self::from_mask(pos);
                let q = qbits & Self::from_mask(pos);
                let ghas = g != 0;
                let qhas = q != 0;
                let gpos = g.trailing_zeros();
                let qpos = q.trailing_zeros();
                if ghas && (!qhas || gpos < qpos) {
                    return Some(gpos as usize);
                }
                if !qhas {
                    return None;
                }
                self.in_quote = self.delims[self.quote_index_at(qpos as usize)];
                pos = qpos as usize + 1;
            } else {
                let qi = self.delim_index(self.in_quote).expect("quote in delims");
                let c = self.masks[qi] & Self::from_mask(pos);
                if c == 0 {
                    return None;
                }
                self.in_quote = 0;
                pos = c.trailing_zeros() as usize + 1;
            }
        }
    }

    /// If exactly one quote type occurs at or after block offset `pos`, return
    /// its delimiter index.
    #[inline]
    fn single_quote_remaining(&self, pos: usize) -> Option<usize> {
        let m = Self::from_mask(pos);
        let mut found = None;
        for &q in self.quotes {
            let qi = self.delim_index(q).expect("quote in delims");
            if self.masks[qi] & m != 0 {
                if found.is_some() {
                    return None;
                }
                found = Some(qi);
            }
        }
        found
    }

    /// Delimiter index of the quote byte at block offset `pos`.
    #[inline]
    fn quote_index_at(&self, pos: usize) -> usize {
        for &q in self.quotes {
            let qi = self.delim_index(q).expect("quote in delims");
            if (self.masks[qi] >> pos) & 1 == 1 {
                return qi;
            }
        }
        unreachable!("quote_index_at: no quote byte at position");
    }

    /// Long-span fallback: if hint >256 use memchr, else BlockMasks.
    #[inline]
    pub fn next_far(&mut self, from: usize, d: u8, hint: usize) -> Option<usize> {
        if hint > 256 {
            memchr::memchr(d, &self.buf[from..]).map(|i| from + i)
        } else {
            self.next(from, d)
        }
    }
}

// SAFETY: `p` must point to at least 64 readable bytes.
#[inline]
unsafe fn mask64(p: *const u8, needle: u8) -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        if has_avx2() {
            // SAFETY: the function contract guarantees 64 readable bytes; AVX2
            // is available (checked).
            return unsafe { mask64_avx2(p, needle) };
        }
        // SAFETY: the contract guarantees 64 readable bytes; SSE2 is baseline.
        unsafe { mask64_sse2(p, needle) }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let _ = has_avx2();
        // SAFETY: the contract guarantees 64 readable bytes.
        unsafe { mask64_scalar(p, needle) }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
/// # Safety
///
/// `p` must point to at least 64 readable bytes.
unsafe fn mask64_avx2(p: *const u8, needle: u8) -> u64 {
    use std::arch::x86_64::*;
    let n = _mm256_set1_epi8(needle as i8);
    let lo = _mm256_loadu_si256(p as *const __m256i);
    let hi = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let m0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, n)) as u32 as u64;
    let m1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, n)) as u32 as u64;
    m0 | (m1 << 32)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
/// # Safety
///
/// `p` must point to at least 64 readable bytes.
unsafe fn mask64_sse2(p: *const u8, needle: u8) -> u64 {
    use std::arch::x86_64::*;
    let n = _mm_set1_epi8(needle as i8);
    let mut mask = 0u64;
    for i in 0..4 {
        let v = _mm_loadu_si128(p.add(i * 16) as *const __m128i);
        let m = _mm_movemask_epi8(_mm_cmpeq_epi8(v, n)) as u64;
        mask |= m << (i * 16);
    }
    mask
}

#[cfg(any(not(target_arch = "x86_64"), test))]
/// # Safety
///
/// `p` must point to at least 64 readable bytes.
unsafe fn mask64_scalar(p: *const u8, needle: u8) -> u64 {
    let mut mask = 0u64;
    for i in 0..64 {
        if *p.add(i) == needle {
            mask |= 1u64 << i;
        }
    }
    mask
}

// SAFETY: `p` must point to at least 64 readable bytes; writes `out[..n]`.
#[inline]
unsafe fn masks64(p: *const u8, delims: &[u8], out: &mut [u64], n: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        if has_avx2() {
            // SAFETY: the function contract guarantees 64 readable bytes and
            // `out[..n]` writable; AVX2 is available (checked).
            unsafe { masks64_avx2(p, delims, out, n) };
            return;
        }
        // SAFETY: the contract guarantees 64 readable bytes and out[..n]; SSE2
        // is baseline.
        unsafe { masks64_sse2(p, delims, out, n) };
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let _ = has_avx2();
        // SAFETY: the contract guarantees 64 readable bytes and out[..n].
        unsafe { masks64_scalar(p, delims, out, n) };
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
/// # Safety
///
/// `p` must point to at least 64 readable bytes.
unsafe fn masks64_avx2(p: *const u8, delims: &[u8], out: &mut [u64], n: usize) {
    use std::arch::x86_64::*;
    let lo = _mm256_loadu_si256(p as *const __m256i);
    let hi = _mm256_loadu_si256(p.add(32) as *const __m256i);
    for i in 0..n {
        let v = _mm256_set1_epi8(delims[i] as i8);
        let m0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, v)) as u32 as u64;
        let m1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, v)) as u32 as u64;
        out[i] = m0 | (m1 << 32);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
/// # Safety
///
/// `p` must point to at least 64 readable bytes.
unsafe fn masks64_sse2(p: *const u8, delims: &[u8], out: &mut [u64], n: usize) {
    use std::arch::x86_64::*;
    let v0 = _mm_loadu_si128(p as *const __m128i);
    let v1 = _mm_loadu_si128(p.add(16) as *const __m128i);
    let v2 = _mm_loadu_si128(p.add(32) as *const __m128i);
    let v3 = _mm_loadu_si128(p.add(48) as *const __m128i);
    for i in 0..n {
        let d = _mm_set1_epi8(delims[i] as i8);
        let m0 = _mm_movemask_epi8(_mm_cmpeq_epi8(v0, d)) as u64;
        let m1 = _mm_movemask_epi8(_mm_cmpeq_epi8(v1, d)) as u64;
        let m2 = _mm_movemask_epi8(_mm_cmpeq_epi8(v2, d)) as u64;
        let m3 = _mm_movemask_epi8(_mm_cmpeq_epi8(v3, d)) as u64;
        out[i] = m0 | (m1 << 16) | (m2 << 32) | (m3 << 48);
    }
}

#[cfg(any(not(target_arch = "x86_64"), test))]
/// # Safety
///
/// `p` must point to at least 64 readable bytes.
unsafe fn masks64_scalar(p: *const u8, delims: &[u8], out: &mut [u64], n: usize) {
    out[..n].fill(0);
    for j in 0..64 {
        let b = *p.add(j);
        for i in 0..n {
            if delims[i] == b {
                out[i] |= 1u64 << j;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use memchr::memchr;

    fn check_next(buf: &[u8], delims: &'static [u8]) {
        let mut bm = BlockMasks::new(buf, delims);
        for &d in delims {
            for pos in 0..buf.len() {
                let a = bm.next(pos, d);
                let b = memchr(d, &buf[pos..]).map(|i| pos + i);
                assert_eq!(
                    a,
                    b,
                    "next mismatch at pos {pos} delim {d:?} buf len {}",
                    buf.len()
                );
            }
        }
    }

    /// Reference: scalar quote-aware scan from `from`, with XML matching-quote
    /// semantics (a quote byte is closed only by the same byte).
    fn scan_unquoted(buf: &[u8], from: usize, d: u8, quotes: &[u8]) -> Option<usize> {
        let mut active: u8 = 0;
        for &b in &buf[..from] {
            if active == 0 {
                if quotes.contains(&b) {
                    active = b;
                }
            } else if b == active {
                active = 0;
            }
        }
        for (i, &b) in buf.iter().enumerate().skip(from) {
            if active == 0 {
                if quotes.contains(&b) {
                    active = b;
                } else if b == d {
                    return Some(i);
                }
            } else if b == active {
                active = 0;
            }
        }
        None
    }

    #[test]
    fn block_boundaries() {
        let delims: &'static [u8] = b"<>";
        // delimiter at byte 63 and 64
        let mut buf = vec![b'x'; 128];
        buf[63] = b'<';
        buf[64] = b'>';
        buf[127] = b'<';
        check_next(&buf, delims);
        // field spanning three blocks
        let mut buf2 = vec![b'x'; 200];
        buf2[10] = b'<';
        buf2[70] = b'>';
        buf2[130] = b'<';
        check_next(&buf2, delims);
    }

    #[test]
    fn straddling() {
        let delims: &'static [u8] = b"<";
        // needle straddling will be tested via raw_text_until candidate+verify,
        // but check next still works across boundary
        let mut buf = vec![b'a'; 128];
        // place "</FormattedValue>" straddling 64
        let needle = b"</FormattedValue>";
        buf[60..(needle.len() + 60)].copy_from_slice(&needle[..]);
        let mut bm = BlockMasks::new(&buf, delims);
        assert_eq!(bm.next(0, b'<'), Some(60));
        assert_eq!(bm.next(61, b'<'), None);
    }

    #[test]
    fn tail_lengths() {
        let delims: &'static [u8] = b"<>\"'=";
        for len in [1, 63, 64, 65, 127, 128, 129] {
            let mut buf = vec![b'x'; len];
            if len > 0 {
                buf[len - 1] = b'"';
            }
            if len > 1 {
                buf[0] = b'<';
            }
            check_next(&buf, delims);
        }
    }

    #[test]
    fn empty_and_single() {
        let delims: &'static [u8] = b"<";
        check_next(b"", delims);
        check_next(b"<", delims);
        check_next(b"x", delims);
    }

    #[test]
    fn equivalence_random() {
        let delims: &'static [u8] = b"<>\"'=";
        let mut rng = 0u64;
        let mut next_rng = || {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            (rng >> 33) as u8
        };
        for _ in 0..100 {
            let len = (next_rng() as usize % 200) + 1;
            let mut buf = vec![0u8; len];
            for b in &mut buf {
                *b = next_rng();
            }
            check_next(&buf, delims);
            // next_any
            let mut bm = BlockMasks::new(&buf, delims);
            for pos in 0..len {
                let a = bm.next_any(pos, delims);
                let mut exp: Option<(usize, u8)> = None;
                for &d in delims {
                    if let Some(p) = memchr(d, &buf[pos..]).map(|i| pos + i) {
                        if exp.is_none() || p < exp.unwrap().0 {
                            exp = Some((p, d));
                        }
                    }
                }
                assert_eq!(a, exp, "next_any mismatch at pos {pos}");
            }
        }
    }

    #[test]
    fn next_any_correct() {
        let delims: &'static [u8] = b"\"'>";
        let buf = br#"a"b'c>d"#;
        let mut bm = BlockMasks::new(buf, delims);
        assert_eq!(bm.next_any(0, delims), Some((1, b'"')));
        assert_eq!(bm.next_any(2, delims), Some((3, b'\'')));
        assert_eq!(bm.next_any(4, delims), Some((5, b'>')));
    }

    #[test]
    fn next_far_fallback() {
        let delims: &'static [u8] = b"<";
        let buf = vec![b'x'; 1000];
        let mut bm = BlockMasks::new(&buf, delims);
        // hint >256 should use memchr path, but still correct
        assert_eq!(bm.next_far(0, b'<', 500), None);
        let mut buf2 = vec![b'x'; 1000];
        buf2[900] = b'<';
        let mut bm2 = BlockMasks::new(&buf2, delims);
        assert_eq!(bm2.next_far(0, b'<', 500), Some(900));
        assert_eq!(bm2.next_far(0, b'<', 10), Some(900));
    }

    #[test]
    fn prefix_xor_basic() {
        assert_eq!(prefix_xor(0), 0);
        assert_eq!(prefix_xor(1), u64::MAX); // bit 0 set => bits 0..=63 set
        assert_eq!(prefix_xor(1 << 3), !0u64 << 3);
        // two bits toggle parity twice
        assert_eq!(
            prefix_xor((1 << 3) | (1 << 5)),
            (!0u64 << 3) & !(!0u64 << 5)
        );
    }

    #[test]
    fn next_unquoted_matches_reference() {
        let delims: &'static [u8] = b"<>\"'";
        let quotes: &'static [u8] = b"\"'";
        let cases: &[&[u8]] = &[
            br#"a<b c="x<y>z">d<e>"#,
            br#"<attr value='has " and < inside'>tail"#,
            br#"<tag attr="it's">next<gt>"#,
            br#"<tag a='say "hi"'>next<gt>"#,
            b"no quotes < > < >",
            br#"quote="alone'd?"t<"#,
            b"",
            b"<",
            b"\">",
        ];
        for buf in cases {
            let mut bm = BlockMasks::with_quotes(buf, delims, quotes);
            for d in *b"<>" {
                let mut from = 0;
                loop {
                    let a = bm.next_unquoted(from, d);
                    let b = scan_unquoted(buf, from, d, quotes);
                    assert_eq!(a, b, "unquoted mismatch d={d} from={from} buf={buf:?}");
                    match a {
                        Some(p) => from = p + 1,
                        None => break,
                    }
                }
            }
        }
    }

    #[test]
    fn next_unquoted_across_blocks() {
        let delims: &'static [u8] = b"<>\"";
        let quotes: &'static [u8] = b"\"";
        // Quote opens in block 0 and closes in block 1, hiding a '<' at 100 and 200.
        let mut buf = vec![b'x'; 300];
        buf[10] = b'"';
        buf[100] = b'<'; // inside quote
        buf[150] = b'"'; // close
        buf[200] = b'<'; // outside
        let mut bm = BlockMasks::with_quotes(&buf, delims, quotes);
        assert_eq!(bm.next_unquoted(0, b'<'), Some(200));
        // Full forward-cursor walk must match the scalar reference at each step.
        let mut from = 0;
        loop {
            let a = bm.next_unquoted(from, b'<');
            let b = scan_unquoted(&buf, from, b'<', quotes);
            assert_eq!(a, b, "from {from}");
            match a {
                Some(p) => from = p + 1,
                None => break,
            }
        }
    }

    #[test]
    fn next_unquoted_random() {
        let delims: &'static [u8] = b"<>\"'";
        let quotes: &'static [u8] = b"\"'";
        let mut rng = 0x1234_5678_9abc_def0u64;
        let mut next_rng = || {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            (rng >> 33) as u8
        };
        let alphabet = b"ab <>\"'";
        for _ in 0..200 {
            let len = (next_rng() as usize % 300) + 1;
            let mut buf = vec![0u8; len];
            for b in &mut buf {
                *b = alphabet[(next_rng() as usize) % alphabet.len()];
            }
            let mut bm = BlockMasks::with_quotes(&buf, delims, quotes);
            for d in *b"<>" {
                let mut from = 0;
                loop {
                    let a = bm.next_unquoted(from, d);
                    let b = scan_unquoted(&buf, from, d, quotes);
                    assert_eq!(a, b, "d={d} from={from} buf={buf:?}");
                    match a {
                        Some(p) => from = p + 1,
                        None => break,
                    }
                }
            }
        }
    }

    #[test]
    fn masks_parity_avx2_sse2_scalar() {
        let delims: &'static [u8] = b"<>\"'=x";
        let mut bytes = [0u8; 128];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = (i % 17) as u8;
        }
        for offset in 0..64 {
            // SAFETY: `bytes` is 128 bytes, so `offset..offset+64` is in bounds.
            unsafe {
                let ptr = bytes.as_ptr().add(offset);
                let mut expected = [0u64; MAX_DELIMS];
                masks64_scalar(ptr, delims, &mut expected, delims.len());
                let mut got = [0u64; MAX_DELIMS];
                masks64(ptr, delims, &mut got, delims.len());
                assert_eq!(got, expected);
                // Single-delimiter path must agree with the multi pass.
                for (i, &d) in delims.iter().enumerate() {
                    assert_eq!(mask64(ptr, d), expected[i]);
                }
                assert_eq!(mask64_scalar(ptr, delims[0]), expected[0]);
                #[cfg(target_arch = "x86_64")]
                {
                    let mut sse = [0u64; MAX_DELIMS];
                    masks64_sse2(ptr, delims, &mut sse, delims.len());
                    assert_eq!(sse, expected);
                    if has_avx2() {
                        let mut avx = [0u64; MAX_DELIMS];
                        masks64_avx2(ptr, delims, &mut avx, delims.len());
                        assert_eq!(avx, expected);
                    }
                }
            }
        }
    }
}
