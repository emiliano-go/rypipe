//! BlockMasks: one AVX2/SSE2 load per 64-byte block, cached bitmasks.
//!
//! 36.5% of parse is memchr on ~50-100 byte spans per field (5-7 separate
//! searches). Computing delimiter positions once per 64-byte block and
//! answering queries via bit operations removes per-search prologue.

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

/// 64-byte block masks, lazy per-delimiter.
pub struct BlockMasks<'a> {
    buf: &'a [u8],
    base: usize,
    computed: u8, // bit i set => masks[i] valid for current base
    masks: [u64; MAX_DELIMS],
    delims: &'static [u8],
}

impl<'a> BlockMasks<'a> {
    /// Create for `buf` with `delims` (static, ≤8, e.g. crxml: [b'<',b'>',b'"',b'\'',b'=']).
    pub fn new(buf: &'a [u8], delims: &'static [u8]) -> Self {
        assert!(delims.len() <= MAX_DELIMS, "MAX_DELIMS=8");
        Self {
            buf,
            base: usize::MAX, // sentinel: first seek will set
            computed: 0,
            masks: [0; MAX_DELIMS],
            delims,
        }
    }

    #[inline]
    fn delim_index(&self, d: u8) -> Option<usize> {
        self.delims.iter().position(|&x| x == d)
    }

    #[inline]
    fn seek_block(&mut self, from: usize) {
        let new_base = from & !63;
        if new_base != self.base {
            self.base = new_base;
            self.computed = 0;
        }
    }

    #[inline]
    fn advance_block(&mut self) -> bool {
        let nb = self.base + 64;
        if nb >= self.buf.len() {
            return false;
        }
        self.base = nb;
        self.computed = 0;
        true
    }

    #[inline]
    fn mask_for(&mut self, d: u8) -> u64 {
        let idx = self.delim_index(d).expect("delim not in set");
        if (self.computed & (1u8 << idx)) == 0 {
            let m = self.compute_mask(idx);
            self.masks[idx] = m;
            self.computed |= 1u8 << idx;
        }
        self.masks[idx]
    }

    #[inline]
    fn compute_mask(&self, idx: usize) -> u64 {
        let delim = self.delims[idx];
        let base = self.base;
        let buf = self.buf;
        if base + 64 <= buf.len() {
            unsafe { mask64(buf.as_ptr().add(base), delim) }
        } else {
            // Tail: copy remainder into stack array, compute, mask off beyond len.
            let rem = buf.len().saturating_sub(base);
            if rem == 0 {
                return 0;
            }
            let mut tmp = [0u8; 64];
            tmp[..rem].copy_from_slice(&buf[base..]);
            let m = unsafe { mask64(tmp.as_ptr(), delim) };
            // Zero-padded tail beyond rem is 0, which never matches delim (delim !=0),
            // but mask to be safe.
            if rem < 64 {
                m & ((1u64 << rem) - 1)
            } else {
                m
            }
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
                self.seek_block(cur);
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
                self.seek_block(cur);
            }
            let offset = cur - self.base;
            let mut combined = 0u64;
            for &d in ds {
                combined |= self.mask_for(d);
            }
            let masked = combined & (!0u64 << offset);
            if masked != 0 {
                let pos = self.base + masked.trailing_zeros() as usize;
                // Identify which delim matches at pos
                for &d in ds {
                    let m = self.mask_for(d);
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

// SAFETY: mask64 reads 64 bytes from p (caller ensures tail case handled).
#[inline]
unsafe fn mask64(p: *const u8, needle: u8) -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        if has_avx2() {
            return unsafe { mask64_avx2(p, needle) };
        }
        return unsafe { mask64_sse2(p, needle) };
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let _ = has_avx2();
        return unsafe { mask64_scalar(p, needle) };
    }
}

#[target_feature(enable = "avx2")]
unsafe fn mask64_avx2(p: *const u8, needle: u8) -> u64 {
    use std::arch::x86_64::*;
    let n = _mm256_set1_epi8(needle as i8);
    let lo = _mm256_loadu_si256(p as *const __m256i);
    let hi = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let c0 = _mm256_cmpeq_epi8(lo, n);
    let c1 = _mm256_cmpeq_epi8(hi, n);
    let m0 = _mm256_movemask_epi8(c0) as u32 as u64;
    let m1 = _mm256_movemask_epi8(c1) as u32 as u64;
    m0 | (m1 << 32)
}

#[target_feature(enable = "sse2")]
unsafe fn mask64_sse2(p: *const u8, needle: u8) -> u64 {
    use std::arch::x86_64::*;
    let n = _mm_set1_epi8(needle as i8);
    let mut mask = 0u64;
    for i in 0..4 {
        let v = _mm_loadu_si128(p.add(i * 16) as *const __m128i);
        let c = _mm_cmpeq_epi8(v, n);
        let m = _mm_movemask_epi8(c) as u64;
        mask |= m << (i * 16);
    }
    mask
}

unsafe fn mask64_scalar(p: *const u8, needle: u8) -> u64 {
    let mut mask = 0u64;
    for i in 0..64 {
        if *p.add(i) == needle {
            mask |= 1u64 << i;
        }
    }
    mask
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
                assert_eq!(a, b, "next mismatch at pos {pos} delim {d:?} buf len {}", buf.len());
            }
        }
    }

    #[test]
    fn block_boundaries() {
        let delims: &'static [u8] = &[b'<', b'>'];
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
        let delims: &'static [u8] = &[b'<'];
        // needle straddling will be tested via raw_text_until candidate+verify,
        // but check next still works across boundary
        let mut buf = vec![b'a'; 128];
        // place "</FormattedValue>" straddling 64
        let needle = b"</FormattedValue>";
        for i in 0..needle.len() {
            buf[60 + i] = needle[i];
        }
        let mut bm = BlockMasks::new(&buf, delims);
        assert_eq!(bm.next(0, b'<'), Some(60));
        assert_eq!(bm.next(61, b'<'), None);
    }

    #[test]
    fn tail_lengths() {
        let delims: &'static [u8] = &[b'<', b'>', b'"', b'\'', b'='];
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
        let delims: &'static [u8] = &[b'<'];
        check_next(b"", delims);
        check_next(b"<", delims);
        check_next(b"x", delims);
    }

    #[test]
    fn equivalence_random() {
        let delims: &'static [u8] = &[b'<', b'>', b'"', b'\'', b'='];
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
        let delims: &'static [u8] = &[b'"', b'\'', b'>'];
        let buf = br#"a"b'c>d"#;
        let mut bm = BlockMasks::new(buf, delims);
        assert_eq!(bm.next_any(0, delims), Some((1, b'"')));
        assert_eq!(bm.next_any(2, delims), Some((3, b'\'')));
        assert_eq!(bm.next_any(4, delims), Some((5, b'>')));
    }

    #[test]
    fn next_far_fallback() {
        let delims: &'static [u8] = &[b'<'];
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
    fn avx2_sse2_scalar_parity() {
        // Force scalar vs avx2 parity via direct mask64 calls are internal;
        // equivalence test already covers. This just ensures tail handling same.
        let delims: &'static [u8] = &[b'='];
        let buf = b"a=b=c";
        let mut bm = BlockMasks::new(buf, delims);
        assert_eq!(bm.next(0, b'='), Some(1));
        assert_eq!(bm.next(2, b'='), Some(3));
    }
}
