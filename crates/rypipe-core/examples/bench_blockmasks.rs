//! BlockMasks microbench: memchr_n_calls vs blockmasks_n.
//!
//! ```bash
//! cargo run --release -p rypipe-core --example bench_blockmasks
//! ```

use std::time::Instant;

const DELIMS: &[u8] = &[b'<', b'>', b'"', b'\'', b'='];
const ITERS: usize = 500_000;

fn main() {
    let spans: &[usize] = &[32, 64, 128, 512];
    let counts: &[usize] = &[1, 2, 3, 5, 8];

    println!("BlockMasks vs memchr microbench ({} iterations)", ITERS);
    println!("{:<6} {:<6} {:<14} {:<14} {:<8}", "span", "n", "memchr (ns)", "blockmasks (ns)", "ratio");
    println!("{}", "-".repeat(60));

    for &span in spans {
        // Generate random-ish data with a few delimiters scattered
        let mut buf = vec![b'x'; span];
        // Place delimiters at known positions for realistic density
        for i in (0..span).step_by(span / 8).take(8) {
            buf[i] = DELIMS[i % DELIMS.len()];
        }

        for &n in counts {
            // Pick n distinct delimiters from the set
            let query_delims: Vec<u8> = DELIMS.iter().take(n).copied().collect();

            // --- memchr_n_calls: n separate memchr calls ---
            let t_memchr = bench_memchr(&buf, &query_delims, ITERS);

            // --- blockmasks_n: 1 block load + n mask queries ---
            let t_blockmasks = bench_blockmasks(&buf, &query_delims, ITERS);

            let ratio = t_memchr as f64 / t_blockmasks as f64;
            println!("{:<6} {:<6} {:<14} {:<14} {:<8.2}x", span, n, t_memchr, t_blockmasks, ratio);
        }
        println!();
    }

    println!("Gate: BlockMasks must win at n >= 4 on 64- and 128-byte spans.");
}

fn bench_memchr(buf: &[u8], delims: &[u8], iters: usize) -> u64 {
    let mut acc = 0u64;
    let start = Instant::now();
    for _ in 0..iters {
        for &d in delims {
            // Simulate the scanner's pattern: search from a random offset
            let off = acc as usize % buf.len().max(1);
            if let Some(pos) = memchr::memchr(d, &buf[off..]) {
                acc = acc.wrapping_add((pos as u64).wrapping_add(1));
            } else {
                acc = acc.wrapping_add(1);
            }
        }
    }
    let elapsed = start.elapsed().as_nanos() as u64;
    // Prevent optimization
    black_box(acc);
    elapsed / iters as u64
}

fn bench_blockmasks(buf: &[u8], delims: &[u8], iters: usize) -> u64 {
    use rypipe_core::block_masks::BlockMasks;
    let mut acc = 0u64;
    let start = Instant::now();
    for _ in 0..iters {
        let mut bm = BlockMasks::new(buf, DELIMS);
        for &d in delims {
            let off = acc as usize % buf.len().max(1);
            if let Some(pos) = bm.next(off, d) {
                acc = acc.wrapping_add((pos as u64).wrapping_add(1));
            } else {
                acc = acc.wrapping_add(1);
            }
        }
    }
    let elapsed = start.elapsed().as_nanos() as u64;
    black_box(acc);
    elapsed / iters as u64
}

#[inline(always)]
fn black_box<T>(v: T) -> T {
    std::hint::black_box(v)
}
