# BlockMasks microbenchmark

`bench_blockmasks.rs` answers one narrow question: when a scanner needs to
search for several delimiters, does one `BlockMasks` load beat several
`memchr` calls?

## Why this shape

The input is synthetic. Each span contains `x` bytes with eight delimiters
placed at fixed positions. Spans are 32, 64, 128, and 512 bytes. The query
uses 1, 2, 3, 5, or 8 delimiters from `<>"'=`. Fixed placement makes runs
repeatable enough for a microbenchmark while still exercising hits and
misses. It is not a corpus model.

The baseline calls `memchr` once per delimiter. The candidate constructs one
`BlockMasks` value, then asks it for each delimiter. Both loops update an
accumulator and pass it through `std::hint::black_box`, so the compiler cannot
discard the work. Timing uses 500,000 iterations and reports nanoseconds per
iteration plus baseline/candidate ratio.

The source prints a gate: BlockMasks should win for four or more delimiters on
64- and 128-byte spans. Treat that as a decision rule for this implementation,
not a universal CPU claim.

## Run it

```console
cargo run --release -p rypipe-core --example bench_blockmasks
```

Compare ratios within one machine and build. Change span or delimiter density
only when testing a new scanner workload. Repeat runs if a result is close to
1.0x. This program does not report CPU counters, allocation counts, or parser
throughput, and it does not prove end-to-end scanner correctness.

Source: [`bench_blockmasks.rs`](../../crates/rypipe-core/examples/bench_blockmasks.rs).
