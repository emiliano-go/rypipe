# Six-tier scanner benchmark

`bench_scanner_single.rs` decomposes one single-threaded parse into six
cost layers:

1. newline scanning;
2. parser traversal;
3. field location and resolution;
4. field extraction and push;
5. row finalization;
6. Arrow export.

## Why this shape

Without tiers, a faster or slower full parse does not say which layer moved.
Each function adds one layer and reports elapsed time, rows, fields, MB/s, and
nanoseconds per field. The final section subtracts adjacent tiers to show an
additive estimate for each layer. Assertions require locate and full parse to
visit the same nonzero row count, catching dead-code elimination and obvious
accounting mistakes.

With no argument, the program generates five million identical five-field TSV
rows. With a path, it reads that file and derives row and field counts from
newlines and the first line. Synthetic input gives a stable baseline; a real
file exposes data-shape effects.

## Run it

```console
cargo run --release -p rypipe-core --example bench_scanner_single
cargo run --release -p rypipe-core --example bench_scanner_single -- path/to/file.tsv
```

Linux counter run:

```console
perf stat -e cycles,instructions,branch-misses,L1-dcache-load-misses,dTLB-load-misses \
  target/release/examples/bench_scanner_single path/to/file.tsv
```

The subtraction is useful for locating cost, not a proof that costs are
independent. Timings include allocation and parser setup inside each tier,
and the generated workload says little about quoted fields, skew, malformed
records, or compressed input.

Source: [`bench_scanner_single.rs`](../../crates/rypipe-core/examples/bench_scanner_single.rs).
