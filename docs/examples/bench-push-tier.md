# Push-tier benchmark

`bench_push_tier.rs` measures parsing plus per-field insertion into
`TableBuilder`, while leaving out row finalization and Arrow export.

## Why this shape

The inline parser reads UTF-8 TSV-like records. Each tab-separated token must
look like `key=value`; values stay borrowed from the input. The `PushOnly`
sink delegates field insertion to `TableBuilder` but calls `advance_row()` at
row end. That deliberately skips `finish_row()` work such as null filling,
dirty-mask handling, and filter checks. The result isolates the cost of
resolving and pushing fields.

One warmup runs before seven measured runs. The page reports median, best,
worst, coefficient of variation, rows, MB/s, and nanoseconds per field. The
row estimate comes from newline count, and field count comes from the first
line, so malformed or heterogeneous input can make the derived rate less
meaningful.

## Run it

```console
cargo run --release -p rypipe-core --example bench_push_tier -- path/to/file.tsv
```

For hardware counters, build once and pass the executable to `perf stat` on
Linux:

```console
perf stat -e cycles,instructions,branch-misses,L1-dcache-load-misses \
  target/release/examples/bench_push_tier path/to/file.tsv
```

Use the same file, build, and machine when comparing changes. This is a push
phase measurement. It does not measure `finish_row()`, Arrow conversion,
parallel scheduling, or source I/O setup.

Source: [`bench_push_tier.rs`](../../crates/rypipe-core/examples/bench_push_tier.rs).
