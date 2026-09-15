# Throughput benchmark

`bench_throughput.rs` measures the core pipeline on five million generated TSV
rows across single-threaded, parallel, and bounded-memory modes.

## Why this shape

The example writes generated data to a temporary TSV file, then runs one
typed plan. `id` and `count` become `Int64`; `amount` becomes `Float64`. Using
the same parser, plan, and file for every mode keeps the comparison focused on
execution strategy.

Each result uses a warmup and adaptive median sampling. Sampling stops after
at least three runs when `1.31 * CoV <= 5%`, or at 31 runs. Parallel runs use
4, 8, and 16 chunks. Streaming uses a 64 MiB `MemoryBudget`. `--only-config`
isolates one configuration for a separate process run. Output includes commit
SHA, dirty-build state, transparent huge-page defrag mode when available, RSS
split into anonymous and file memory, rows/s, MB/s, and CoV.

The build-SHA check prevents stale binaries from producing plausible numbers.
Use `--allow-dirty` only when the binary was intentionally built from a dirty
checkout. A CoV above 8% gets a marker. Parallel runs also print chunk split,
sum, maximum, and count profiles on stderr.

## Run it

```console
cargo run --release -p rypipe-core --example bench_throughput
cargo run --release -p rypipe-core --example bench_throughput -- --only-config 2
cargo run --release -p rypipe-core --example bench_throughput -- --allow-dirty
```

The benchmark is Linux-aware for THP and RSS details but still runs on other
platforms with unavailable fields shown as `unknown` or `N/A`. Generated data
does not represent a production format distribution. Results compare this
engine and build on this machine; they do not establish a cross-machine speed
ranking or a memory guarantee for arbitrary records.

Source: [`bench_throughput.rs`](../../crates/rypipe-core/examples/bench_throughput.rs).
