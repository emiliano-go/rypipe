# Profiling { #profiling }

Optimization without measurement is guessing. This page describes how to profile rypipe pipelines and interpret the results.

## The `bench_throughput` example { #the-bench_throughput-example }

`crates/rypipe-core/examples/bench_throughput.rs` is a self-contained benchmark. It uses a tiny inline TSV-like adapter so the result measures the engine, not an external parser.

Run it:

```bash
cargo run --release -p rypipe-core --example bench_throughput
```

Or use the Python wrapper that writes JSON results:

```bash
python benchmarks/bench_throughput.py --output .benchmarks/rypipe.json
```

The output reports rows, time, rows per second, MB/s, and RSS. Use these to compare configurations.

## Release builds { #release-builds }

Always profile a release build. Debug builds are 10-50x slower and the profile will be dominated by unrelated overhead.

```bash
cargo build --release -p rypipe-core
```

For symbols without full debug overhead, use the `profiling` profile if it exists:

```bash
cargo build --profile profiling -p rypipe-core
```

## Profiling with `perf` { #profiling-with-perf }

On Linux:

```bash
perf record -g cargo run --release -p rypipe-core --example bench_throughput
perf report -g 'graph,0.5,caller'
```

Look for time spent in:

- `find_split_points`: splitter is expensive.
- `parse_chunk`: parser is the bottleneck.
- `StrColumn::push` or builder append: string allocation/copy dominates.
- Arrow export or compute kernels: export is expensive.
- Python GIL-related functions: Python boundary is the bottleneck.

## Flamegraphs { #flamegraphs }

`cargo flamegraph` produces an SVG flamegraph:

```bash
cargo install flamegraph
cargo flamegraph --release -p rypipe-core --example bench_throughput
```

Open `flamegraph.svg` in a browser. Wide bars are hot functions. Look for unexpected wide bars such as JSON serialization, Python dict construction, or allocations.

## Measuring RSS { #measuring-rss }

Use `/usr/bin/time -v` on Linux:

```bash
/usr/bin/time -v cargo run --release -p rypipe-core --example bench_throughput
```

Look at `Maximum resident set size (kbytes)`. Compare this across engine modes and chunk counts.

In Python, you can sample RSS during a run with `psutil`:

```python
import psutil, time, os

proc = psutil.Process(os.getpid())
peak = 0
while running:
    peak = max(peak, proc.memory_info().rss)
    time.sleep(0.01)
print(f"peak RSS: {peak / 1024 / 1024:.1f} MiB")
```

## Separating Python and Rust time { #separating-python-and-rust-time }

If the pipeline includes Python stages, wrap the Rust parse in `py.allow_threads` (PyO3) so the GIL is released. Profile the Python side separately with `cProfile`:

```bash
python -m cProfile -o profile.stats script.py
python -c "import pstats; pstats.Stats('profile.stats').sort_stats('cumtime').print_stats(20)"
```

If most time is in `_rypipe` native code, optimize Rust. If most time is in Python callables, move work into fused stages or Rust.

## What to vary { #what-to-vary }

When benchmarking, change one variable at a time:

- `chunks`: 1, 2, 4, 8, 16, 32.
- `memory`: 64 MiB, 256 MiB, 512 MiB, 1 GiB.
- `use_mmap` and `prefault`: all four combinations.
- `auto_dict`: on vs off.
- `field_types`: typed parse vs string inference.

Plot throughput vs RSS to find the Pareto frontier.

## Summary { #summary }

- Use `bench_throughput` as a baseline.
- Profile release builds with `perf` or `cargo flamegraph`.
- Measure RSS separately; throughput is not the only metric.
- Separate Python time from Rust time before optimizing.
