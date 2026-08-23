# Performance

`rypipe` is designed to keep parsing fast: arena string storage, SIMD UTF-8
validation, zero-copy event parsing, GIL-free Rust work, parallel chunking, and
a bounded-memory streaming path. This page explains how to measure and tune the
engine.

## Measured throughput

The numbers below come from the built-in `bench_throughput` example. It uses a
tiny inline TSV-like adapter so the result measures the engine, not an external
parser.

Run it yourself:

```bash
cargo run --release -p rypipe-core --example bench_throughput
```

Hardware: Linux workstation, AMD Ryzen 9 5900X, DDR4-3200, release build.

| Path | Rows | Time | Rows/s | MB/s | RSS |
|------|------|------|--------|------|-----|
| `Pipeline::read_path` | 5,000,000 | 1.31 s | 3.83 M | 229 | 290 MB |
| `Pipeline::read_path_par` (4 chunks) | 5,000,000 | 1.46 s | 3.43 M | 205 | 432 MB |
| `Pipeline::read_path_par` (8 chunks) | 5,000,000 | 1.49 s | 3.35 M | 200 | 529 MB |
| `Pipeline::read_path_stream` (64 MiB) | 5,000,000 | 1.98 s | 2.53 M | 151 | 629 MB |

Single-thread parse is fastest for this simple TSV adapter because chunk
overhead dominates. Parallel mode trades a small throughput drop for much
higher CPU utilization and scales better with heavier parsers. The bounded
streaming path keeps intermediate batches near the 64 MiB budget but has higher
peak RSS because of the Arrow export buffer.

Run the benchmark yourself:

```bash
# Rust example
cargo run --release -p rypipe-core --example bench_throughput

# Python wrapper that also writes JSON results
python benchmarks/bench_throughput.py --output .benchmarks/rypipe.json
```

## Tuning knobs

### Number of chunks (`num_chunks`)

`ParallelExecutor::parse` accepts `num_chunks`. More chunks improve load
balancing but add scheduling overhead. For most workloads, 2-4 times the number
of logical cores is a good starting point. Very fast parsers (like the simple
TSV adapter above) can become memory-bandwidth bound, so adding chunks beyond a
certain point stops helping.

### Memory budget (`memory`)

`BoundedExecutor` keeps intermediate builder storage under the given byte
budget. It does not include Arrow export or downstream pandas conversion; set
the budget lower than total RAM. A value like 500 MB is reasonable for
workstations.

### `use_mmap` and `prefault`

| Combination | Best for |
|-------------|----------|
| `use_mmap=True, prefault=True` | Speed when the file fits in RAM. |
| `use_mmap=True, prefault=False` | Large files where RSS matters. |
| `use_mmap=False` | Portability; reads into a `Vec<u8>`. |

`prefault=True` uses `MADV_WILLNEED` to fault the whole file up front.
`prefault=False` uses `MADV_SEQUENTIAL` so the kernel can drop pages behind the
reader.

### `auto_dict`

When `auto_dict=True`, string columns with low cardinality are upgraded to
dictionary encoding. This reduces memory and can speed up downstream operations,
but it forces a serial merge of all chunk builders in parallel mode, which
raises peak RSS and removes the fast per-chunk export path.

Use `auto_dict=True` when:

- Columns have many repeated values.
- You need smaller Arrow files or faster group-by/filter operations.

Use `auto_dict=False` when:

- Throughput is the top priority.
- Columns are high cardinality or already numeric.

### `field_types`

Casting strings to numbers during parse avoids storing intermediate strings and
lets numeric `Compare` filters use native kernels. If you know a column is
numeric, declare it.

### `schema`

Providing `schema_order` avoids the small cost of sorting columns at finish time
and makes output column order deterministic across chunks.

## Fast path vs merge path

`ParallelExecutor` has two internal paths:

- **Fast path**: when `auto_dict` is false and there is no `Compare` filter,
  each chunk is exported as its own `RecordBatch` in parallel. No serial merge.
- **Merge path**: when `auto_dict` or a `Compare` filter is enabled, chunk
  builders are merged sequentially before export. Peak RSS is higher.

If you need both a `Compare` filter and maximum throughput, consider filtering
after export in Python/Arrow instead.

## GIL behavior

All parse paths release the GIL during the heavy Rust work. The Arrow C Data
Interface export re-acquires the GIL briefly. For `read_path_par`, the entire
parallel parse runs outside the GIL.

## Profiling

Build with the `profiling` profile for symbols:

```bash
cargo build --profile profiling -p rypipe-core
```

Then use `perf`, `cargo flamegraph`, or `samply` to profile.

## Future work

- A generic streaming `RecordParser` could support chunked async input.
- Dictionary encoding could be made incremental across chunks to recover the
  fast path for `auto_dict=True`.

## See also

- [Architecture](./architecture.md): engine design and fast/merge paths.
- [Rust API](./rust-api.md): tuning `num_chunks` and `memory` from Rust.
- [Python API](./python-api.md): the same knobs from Python.
