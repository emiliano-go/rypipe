# Performance

`rypipe` is designed to keep parsing fast: arena string storage, SIMD UTF-8
validation, zero-copy event parsing, GIL-free Rust work, parallel chunking, and
a bounded-memory streaming path. This page is a summary reference; see the
[Architecture](./architecture/) and [Advanced](./advanced/) sections for
detailed explanations.

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
| `Pipeline::read_path` | 5,000,000 | 1.18 s | 4.23 M | 253 | 290 MB |
| `Pipeline::read_path_par` (4 chunks) | 5,000,000 | 1.33 s | 3.76 M | 225 | 388 MB |
| `Pipeline::read_path_par` (8 chunks) | 5,000,000 | 1.35 s | 3.71 M | 222 | 486 MB |
| `Pipeline::read_path_stream` (64 MiB) | 5,000,000 | 1.92 s | 2.61 M | 156 | 693 MB |

Single-thread parse is fastest for this simple TSV adapter because chunk
overhead dominates. Parallel mode trades a small throughput drop for much
higher CPU utilization and scales better with heavier parsers. The bounded
streaming path keeps intermediate batches near the 64 MiB budget but has higher
peak RSS because of the Arrow export buffer.

## Quick tuning reference

| Knob | Recommended | See also |
|------|-------------|----------|
| `num_chunks` | `4 * physical_cores` | [Parallelism](./advanced/parallelism.md) |
| `memory` | 500 MB for workstations | [Memory and chunking](./advanced/memory-and-chunking.md) |
| `use_mmap` + `prefault` | `True`/`True` for cached files; `True`/`False` for large | [I/O tuning](./advanced/io-tuning.md) |
| `auto_dict` | `False` for throughput; `True` for compression | [Dictionary encoding](./advanced/dictionary-encoding.md) |
| `field_types` | Declare known numeric columns | [Schema and types](./advanced/schema-and-types.md) |
| `schema_order` | Provide when column order matters | [Schema and types](./advanced/schema-and-types.md) |

## Fast path vs merge path

`ParallelExecutor` has two internal paths:

- **Fast path**: when `auto_dict` is false and schemas are consistent across
  chunks, each chunk is exported as its own `RecordBatch` in parallel. No
  serial merge. Compare filters are applied per-row during parse AND
  reapplied after export via `apply_compare_filter`, so they do not force
  the merge path.
- **Merge path**: when `auto_dict` is enabled or schemas are inconsistent,
  chunk builders are merged sequentially before export. Peak RSS is higher.

If you need both a `Compare` filter and maximum throughput, the fast path
already handles it (Compare is applied post-export). For `auto_dict` with
Compare, the incremental dict path preserves the fast path when schemas
are consistent.

## Example: `crxml` adapter

`crxml` (Crystal Reports XML, at `docs/crxml-adapter.md`) is the reference
for what a good adapter looks like: hand-rolled `memchr` scanner, shared
between columnar and streaming paths, `wants`-driven skip-bytes, and
parallel fast path. On a Ryzen 5800X: **714 MB/s single, 2.6-3.0 GB/s
parallel**, with streaming at **508 MB/s** (2x over `quick-xml`).

## Future work

- A generic streaming `RecordParser` could support chunked async input.
- Dictionary encoding could be made incremental across chunks to recover the
  fast path for `auto_dict=True`.

## See also

- [Architecture](./architecture/): engine design and fast/merge paths.
- [Execution modes](./advanced/execution-modes.md): when to use each mode.
- [Memory and chunking](./advanced/memory-and-chunking.md): budget sizing.
- [Parallelism](./advanced/parallelism.md): chunk scaling.
- [Profiling](./advanced/profiling.md): how to measure performance.
