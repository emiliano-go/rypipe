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
| `Pipeline::read_path` | 5,000,000 | 1.18 s | 4.23 M | 253 | 290 MB |
| `Pipeline::read_path_par` (4 chunks) | 5,000,000 | 1.33 s | 3.76 M | 225 | 388 MB |
| `Pipeline::read_path_par` (8 chunks) | 5,000,000 | 1.35 s | 3.71 M | 222 | 486 MB |
| `Pipeline::read_path_stream` (64 MiB) | 5,000,000 | 1.92 s | 2.61 M | 156 | 693 MB |

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
dictionary encoding. This reduces memory and can speed up downstream operations.
In parallel mode, the incremental dict path performs per-chunk upgrade in
parallel, then unifies dictionaries (tiny serial). The fast per-chunk export
path is preserved when schemas are consistent across chunks.

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

## Example: `crxml` adapter

`crxml` (Crystal Reports XML, at `docs/crxml-adapter.md`) is the reference for what a good adapter looks like: hand-rolled `memchr` scanner `crxml-core/src/xml/scanner.rs` instead of `quick-xml`, shared between columnar `scan_chunk` and streaming `scan_one_row` + `RowSink` `crxml-core/src/lib.rs:564`, `Vec<ColumnBuilder>`+`field_index` + `row_dirty` bitmask in `engine.rs:16`, `InputBuffer` `mmap` auto for >50 MB (`crxml-core` `auto_mmap`), and `wants`-driven skip-bytes. On a Ryzen 5800X it holds **714 MB/s single / 2.6-3.0 GB/s parallel** (1 GB, 926k rows) and **4183 MB/s** with `drop_all` pushdown, with streaming now **508 MB/s** (2× over `quick-xml`'s 251). See `benchmarks/bench_extended.py` (104 benchmarks/file) for the full matrix (native, source×sink, pushdowns, chunk/bounded/batch/pipeline).

## Future work

- A generic streaming `RecordParser` could support chunked async input.
- Dictionary encoding could be made incremental across chunks to recover the
  fast path for `auto_dict=True`.

## BlockMasks: negative result (closed)

BlockMasks precomputes 64-byte SIMD bitmasks per delimiter and answers
multiple byte-search queries via bit operations. The idea: one AVX2/SSE2
load per block amortized across N delimiter searches in the same span.

**Microbench result (i5-1335U, release build, 500K iterations):**

| Span | n=1 | n=2 | n=3 | n=5 | n=8 |
|------|-----|-----|-----|-----|-----|
| 32B | memchr 1.7x | memchr 1.9x | memchr 2.3x | memchr 3.7x | memchr 3.5x |
| 64B | memchr 1.3x | memchr 1.3x | memchr 1.1x | memchr 1.3x | memchr 1.3x |
| 128B | memchr 1.0x | memchr 1.1x | memchr 1.8x | **BM 1.2x** | memchr 1.4x |
| 512B | memchr 1.4x | memchr 1.7x | memchr 1.2x | memchr 1.8x | memchr 1.8x |

**Gate:** BlockMasks must win at n ≥ 4 on 64- and 128-byte spans.

**Result:** Fails the gate. At 64 bytes memchr wins at all query counts.
At 128 bytes, only n=5 shows a marginal win (1.2x) while n=8 regresses
(0.73x). The mask-compute overhead (~20 instructions per block) cannot
amortize across fewer than ~6 queries, and XML field spans rarely exceed
100 bytes with more than 5 delimiter types active.

**Verdict:** Close permanently. The scalar-loop `memchr`-based scanner
remains the right choice for XML. If delimiter density increases (e.g.,
JSON with 8+ active delimiters per 64-byte block), revisit with a
worst-case microbench first.

## See also

- [Architecture](./architecture/): engine design and fast/merge paths.
- [Rust API](./rust-api.md): tuning `num_chunks` and `memory` from Rust.
- [Python API](./python-api.md): the same knobs from Python.
