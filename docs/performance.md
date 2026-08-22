# Performance

`rypipe` inherits the performance profile of the original crxml engine: arena
string storage, SIMD UTF-8 validation, zero-copy event parsing, GIL-free
parsing, and parallel chunking. This page shows measured numbers and explains
the tuning knobs.

## Measured throughput

Hardware: Linux workstation, 100 MB synthetic Crystal Reports XML file,
90,384 rows, Python 3.12, release build.

| Path | Time | Rows/s | MB/s |
|------|------|--------|------|
| `read_to_columnar` | 0.395 s | 229k | 253 |
| `read_to_columnar_multi` (4 chunks) | 0.267 s | 339k | 375 |
| `read_to_columnar_par` (8 chunks) | 0.098 s | 923k | 1,021 |
| Stream iteration | 0.449 s | 201k | 223 |
| Columnar iteration | 0.318 s | 284k | 314 |
| Parallel iteration | 0.159 s | 567k | 627 |
| Columnar → Arrow Table | 0.224 s | 403k | 446 |
| Parallel → Arrow Table | 0.050 s | 1.80M | 1,989 |
| Columnar → DataFrame | 0.214 s | 422k | 467 |
| Parallel → DataFrame | 0.055 s | 1.64M | 1,808 |

Memory stayed around **530 MB RSS** for native exports; Python object overhead
added ~60 MB for iteration or DataFrame conversion.

## Tuning knobs

### Number of chunks (`num_chunks`)

`rypipe-core::ParallelExecutor` accepts `num_chunks`. More chunks improve load
balancing but add scheduling overhead. crxml uses `threads * 4` based on VTune
measurements on a 24-core machine. For most workloads, 2-4 times the number of
logical cores is a good starting point.

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
Interface export re-acquires the GIL briefly. For `read_to_columnar_par`, the
entire parallel parse runs outside the GIL.

## Profiling

Build with the `profiling` profile for symbols:

```bash
cargo build --profile profiling -p rypipe-core
```

Then use `perf`, `cargo flamegraph`, or ` samply` to profile.

## Future work

- CSV and NDJSON adapters should be faster than XML because their splitters and
  parsers are simpler.
- A generic streaming `RecordParser` could support chunked async input.
- Dictionary encoding could be made incremental across chunks to recover the
  fast path for `auto_dict=True`.
