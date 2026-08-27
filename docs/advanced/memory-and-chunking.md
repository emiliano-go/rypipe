# Memory and chunking

`rypipe` tries to parse as fast as the hardware allows while staying inside a memory budget. Two knobs control that trade-off:

- `memory`: maximum bytes the parser should hold in flight. A string like `"512MiB"` is parsed into bytes.
- `chunks`: number of chunks for parallel mode. More chunks improve load balancing but increase scheduling overhead.

This page explains how `BoundedExecutor` enforces the budget and how to size chunks for files larger or smaller than RAM.

## How `BoundedExecutor` works

`BoundedExecutor::run` keeps peak memory near the configured budget:

1. Opens the file via `InputBuffer`.
2. Estimates `bytes_per_row` from `Splitter::estimate_bytes_per_row`.
3. Computes `rows_per_batch` from the budget.
4. Splits the file into batches sized to fit the memory budget, capped at 256
   split points as an internal safeguard against pathological chunk counts.
5. Parses each batch into a `TableBuilder`, exports it to a `RecordBatch`, and resets the builder.
6. Returns a `Vec<RecordBatch>`; the caller concatenates.

The budget covers builder storage: string arenas, numeric buffers, and validity bitmaps. It does not include the input buffer, Arrow export buffers, or downstream pandas conversion. Set the budget lower than total RAM.

## Sizing the memory budget

A reasonable starting point for a workstation is 500 MiB. For a server with many concurrent parsers, divide available RAM by the expected concurrency. For embedded or container workloads, use 128 MiB or less.

The budget is a target, not a hard limit. Spikes can happen when:

- a batch contains an unusually wide row;
- a string column receives a very large value;
- the `bytes_per_row` estimate was wrong because of high variance.

If you see RSS overshoots, lower the budget and add more batches.

## Sizing chunks for parallel mode

Rule of thumb for parallel mode:

```
chunks = 4 * physical_cores
```

Finer chunks even out variable record parse times. Beyond 4-8x core count, synchronization overhead usually wins. Measure with your data; text-heavy formats benefit from fewer chunks because per-chunk setup dominates.

For a CPU-bound parser on many cores, start with 4x physical cores and increase until throughput flattens. For a memory-bandwidth-bound parser, fewer chunks may be better because each chunk touches the same memory hierarchy.

## Impact of row size variance

`BoundedExecutor` uses `bytes_per_row` to convert a byte budget into a row count. If rows vary in size, the row count can be wrong in either direction:

- Underestimate: a batch exceeds the budget and RSS spikes.
- Overestimate: batches are tiny and overhead rises.

High variance is common in:

- XML with mixed text and attribute payloads;
- JSON with nested arrays or large string fields;
- log files with variable field counts.

For these formats, prefer a smaller memory budget and more batches, or use stream mode with a conservative row estimate.

## Files larger than RAM

Stream mode is designed for this case. The input buffer is dropped before parsing, so mapped pages are released before downstream work starts. Each batch is parsed, exported, and discarded independently.

Tips:

- Use `prefault=False` so the kernel can drop pages behind the reader.
- Set `memory` to a fraction of RAM (for example, 25%).
- Avoid `auto_dict`; it forces a full table merge in parallel mode.
- Sink directly to Parquet or another stream-friendly format instead of building a pandas DataFrame.

## Files smaller than RAM

For small files, columnar mode is usually fastest. There is no chunk setup, no rayon scheduling, and no merge step. The entire file is parsed in one pass and exported once.

If the file is small but the parser is slow (for example, complex XML), parallel mode may still win despite overhead. Benchmark both.

## Memory model

- `InputBuffer::Mmap` maps the file and applies `MADV_WILLNEED` (prefault) or `MADV_SEQUENTIAL` (RSS-sensitive) advice on Unix. The mapping is dropped before Arrow export, so no borrowed bytes outlive it.
- `InputBuffer::Owned` simply reads the file into a `Vec<u8>`.
- `StrColumn` owns its bytes; Arrow arrays are built from owned buffers.
- Numeric columns use dense `Vec<Option<T>>`.

## Summary

- Use `memory` to cap builder storage; leave headroom for export and downstream work.
- Start with `chunks = 4 * physical_cores` and tune by measurement.
- Reduce batch size when row size variance is high.
- Use stream mode for files larger than RAM; use columnar mode for small files.
