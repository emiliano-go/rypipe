---
title: "Memory and chunking"
---

# Memory and chunking { #memory-and-chunking }

`rypipe` uses a memory budget to size batches and in-flight work. The default
budget is a soft target. Strict Rust callers can turn tracked capacity checks
into errors. Two knobs control batch planning:

- `memory`: batch sizing allowance in bytes. Pass a plain
  integer (bytes) or a string with a unit. `rypipe` understands `B`, `KB`,
  `MB`, `GB`, `TB` (decimal, 1000-based) and `KiB`, `MiB`, `GiB`, `TiB`
  (binary, 1024-based). Adapters may parse strings differently: `crxml`
  accepts the same units with 1024-based multipliers throughout
  (case-insensitive, no space before the unit), so `KB` means 1024 bytes
  there, not 1000. Check your adapter's documentation.
- `chunks`: number of chunks for parallel mode. More chunks improve load
  balancing but increase scheduling overhead.

This page explains what the budget covers and how chunk planning changes with
file size and execution mode.

## How `BoundedExecutor` works { #how-boundedexecutor-works }

`BoundedExecutor::run` uses the configured budget to size work batches:

1. Opens the file via `InputBuffer`.
2. Estimates `bytes_per_row` from `Splitter::estimate_bytes_per_row`.
3. Estimates input rows with a conservative `budget / 64` target, then adjusts
   output row targets from observed builder capacity.
4. Splits the file into batches sized to fit the memory budget, capped at 100,000
   split points by default. This executor cap is separate from the default
   splitter's 1024-point cap and is configurable via `max_split_chunks` on the
   plan. A batch can still exceed the allowance when one record or either cap
   prevents finer splitting.
5. Parses each batch into a `TableBuilder`, exports it to a `RecordBatch`, and resets the builder.
6. Returns a `Vec<RecordBatch>`; the caller concatenates.

The engine divides its allowance conservatively across stages. Tracked work
includes builder columns, row buffers, merge state, Arrow batches, queues, and
some input or chunk storage. Caller-owned input, adapter-private allocations,
and output retained by the caller remain outside this accounting. A check can
observe capacity after an allocation has grown, so strict mode is an error
policy, not an allocator or OS limit.

## Sizing the memory budget { #sizing-the-memory-budget }

A reasonable starting point for a workstation is 500 MiB. For a server with many concurrent parsers, divide available RAM by the expected concurrency. For embedded or container workloads, use 128 MiB or less.

By default, the budget guides batch sizing and is not a hard process-memory
limit. Spikes can happen when:

- a batch contains an unusually wide row;
- a string column receives a very large value;
- the `bytes_per_row` estimate was wrong because of high variance.

Lower the budget when you need smaller engine batches. RSS can still exceed it
because several stages and untracked allocations coexist.

## Strict budget errors { #strict-budget-errors }

Rust callers that need an allocation allowance can opt into strict checks:

```rust
let budget = MemoryBudget::new(64 * 1024 * 1024).with_strict(true);
let batches = pipeline.read_bytes_stream(data, budget)?;
```

Strict mode returns `Error::Memory { used, limit }` when tracked builder,
parser, merge, input, or output work exceeds the allowance. The Python bridge
maps this error to `MemoryError`. It checks at
allocation and handoff points, so a long row can fail even when the estimated
row count looked safe. A failure after earlier batches is still an error from
the iterator or parallel stream; callers must handle it instead of treating
the already-consumed batches as a complete result.

`with_strict(false)` is the default. Strict mode is an accounting policy for
engine-owned work. It does not cap OS RSS, mmap pages, adapter-owned memory,
Arrow allocations outside the tracked points, or downstream objects.

## Sizing chunks for parallel mode { #sizing-chunks-for-parallel-mode }

Rule of thumb for parallel mode:

```
chunks = 4 * physical_cores
```

Finer chunks even out variable record parse times. Beyond 4-8x core count, synchronization overhead usually wins. Measure with your data; text-heavy formats benefit from fewer chunks because per-chunk setup dominates.

For a CPU-bound parser on many cores, start with 4x physical cores and increase until throughput flattens. For a memory-bandwidth-bound parser, fewer chunks may be better because each chunk touches the same memory hierarchy.

## Impact of row size variance { #impact-of-row-size-variance }

`BoundedExecutor` uses `bytes_per_row` to convert a byte budget into a row count. If rows vary in size, the row count can be wrong in either direction:

- Underestimate: a batch exceeds the budget and RSS spikes.
- Overestimate: batches are tiny and overhead rises.

High variance is common in:

- XML with mixed text and attribute payloads;
- JSON with nested arrays or large string fields;
- log files with variable field counts.

For these formats, prefer a smaller memory budget and more batches, or use stream mode with a conservative row estimate.

## Files larger than RAM { #files-larger-than-ram }

Stream mode is designed for this case. Uncompressed mmap input uses the mapping
for planning, then reads chunks from the file and releases the mapping before
parsing. Compressed or non-mmap input may retain a full decompressed/read
buffer. Each batch is parsed, exported, and discarded independently, but no
mode guarantees an OS RSS ceiling.

Tips:

- Use `prefault=False` so the kernel can drop pages behind the reader.
- Set `memory` to a fraction of RAM (for example, 25%).
- Avoid `auto_dict`; it forces a full table merge in parallel mode.
- Sink directly to Parquet or another stream-friendly format instead of building a pandas DataFrame.

## Files smaller than RAM { #files-smaller-than-ram }

For small files, columnar mode is usually fastest. There is no chunk setup, no rayon scheduling, and no merge step. The entire file is parsed in one pass and exported once.

If the file is small but the parser is slow (for example, complex XML), parallel mode may still win despite overhead. Benchmark both.

## Memory model { #memory-model }

- `InputBuffer::Mmap` may map an uncompressed file and apply `MADV_WILLNEED` (prefault) or `MADV_SEQUENTIAL` advice on Unix. Stream execution drops that mapping after planning and reads chunks with seek/read. Other execution modes can retain the input through parsing and export.
- `InputBuffer::Owned` simply reads the file into a `Vec<u8>`.
- `StrColumn` owns its bytes; Arrow arrays are built from owned buffers.
- Numeric columns use `PrimColumn<T>` (flat Vec + ValidityBitmap).

## Summary { #summary }

- Use `memory` to size tracked engine work; leave headroom for export and downstream work.
- `BoundedExecutor` combines splitter estimates with conservative per-stage
  allowances, then adjusts output targets from observed builder capacity. It
  caps the batch count at 100,000 split points (`max_split_chunks` overrides
  the cap).
- Start with `chunks = 4 * physical_cores` and tune by measurement.
- Reduce the budget when row size variance is high.
- Use stream mode for files larger than RAM; use columnar mode for small files.
