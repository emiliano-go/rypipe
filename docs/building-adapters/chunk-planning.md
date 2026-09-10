# Chunk Planning { #chunk-planning }

The engine decides how many chunks to create based on file size, thread count,
and execution mode. This is controlled by `plan_chunk_count` and related
constants.

## Constants { #constants }

```rust
/// Minimum chunk size in bytes. Sub-MB chunks collapse throughput.
pub const MIN_CHUNK_BYTES: usize = 2 << 20; // 2 MiB

/// Maximum number of split chunks.
pub const MAX_SPLIT_CHUNKS: usize = 1024;
```

## `plan_chunk_count` { #plan_chunk_count }

```rust
pub fn plan_chunk_count(bytes: usize, threads: usize, mode: SplitMode) -> usize
```

Returns the number of chunks for a given file size, thread count, and mode.

```rust
pub enum SplitMode {
    Parallel,   // peaks at ~4 MB chunks
    Streaming,  // peaks at ~2 MB chunks
}
```

**Algorithm:**

1. `by_size = bytes / MIN_CHUNK_BYTES` (chunk count from file size)
2. `cap = 16 * threads` (Parallel) or `8 * threads` (Streaming)
3. `result = min(by_size, cap).max(threads).min(MAX_SPLIT_CHUNKS)`

## Why 2 MiB floor { #why-2-mib-floor }

Sub-1 MB chunks collapse throughput due to per-chunk fixed cost (thread
dispatch, cache cold start). Measured: 100 MB at par128 (0.78 MB chunks)
= 2,265 MB/s vs par16 (6.25 MB chunks) = 3,735 MB/s.

## Why Parallel and Streaming differ { #why-parallel-and-streaming-differ }

- **Parallel** peaks at ~4 MB chunks (measured on 533 MB, 16 cores)
- **Streaming** peaks at ~2 MB chunks (measured with bounded memory)

The streaming path has additional per-chunk overhead from the channel-based
backpressure, so smaller chunks help amortize it.

## Auto-tuning in Python { #auto-tuning-in-python }

`CrystalXMLSource` uses the same formula:

```python
t = threads if threads > 0 else cpu_count()
file_bytes = path.stat().st_size
num_chunks = max(t, min(16 * t, file_bytes // (2 * 1024 * 1024)))
```

This matches `plan_chunk_count(bytes, threads, SplitMode::Parallel)`
(`MIN_CHUNK_BYTES` is 2 MiB).

## Build and test { #build-and-test }

Verify the planner's floor and thread clamp against the real constants:

```console
$ cargo test chunk_planning
running 1 test
test tests::chunk_planning_respects_floor ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 9 filtered out; finished in 0.00s
```

The test calls `rypipe_core::decoder::plan_chunk_count` directly:
`plan_chunk_count(1024, 4, Parallel)` returns 4 (clamped up to the thread
count), and a 1 GiB file on 8 threads returns 128 in Parallel mode and 64
in Streaming mode, never exceeding `MAX_SPLIT_CHUNKS` (1024).

## What the end user sees { #what-the-end-user-sees }

Chunk sizing is automatic, but the user's choice of execution mode feeds
the planner. Passing `memory=` switches the adapter into streaming mode
(`SplitMode::Streaming`), where the engine plans smaller chunks to hold
the memory budget:

```python
from rypipe_log import LogSource

# Streaming mode: chunk count and sizes come from plan_chunk_count.
for batch in LogSource("sample.log").iter_record_batches(
    memory="64MiB", batch_size=10_000
):
    print(batch.num_rows)
```
