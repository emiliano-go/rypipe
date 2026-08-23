# Parallelism

Parallel mode uses `rayon` to parse chunks concurrently. Understanding how `rayon` schedules work and how `num_chunks` maps to hardware helps avoid the common mistake of over-parallelizing.

## The `num_chunks` formula

A safe starting point is:

```
num_chunks = 4 * physical_cores
```

For a CPU-bound parser, this provides enough tasks to keep all cores busy even when chunks finish at different speeds. For a memory-bandwidth-bound parser, fewer chunks may be better because each chunk contends for the same DRAM channels and caches.

The built-in `bench_throughput` example is a simple TSV adapter. On a 12-core/24-thread Ryzen 9 5900X, parallel mode is slightly slower than single-threaded parse because the parser is so fast that chunk overhead dominates. Real adapters with heavier parsing usually see a win.

## How `rayon` schedules chunks

`ParallelExecutor` calls `rayon::par_iter` over the chunk ranges. `rayon` maintains a thread pool sized to the number of logical cores and uses work-stealing to balance load. Each chunk is parsed by one thread into its own `TableBuilder`.

Key properties:

- Tasks are not pinned to cores. The OS migrates them based on load.
- Work-stealing helps when chunks have variable cost.
- The global thread pool is shared with other `rayon` users in the same process.

## Hyperthreading

`rayon` uses logical cores by default. On a CPU with SMT (hyperthreading), two logical cores share execution units, L1, and L2. For memory-bandwidth-bound parsers, logical cores may not add much throughput. For CPU-bound parsers, they often add 10-30%.

If you want to exclude hyperthreads, set the `RAYON_NUM_THREADS` environment variable to the physical core count before starting Python:

```bash
export RAYON_NUM_THREADS=12  # physical cores only
python script.py
```

## NUMA and cache effects

On multi-socket or large NUMA machines, memory bandwidth and latency depend on which socket owns the buffer. `rayon` does not bind tasks to NUMA nodes, so a chunk parsed on socket 1 may read input allocated on socket 0.

For maximum throughput on NUMA hardware:

- allocate the input buffer on the same node that will do most of the parsing;
- pin the Python process to one socket if the file fits in one node's RAM;
- expect lower scaling when the file is larger than a single node's memory.

L3 cache size also matters. If the working set for one chunk fits in L3, scaling is good. If chunks are larger than L3, all cores contend for DRAM and speedup flattens.

## Why too many chunks hurt

More chunks are not always better. Each chunk pays fixed costs:

- `Splitter` validation and boundary search;
- `RecordParser` setup (readers, buffers, state);
- `TableBuilder` allocation and finish;
- Arrow export and, on the merge path, builder merging.

When the number of chunks grows, these fixed costs multiply. At some point the cost of starting a chunk exceeds the parsing work inside it. The result is lower throughput and higher memory use.

Too many chunks also increase peak RSS because each chunk holds its own builder until all chunks finish. On the merge path, all builders must coexist before the serial merge begins.

## Measuring speedup

Run the same parse at several chunk counts and plot throughput:

```bash
python benchmarks/bench_throughput.py --chunks 1 --output c1.json
python benchmarks/bench_throughput.py --chunks 4 --output c4.json
python benchmarks/bench_throughput.py --chunks 8 --output c8.json
python benchmarks/bench_throughput.py --chunks 16 --output c16.json
python benchmarks/bench_throughput.py --chunks 32 --output c32.json
```

Look for the elbow where adding chunks stops helping. Also measure RSS at each point; sometimes the fastest setting is not the most memory-efficient.

## Summary

- Start with `chunks = 4 * physical_cores`.
- Reduce chunks for simple, memory-bandwidth-bound parsers.
- Consider `RAYON_NUM_THREADS` to test physical-core-only behavior.
- Measure throughput and RSS; do not assume more parallelism is faster.
