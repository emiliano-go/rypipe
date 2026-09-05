# Advanced rypipe { #advanced-rypipe }

This section is for adapter authors and power users who want to understand why rypipe is fast and how to keep it fast. It assumes you have read the Python API, Rust API, and Architecture pages.

Each page takes one optimization topic and explains the mechanism, the tuning knobs, and the trade-offs.

## Roadmap { #roadmap }

| Page | What you will learn |
|------|---------------------|
| [Fusion](./fusion.md) | How `RenameFields`, `DropFields`, `CastTypes`, and `FilterRows` (keyword form or compiled lambda) are rewritten into a single `ExecutionPlan`; what is fusable and what falls back to Python. |
| [Execution modes](./execution-modes.md) | `stream`, `columnar`, `parallel`, and `auto`; when each wins on memory, latency, and throughput. |
| [Memory and chunking](./memory-and-chunking.md) | How `BoundedExecutor` enforces a memory budget; sizing chunks for files larger or smaller than RAM. |
| [Parallelism](./parallelism.md) | `rayon` internals; the `num_chunks` formula; why too many chunks hurt; measuring speedup. |
| [Dictionary encoding](./dictionary-encoding.md) | Arrow dictionaries in rypipe; `auto_dict` heuristics; the merge path vs the fast path; explicit `dictionary_columns`. |
| [Schema and types](./schema-and-types.md) | Using `schema_order` and `field_types` to skip inference passes, stabilize column order, and enable numeric compare filters. |
| [I/O tuning](./io-tuning.md) | `mmap` vs buffered reads; `prefault`; OS page cache; storage class considerations. |
| [Adapter design](./adapter-design.md) | Writing a fast `Splitter` and `RecordParser`; `memchr`; comments, CDATA, and quoted fields; borrowing strings; sparse rows; `sink.wants`. |
| [Profiling](./profiling.md) | Profiling with `perf`, `cargo flamegraph`, and the `bench_throughput` example; measuring RSS; separating Python and Rust time. |
| [Anti-patterns](./anti-patterns.md) | Common mistakes that silently remove fusion, increase memory, or waste CPU. |
| [Case study: crxml](./case-study-crxml.md) | How crxml reaches ~4.2 GB/s by combining the techniques from the other pages. |

## Quick checklist { #quick-checklist }

- [ ] Provide `schema_order` and `field_types` when the schema is known.
- [ ] Use `dictionary_columns` for low-cardinality strings; be careful with `auto_dict` in parallel mode.
- [ ] Implement `_read_arrow(plan_overrides=...)` so fused stages stay in Rust.
- [ ] Pick `columnar` for tables that fit in RAM, `parallel` for large cached files, and `stream` for huge or row-oriented consumers.
- [ ] Tune `chunks = 4 * physical_cores` and measure with your data.
- [ ] Export Arrow from Rust and sink directly to Parquet when possible.
