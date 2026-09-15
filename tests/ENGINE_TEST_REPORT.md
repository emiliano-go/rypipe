# Engine testing and code quality review

September 14, 2026. This pass covered core execution, Python bindings, public
sinks, and the Properties, INI, and LDIF adapters on Windows x86-64.

| Priority | Area | Evidence | Status |
| --- | --- | --- | --- |
| P0 | Boundary/parser fuzz | 469,822 executions across two AddressSanitizer campaigns, no failures | Bounded runs passed; not exhaustive |
| P0 | Core tests | Workspace all-features: 278 passed, 5 ignored; Clippy all-targets/all-features with warnings denied passed | Passed |
| P0 | Python/adapters | 215 passed, 15 warnings against rebuilt native extensions; isolated wheel: 138 passed | Passed |
| P0 | Packaging | Root wheel, all three adapter extensions, and generated template rebuilt in release mode; adapter Rust tests: 22 passed; template smoke: 1 passed | Passed |
| P1 | Stateful boundaries | 9 tests; LF/CRLF, comments, escapes, custom continuation, blank separators, bounds, skip regions, 1 MiB line, single/parallel/stream parity | Passed, including 4,096 cases per property test |
| P1 | Input handling | 3 tests: empty mmap/read input, file identity during stream planning, validation/parse panic parity | Passed |
| P1 | Robustness contracts | 11 tests covering row lifecycle, partial merges, deferred errors, schema, dictionary, observers, cache concurrency and poison recovery | Passed |
| P1 | Streaming budget | 10 MiB budget: 25.5 MiB extra RSS for plain records, 24.4 MiB for continued records | RSS ±10% requirement failed |
| P2 | Boundary benchmark | Median paired declarative/manual ratio 1.013; individual ratios 0.913, 1.058, 1.013 | Median meets 5% target; one run exceeded it |
| P2 | SIMD | Direct scalar/SSE2/AVX2 comparison over aligned and unaligned 64-byte windows; both CPU extensions available | Passed on x86-64; ARM runtime unavailable |

Python/API fixes covered cache sharing and invalidation, source-stream
RecordBatch validation, Arrow dtype handling for generic rows, and Parquet
reader/writer kwarg routing. Parquet row-group regression confirms 10 rows
with `row_group_size=3` produce 4 row groups.

The review produced these structural changes:

- Split `plan.rs` from 1,043 lines into a 407-line execution-plan module and
  640-line predicate module. Existing public import paths remain valid.
- Centralized deferred errors, observer completion, and normalization in
  `TableBuilder::prepare_finish` for both finalization paths.
- Centralized byte/file chunk parsing and batch consumption in the bounded
  executor. This removed duplicated flush/filter/error loops and aligned
  panic handling.
- Removed unreachable pipeline batch fallbacks and the streaming iterator's
  no-op destructor. Materializers retain their existing cache contract.
- Replaced the Parquet keyword allowlist with the installed PyArrow API's
  signatures, keeping adapter options and row-group sizing routed correctly.

Fixed defects:

- Merging an unfinished row could retain its values in place of a later
  committed row. Both builders now normalize before appending.
- Merging a builder dropped its deferred unknown-field error.
- Parallel export bypassed the chunk-completion observer. Completion callback
  panics now return errors through the shared finalization path.
- Bounded file parsing let validation/parser panics escape, unlike byte input.
- Reopening a path after mmap planning could read a replacement file. Planning,
  mapping, decompression detection, and streaming retain the opened file.
- Continuation checks mishandled escaped backslashes and CRLF; blank-line
  scanning could miss a separator when scanning began inside it.
- Adapter splitters could panic on out-of-range offsets. INI parallel chunks
  could lose section state; LDIF splitting missed CRLF record separators.
- Decompression-limit reads could over-read their allowance and reported the
  remaining allowance instead of the configured limit. Tests cover exactly
  1 GiB, one extra byte, empty reads, and a real gzip expansion with a small cap.

Boundary guards also cover regex patterns over 1 MiB, expression nesting at 128,
membership sets over 100,000 elements, invalid dictionary settings, memory
strings, and zero threads. Ordered-stream overflow is tested as an error;
ordered reads do not silently change to unordered output.

Earlier API polishing in this branch is covered by the combined Python suite:
source/pipeline materializers share their cache paths; pipeline execution
respects stage order during fusion; Arrow and row execution preserve cast and
filter behavior; reader options reach adapters; invalid batch sizes and
adapter return types fail explicitly. The adapter template, package versions,
and documentation examples were updated and exercised through a generated
adapter. Documentation was edited with Unslop and the 57-page site built
without issues.

Fuzz runs compile the actual adapter source files with their Python bridges
disabled. This avoids the Windows cargo-fuzz linker flag being applied to
Python shared libraries; parser logic is not copied into the harness.

| Target | Initial campaign | Final campaign |
| --- | ---: | ---: |
| Boundary | 100,000 | 100,000 |
| Splitter | 13,384 | 14,480 |
| Parser to Arrow | 32,332 | 9,626 |
| UTF-8 validation | 100,000 | 100,000 |

Each run stopped at 100,000 executions or about 45 seconds. Logs are in
`target/fuzz-*.log`; the parser corpus persists under `fuzz/corpus/fuzz_parser`.

## Performance and memory

Release measurements used three fresh processes per case after compilation
finished. Each input contains at least 100 MiB, rounded up to a complete record.
Plain records contain 128 payload bytes; continued records carry the same
payload across two physical lines. The consumer verifies every value and the
expected row count, then drops streamed batches. Timings include this check.

| Execution path | Plain MiB/s | Continued MiB/s |
| --- | ---: | ---: |
| Preloaded bytes, serial | 559.2 | 329.9 |
| Preloaded bytes, parallel | 1,207.2 | 1,070.1 |
| File through mmap, serial | 354.1 | 307.9 |
| File streaming, 10 MiB target | 372.3 | 253.2 |

File modes include opening and reading/mapping the file; preloaded-byte modes
exclude input loading. These are different workloads, not interchangeable
measures of parser speed. The parallel argument is a chunk-planning hint, not
a dedicated four-thread pool.

The boundary microbenchmark alternates declarative and manual `memchr` scans
for seven rounds per process. Its median paired ratio was 1.013. Three process
ratios ranged from 0.913 to 1.058; this small sample does not establish a stable
5% ceiling.

Streaming peak RSS was 29.3 MiB for plain records and 28.2 MiB for continued
records, including about 3.8 MiB of process baseline. Largest Arrow batch
allocations were 16.0 and 15.7 MiB respectively. Buffer capacity, chunk input,
and output coexist, so a 10 MiB batch-sizing target cannot be advertised as a
10 MiB process-memory or allocation cap. Mapped serial reads peaked around
206 MiB because they retained the complete output.

Raw medians, per-process runs, row counts, timings, and RSS measurements are
saved in `target/engine-measurements/results.json`. The reproducible harness
is `scripts/benchmark_engine.py` and `crates/rypipe-core/examples/engine_probe.rs`.

Adapter checks use real bundled adapters. Properties Java escapes and
continuations remain explicitly unsupported; LDIF folded, base64, and URL
values remain explicitly unsupported and raise `ParserError`.

Reproduction commands, from the repository root with the development
dependencies installed:

```text
python -m pytest crates/rypipe-python/tests tests/adapters target/rypipe-smoke/tests -q
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo check -p rypipe-core --no-default-features --all-targets
cargo build --release -p rypipe-core --features mmap --example engine_probe
python scripts/benchmark_engine.py
cargo +nightly fuzz run fuzz_boundary -- -max_total_time=45
cargo +nightly fuzz run fuzz_splitter -- -max_total_time=45
cargo +nightly fuzz run fuzz_parser -- -max_total_time=45
cargo +nightly fuzz run fuzz_validate -- -max_total_time=45
```

Set `PROPTEST_CASES=4096` for the expanded property checks. Native Windows
builds require an x64 Visual Studio developer environment. Fuzz builds also
require nightly Rust, cargo-fuzz, and AddressSanitizer. The benchmark RSS
sampler supports Windows and Linux. The template test path requires a
generated adapter; it is not part of the source distribution.

Remaining limits: no ARM runtime evidence and no hard process-memory cap.
Existing large columnar/table-builder modules still warrant separate design
work; this review did not claim a complete rewrite of those subsystems.
