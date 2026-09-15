# Engine testing and code quality review

## September 15 framework completion pass

Upstream `master` was fast-forwarded from `9b4be276` to `c257ea2a`. Local
framework work was restored over the merge, including upstream's adapter moves
to `examples/`. The pre-merge stash remains available as a backup.

This pass concentrates on the framework. The nine example pages explain the
existing examples; it adds no adapter format features. Source and pipeline
materializers retain their shared cache path.

### Changes and fixed defects

- Added soft memory targets and `MemoryBudget::with_strict(true)`. Strict
  executor checks return `Error::Memory`; the shared Python bridge maps that
  error to `MemoryError`. Checks cover tracked input, builders, pending output,
  and Arrow batches. They are not allocator interception or an OS RSS limit;
  some checks happen after allocation. Caller-retained output, Python's baseline,
  arbitrary adapter allocations, and allocator retention are outside a hard cap.
- Fixed an initial-capacity calculation that used byte counts as row counts.
  Input chunks now target one sixty-fourth of the bounded executor budget.
  Output sizing uses observed builder capacity, and large merges flush first.
  Empty destination columns take ownership of source buffers. Dictionary
  lookups no longer allocate a temporary key for already-known values.
- File schema discovery and signature hashing now use bounded reads instead
  of faulting an entire small-file mapping into RSS. File and byte discovery
  share signature ranges and cache keys. Planning drops the mapping before
  streaming; reads retain the original file handle. Default split planning
  runs serially, avoiding an otherwise unnecessary global Rayon pool.
- Ordered parallel workers cannot dispatch arbitrarily far ahead of a slow
  chunk. The dispatch window backpressures workers before the reorder buffer
  grows too far. Worker panics become errors, and cancellation releases workers
  waiting on the dispatch condition variable. Soft mode permits oversized
  pending batches; strict mode reports memory overflow. Ordering stays enabled.
- Collected bounded batches now align columns that first appear later, filling
  earlier rows with nulls. Mixed automatic dictionary/string encodings are
  reconciled to strings; uniform dictionary output remains encoded. The
  regression verifies both original encodings exist, then checks all 12,000
  collected values, common schemas, and 6,000 missing-field nulls.
- Split predicate-row handling, memory accounting, scalar conversion, and
  parallel schema discovery into focused modules. Shared batch finalization
  applies filtering and budget checks at every bounded flush.
- Made all five Rust doctests compile: three execute and two use `no_run`.
  No doctest is ignored. Expanded decoder coverage and specialized common
  boundary paths without changing stateful splitter semantics.
- CI now builds and tests native adapters and a fresh generated template,
  tests installed Python wheels in isolated mode, and defines a native Linux
  ARM64 core job. Corrected action pins that pointed to v1 releases despite
  newer version labels. Added four fuzz smoke targets and weekly/manual longer
  campaigns with persistent corpora and separate failure artifacts.
- Added nine dedicated example pages and navigation. The 66-page site passes
  a clean strict build after Unslop; `llms-full.txt` includes the pages.

### Local verification

All local runtime results below are Windows x86-64. The Python harness runs
outside the measured Rust child process. Platform-matrix and ARM execution
still require a remote CI run for these local changes.

| Check | Result |
| --- | --- |
| Workspace Rust, all features | 294 passed, including five doctests; none ignored |
| Memory and collection regressions | Nine passed with all features and without default features |
| Property-test cases | `PROPTEST_CASES=4096` for the final workspace run |
| Clippy and formatting | All targets/features, warnings denied; formatting checked |
| Isolated root wheel tests | 138 passed, 15 warnings |
| Isolated adapter wheel tests | 76 passed |
| Adapter Rust tests | 22 passed |
| Fresh generated-template wheel | One Python smoke test passed |
| Documentation | 66 pages; strict clean build passed |

The final root wheel includes the bounded-memory and collection fixes. Adapter
and template wheels were rebuilt and tested earlier in this pass; their tested
entry points do not call the subsequently changed bounded collector or ordered
streaming coordinator. They are not claimed as rebuilds after every core edit.

Four AddressSanitizer campaigns ran for 181 seconds each. Their scope is
boundary selection, splitter behavior, parser-to-Arrow conversion, and UTF-8
validation; they do not fuzz the ordered streaming coordinator.

| Fuzz target | Executions | Result |
| --- | ---: | --- |
| Boundary | 2,174,683 | Passed |
| Splitter | 22,014 | Passed |
| Parser to Arrow | 106,807 | Passed |
| UTF-8 validation | 14,158,043 | Passed |
| Total | 16,461,547 | No failures or crash artifacts |

An earlier four-target campaign added 32,399,043 executions against the earlier
revision. These bounded campaigns are not exhaustive. ASan process RSS includes
instrumentation overhead and is not used to assess the engine memory target.
Logs are `target/framework-fuzz-final-{boundary,splitter,parser,validate}.log`.

### Final performance and memory measurements

The final streaming matrix used 232 fresh Rust child processes after native
builds and fuzzing finished. Each consumer checks every emitted value and the
expected row count, then drops output. Timings include those checks. The budget
was 10 MiB. RSS figures below are median peak increases above each child's
pre-execution baseline, about 5.1 MiB for file modes. Byte modes preload input
before that baseline, so their delta excludes the caller-owned input.

For 100 MiB inputs, 128-byte payloads, one output column, and ten processes per
case, extra RSS in MiB was:

| Record style | File stream | Byte stream | Iterator | Ordered parallel |
| --- | ---: | ---: | ---: | ---: |
| Plain | 2.68 | 1.61 | 4.34 | 6.08 |
| Continued | 2.68 | 1.59 | 4.43 | 6.19 |
| Comments | 2.84 | 1.61 | 8.34 | 10.07 |
| Blank separators | 2.68 | 1.61 | 4.34 | 6.08 |

Throughput for those cases, in input MiB/s:

| Record style | File stream | Byte stream | Iterator | Ordered parallel |
| --- | ---: | ---: | ---: | ---: |
| Plain | 833.4 | 914.8 | 906.6 | 716.4 |
| Continued | 559.2 | 594.2 | 577.9 | 547.2 |
| Comments | 765.0 | 906.7 | 785.0 | 736.6 |
| Blank separators | 727.0 | 833.2 | 773.0 | 715.9 |

Three processes per case covered larger input, longer rows, and output
expansion. The probe repeats each input payload into every requested column,
so the eight-column case expands payload data eightfold. Extra RSS in MiB:

| Input / payload / columns | Style | File stream | Byte stream | Iterator | Ordered parallel |
| --- | --- | ---: | ---: | ---: | ---: |
| 10 MiB / 32 bytes / 8 | Plain | 5.28 | 5.02 | 5.27 | 14.55 |
| 10 MiB / 32 bytes / 8 | Continued | 5.25 | 5.40 | 4.67 | 12.75 |
| 100 MiB / 4,096 bytes / 4 | Plain | 5.84 | 5.56 | 5.07 | 14.95 |
| 100 MiB / 4,096 bytes / 4 | Continued | 5.84 | 5.57 | 5.34 | 14.13 |
| 256 MiB / 128 bytes / 1 | Plain | 4.19 | 3.36 | 4.34 | 12.04 |
| 256 MiB / 128 bytes / 1 | Continued | 4.28 | 3.82 | 4.44 | 13.29 |

The largest individual extra-RSS sample was 19.88 MiB in the continued,
eight-column ordered-parallel case. The soft target therefore is not a uniform
RSS ceiling. Concurrent buffers, output expansion, and allocator retention
still matter. Largest emitted Arrow batches were about 2.0 MiB in the long-row
case. Strict-mode regressions verify tracked-overflow errors and recovery;
these soft-mode RSS measurements do not prove a strict process-memory cap.

Default splitter planning caps also remain relevant. A splitter may return
larger chunks than requested when its record boundaries or chunk cap require
it. Strict execution can reject those chunks. Retaining consumer output and
up-front decompression can also increase memory independently of streaming
batch sizing.

The boundary comparison checked identical offsets before timing seven
alternating declarative/manual rounds in each of ten fresh processes per
style. The decoder was unchanged by subsequent allocation/collection fixes.

| Style | Median ratio | Minimum | p90 | Maximum | Runs above 1.05 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Plain | 0.8161 | 0.7263 | 0.9039 | 0.9655 | 0/10 |
| Continued | 0.8917 | 0.8066 | 0.9568 | 0.9613 | 0/10 |
| Comments | 1.0072 | 0.9483 | 1.0499 | 1.0685 | 1/10 |
| Blank separators | 0.9594 | 0.8299 | 1.0083 | 1.0108 | 0/10 |

All medians meet the 5% overhead target. The comment-mode outlier prevents a
claim that every run stays within 5%. Ratios and throughput describe these
fixtures on this host, not a guarantee for other adapters or machines.

Raw runs are in `target/engine-measurements/framework-final-v6.json`,
`framework-wide-schema-v6.json`, `framework-long-rows-v6.json`, and
`framework-large-input-v6.json`. Boundary distributions are in
`framework-final-v4.json`. These generated files are ignored by Git;
the tables above preserve their measured results.

Reproduce from the repository root, after builds finish:

```text
cargo build --release -p rypipe-core --all-features --example engine_probe
python scripts/benchmark_engine.py --reps 10 --modes boundary stream stream-bytes iterator stream-parallel
python scripts/benchmark_engine.py --reps 3 --mib 10 --width 32 --columns 8 --kinds plain continued --modes stream stream-bytes iterator stream-parallel
python scripts/benchmark_engine.py --reps 3 --mib 100 --width 4096 --columns 4 --kinds plain continued --modes stream stream-bytes iterator stream-parallel
python scripts/benchmark_engine.py --reps 3 --mib 256 --width 128 --columns 1 --kinds plain continued --modes stream stream-bytes iterator stream-parallel
```

Pass `--output <path>` to retain each report separately. The Python RSS sampler
supports Windows and Linux. See the CI workflow for clean native packaging and
platform jobs. Publishing and running that matrix remains the only unchecked
acceptance item in `REMAINING_WORK.md`.

## September 14 baseline

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
