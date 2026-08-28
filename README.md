<p align="center">
  <h1 align="center">rypipe</h1>
</p>

<p align="center">
  <strong>Format-agnostic columnar ingestion engine with Rust core and Python bindings.</strong>
</p>

<p align="center">
  Parse row-oriented byte streams into Apache Arrow record batches with parallel
  scheduling, memory-bounded execution, query pushdown, and a chainable
  pipeline API. Format adapters live in separate packages.
</p>

<p align="center">
  <a href="https://www.python.org/downloads/">
    <img src="https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white&style=for-the-badge" alt="Python">
  </a>
  <a href="https://www.rust-lang.org/">
    <img src="https://img.shields.io/badge/Rust-1.78%2B-000000?logo=rust&logoColor=white&style=for-the-badge" alt="Rust">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/License-MIT-10AC84?style=for-the-badge" alt="License">
  </a>
  <a href="https://github.com/emiliano-go/rypipe/actions/workflows/test.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/emiliano-go/rypipe/test.yml?branch=master&style=for-the-badge&logo=github&label=Tests" alt="Tests">
  </a>
  <a href="https://rypipe.emiliano-go.com/">
    <img src="https://img.shields.io/badge/Docs-rypipe.emiliano--go.com-8A2BE2?style=for-the-badge&logo=readthedocs" alt="Docs">
  </a>
  <a href="https://pypi.org/project/rypipe/">
    <img src="https://img.shields.io/badge/PyPI-rypipe-006DAD?style=for-the-badge&logo=pypi&logoColor=white" alt="PyPI">
  </a>
</p>

---

## What is rypipe

`rypipe` is a pure ingestion-to-Arrow engine. It separates format-specific
parsing (splitting, row extraction) from format-agnostic execution (typed
column builders, filtering, projection, dictionary encoding, parallel
scheduling, memory-bounded execution, and Arrow export). Add a new format by
implementing two small traits: `Splitter` and `RecordParser`.

`rypipe` itself does **not** ship parsers for XML, JSON, CSV, HTML, or any
other format. Those live in separate adapter packages. Install the engine plus
the adapters you need.

> **Note:** `crxml` was the original idea: a fast Crystal Reports XML parser that needed parallel, bounded-memory, and Arrow-native execution. The engine that made `crxml` fast (`Splitter` + `RecordParser` + `TableBuilder` + `ExecutionPlan`) was then separated and abstracted into `rypipe` so any format could reuse it. `crxml` now lives as a thin adapter (`crxml-core` + `CrystalXMLSource`) on top of `rypipe-core`. See `rypipe` `docs/crxml-adapter.md` for the `3 GB/s` evolution.

## Features

- **Zero-copy friendly**: decoders emit borrowed strings; the engine copies only
  when necessary.
- **GIL-free parsing**: heavy work runs outside Python's GIL.
- **Parallel by default**: chunked parsing with `rayon` scales to many cores.
- **Memory bounded**: stream files larger than RAM with a configurable budget.
- **Typed columns**: cast strings to `int64`, `float64`, or `bool` during parse.
- **Pushdown filters**: rename, drop, type, and filter rows while parsing.
- **Pipeline API**: chainable rename/drop/cast/filter stages with automatic fusion.
- **Dictionary encoding**: explicit or automatic low-cardinality encoding.
- **Arrow native**: produces `RecordBatch` and exports via the C Data Interface.

## Crates

| Crate | Purpose |
|-------|---------|
| `rypipe-core` | Pure Rust engine: `Value`, `ExecutionPlan`, `TableBuilder`, `ColumnarSink`, `RecordParser`, `Splitter`, `Pipeline`, parallel/bounded drivers, Arrow export |
| `rypipe-python` | PyO3 bindings and helper functions for adapter packages; exposes the `rypipe` package |

## Python quick start

```bash
pip install rypipe my-adapter
```

Wheels are built against CPython's stable ABI (`abi3`, 3.10+), so one wheel per
platform covers every supported interpreter, including versions released after
a given rypipe release. Prebuilt wheels ship for manylinux (glibc 2.17+),
musllinux, macOS (x86_64 and arm64), and Windows x64; anything else builds from
the sdist and needs a Rust toolchain.

Optional DataFrame sinks pull their own dependencies:

```bash
pip install "rypipe[pandas]"   # to_pandas / to_dataframe
pip install "rypipe[polars]"   # to_polars
pip install "rypipe[all]"      # both
```

```python
import rypipe
import my_adapter

table = rypipe.read(
    "data.myfmt",
    fields={"amount": "float64", "qty": "int64"},
    filter={"field": "status", "op": "==", "value": "active"},
)
print(table.num_rows, table.num_columns)
```

```python
# Bounded-memory streaming.
table = rypipe.read_stream("huge.myfmt", memory="256MiB")
```

### Pipeline API

Adapters that expose a `rypipe.Adapter` subclass give you a chainable pipeline
with automatic fusion of rename, drop, cast, and filter stages into the Rust
parse loop. Subclasses only implement ``read(path, **kwargs)``::

```python
from rypipe import RenameFields, DropFields, CastTypes, FilterRows
import my_adapter

source = my_adapter.MySource("data.myfmt")

df = (
    source
    | RenameFields({"old_name": "new_name"})
    | DropFields(["internal_id"])
    | CastTypes({"amount": float, "qty": int})
    | FilterRows(field="status", op="==", value="active")
).to_dataframe()
```

The same operations work as kwargs on `rypipe.read` when you only need a table.

## Rust quick start

```rust
use rypipe_core::{ExecutionPlan, FieldType, Pipeline};
use my_adapter::{MySplitter, MyDecoder}; // separate adapter crate

let batch = Pipeline::new(MySplitter::new(), MyDecoder::new())
    .with_plan(
        ExecutionPlan::new()
            .type_as("amount", FieldType::Float64)
            .type_as("qty", FieldType::Int64)
            .filter_eq("status", "active"),
    )
    .read_path("data.myfmt", false, false)?;
```

## Building

```bash
# Rust only
cargo build --workspace --release

# Python extension
maturin develop --release
```

## Testing

```bash
# Rust
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features : -D warnings

# Python (against an installed build)
pip install -e ".[dev]"
pytest crates/rypipe-python/tests/
```

Tests covering optional dependencies (`pandas`, `polars`) skip when those
packages are absent. Set `RYPIPE_REQUIRE_OPTIONAL_DEPS=1` to turn a missing
optional dependency into a hard failure instead. CI sets it so that optional
coverage cannot silently disappear from a green run.

## Benchmark — parallel streaming with frozen schema (parallel Discovery)

`crxml` (reference adapter) on Ryzen 5800X, 533 MB real / 1 GB synthetic, warm, median-of-7, frozen schema (`FrozenSchema` `crates/rypipe-core/src/schema.rs:14`, `discovery_ns` in `get_par_profile()`):

| Mode | 533 MB Table | 1 GB Table | RssAnon 533 MB | Schema |
|---|---|---|---|---|
| `Pipeline::read_path_par` `par128` (4 MB) | **4470 MB/s** | 4278 MB/s | 137 MB | — |
| `ParallelStreamingExecutor` `64MB/16t` auto (2 MB) | **4497 MB/s** | 3782* MB/s | **88 MB** | parallel 16×2 MiB sampled Discovery (5.3 ms, was 19 ms serial) |
| `ParallelStreamingExecutor` `64MB/16t` explicit `schema=[10 cols]` | **4980 MB/s** | ~4900 MB/s | **88 MB** | `from_plan` exact, no Discovery |
| `ParallelStreamingExecutor` Vec\<Batch\> auto | 4485 MB/s | 3863* MB/s | 88 MB | same |

\* 1 GB auto still 3782/3863 from before parallel Discovery was re-measured on 533 MB only (parallel 19→5.3 ms). Before frozen schema, auto was 4770/4551 (unstable: batch 2 order `FieldG` vs `Text20` last, `pq.ParquetWriter` raised). Frozen schema fixes order (every batch same, sparse `FieldG` 30%/`Text21` 1% as all-null) via `ensure_schema` `crates/rypipe-core/src/engine.rs:79` but adds 5.3 ms (was 15% at 19 ms, now 4% at 5.3 ms), making auto **+0.6% vs `par128`** (4497 vs 4470, within CoV) — unblocks `auto` default. Explicit still +11% and defines the ceiling. Sweep: `par` peaks at 4 MB, streaming at 2 MB — one divisor cannot serve both, kept split (`par` `4 MB`, streaming `2 MB` via `budget/(threads×2)`). Cap raised `8×threads`→`16×threads` (256) so 533 MB now hits ideal 133 for 4 MB (was capped 128). See `crxml` `docs/performance.md` for like-for-like, chunk-per-cell, fixed-chunk isolation (par 1 MB collapses 3553 vs streaming 3812, +7% — `chunk_buf` reuse).

Run the engine throughput benchmark:

```bash
cargo run --release -p rypipe-core --example bench_throughput
```

## Documentation

Full docs and integration guides are in the `docs/` directory:

- [Overview](docs/index.md)
- [Architecture](docs/architecture.md)
- [Python API](docs/python-api.md)
- [Rust API](docs/rust-api.md)
- [Writing a format adapter](docs/writing-adapters.md)
- [Performance](docs/performance.md)

## Why Python, not pure Rust

Data work lives in Python : `pip`, notebooks, and the PyArrow/pandas/Polars
ecosystem. ETL is glue: `S3 → parse → rename/drop/cast/filter → validate →
write Parquet`. That glue is Python; the hot loop that touches every byte at
700 MB/s (2.5 GB/s parallel) must be Rust.

`rypipe` is **Rust where it counts, Python where it ships**:

* **Zero-copy, GIL-free Rust core** parses outside the GIL and hands Arrow
  `RecordBatch`es to `pyarrow` via the C Data Interface : no copy, no
  serialization.
* **Python composition** : `source | RenameFields | FilterRows | CastTypes |
  .to_dataframe()` : lets the same pipeline be explored in a notebook and
  scaled unchanged in Airflow/Dagster on 100 GB of files.
* **Pure-Rust still works** when you need it: `rypipe-core` has no Python
  dependency and is usable as a crate (`Pipeline::new(Splitter, Parser)` →
  `read_path_par`).

A pure-Rust library would save ~0.1 ms of orchestration and cost the 90% of
users who have never run `cargo build` their workflow. For data teams, a
`pip install` beats a toolchain install every time.

> `polars` didn’t win by being pure Rust : it won with a Rust engine behind
> `pl.DataFrame`. `rypipe` does the same for ingestion. See the full
> data-driven justification in [Why Python?](docs/why-python.md).

## License

MIT
