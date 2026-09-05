<p align="center">
  <h1 align="center">rypipe</h1>
</p>

<p align="center">
  <strong>Format-agnostic columnar ingestion engine. Rust core, Python bindings.</strong>
</p>

<p align="center">
  Parse row-oriented byte streams into Apache Arrow tables with parallel
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

`rypipe` is a format-agnostic columnar ingestion engine. It separates
format-specific parsing from format-agnostic execution. Add a new format by
implementing two small traits: `Splitter` and `RecordParser`.

`rypipe` itself does **not** ship parsers. Those live in separate adapter
packages. Install the engine plus the adapters you need.

## Quick start

```bash
pip install crxml
```

```python
from crxml import CrystalXMLSource, CastTypes, FilterRows

source = CrystalXMLSource("report.xml", row_tag="Details")

# Simple read
table = source.to_arrow()

# Pipeline with stages
result = (
    source
    | CastTypes({"Amount": float})
    | FilterRows(field="Status", op="==", value="Active")
).to_arrow()

# Convert to DataFrame
df = source.to_pandas()
```

## Why rypipe

- **One runtime, many formats.** XML, JSON, CSV, and any future format share
  the same parallel scheduler, memory-bounded executor, and pushdown
  infrastructure. An adapter is two small traits, not a full engine.

- **Performance without compromise.** Parallel ~4.5 GB/s, single-thread ~1 GB/s.
  Zero-copy Arrow export. Predicate-first evaluation. Layout prediction via memcmp.

- **Correctness by construction.** Differential testing, fuzz targets, property
  tests, and a tier-ladder profiler.

- **Python-native ergonomics.** Chainable pipeline API with automatic fusion of
  rename/drop/cast/filter into the Rust parse loop. Streaming with bounded
  memory. Schema discovery. DataFrame and Parquet sinks.

## What rypipe is not

- **Not a query engine.** No joins, aggregations, window functions, or SQL.
- **Not a parser.** Each format needs an adapter package.
- **Not a data warehouse.** It ingests into Arrow; it does not store or serve.

## Features

- **Zero-copy friendly**: decoders emit borrowed strings; the engine copies only
  when necessary.
- **GIL-free parsing**: heavy work runs outside Python's GIL.
- **Parallel by default**: chunked parsing with `rayon` scales to many cores.
- **Memory bounded**: stream files larger than RAM with a configurable budget.
- **Pushdown filters**: rename, drop, type, and filter rows while parsing.
- **Pipeline API**: chainable rename/drop/cast/filter stages with automatic fusion.
- **Arrow native**: produces `RecordBatch` and exports via the C Data Interface.

## Crates

| Crate | Purpose |
|-------|---------|
| `rypipe-core` | Pure Rust engine: `Value`, `ExecutionPlan`, `TableBuilder`, `Pipeline`, parallel/bounded drivers, Arrow export |
| `rypipe-python` | PyO3 bindings for adapter packages; exposes the `rypipe` package |

## Documentation

- [Tutorial](docs/tutorial/index.md): install, first read, pipeline, stages, sinks, streaming
- [Writing Adapters](docs/writing-adapters/index.md): add a new format
- [Architecture](docs/architecture/index.md): how the engine works internally
- [Advanced](docs/advanced/index.md): fusion, execution modes, memory, parallelism
- [Python API](docs/reference/python-api.md): full API reference
- [Rust API](docs/reference/rust-api.md): full API reference

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

# Python
pip install -e ".[dev]"
pytest crates/rypipe-python/tests/
```

## License

MIT
