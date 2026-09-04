# rypipe documentation { #rypipe-documentation }

`rypipe` is a format- and source-agnostic ingestion framework that provides a common
execution runtime for turning arbitrary record-oriented data sources into typed
columnar data. It separates **format-specific** parsing from **format-agnostic**
execution, so the same engine can parse XML, JSON, CSV, HTML, or any other
row-oriented format once you provide a small adapter.

`rypipe` itself does **not** ship parsers for any format. Adapters live in
separate packages. Install the engine plus the adapters you need.

## What rypipe is { #what-rypipe-is }

- A Rust workspace with two crates:
  - [`rypipe-core`](./architecture/): the generic engine (see [Architecture overview](./architecture/) for crate map).
  - [`rypipe-python`](./architecture/#rypipe-python): PyO3 bindings and helper
    functions for adapter packages.
- Zero-copy friendly: decoders emit borrowed strings; the engine copies only
  when necessary.
- GIL-free parsing: all heavy work runs outside Python's GIL.
- Memory-bounded and parallel by design.

## Why rypipe { #why-rypipe }

- **One runtime, many formats.** XML, JSON, CSV, HTML, TSV, and any future
  format share the same parallel scheduler, memory-bounded executor, Arrow
  export, and pushdown infrastructure. An adapter is two small traits, not a
  full engine.
- **Performance without compromise.** Single-thread ~950 MB/s, parallel ~4.2 GB/s
  (par128), streaming with explicit schema ~5 GB/s bounded. Zero-copy Arrow
  export. Predicate first evaluation. Layout prediction via memcmp.
- **Correctness by construction.** Differential testing, fuzz targets, property
  tests, and a tier-ladder profiler that decomposes every nanosecond of the hot
  path.

## What rypipe is not { #what-rypipe-is-not }

- **Not a query engine.** It handles projection, renaming, dropping, casting,
  filtering, and dictionary encoding. It does not do joins, aggregations, window
  functions, or SQL.
- **Not a one-size-fits-all parser.** Each format needs a `RecordParser` +
  `Splitter` adapter from a separate package.
- **Not a data warehouse.** It ingests data into Arrow; it does not store it,
  index it, or serve queries over it.
- **Not pure Python.** Adapters are written in Rust for performance. Python
  users consume data through adapter APIs; they do not need to write Rust
  unless creating a new adapter.

## Quick start { #quick-start }

### From Python { #from-python }

```bash
pip install crxml
```

Download a sample [report.xml](examples/report.xml) or create your own:

```python
from crxml import CrystalXMLSource

source = CrystalXMLSource("report.xml", row_tag="Details")
table = source.to_arrow()

print(table.num_rows, table.num_columns)
# 9 8
```

### Pipeline API { #pipeline-api }

```python
from crxml import CrystalXMLSource, CastTypes, FilterRows

src = CrystalXMLSource("report.xml", row_tag="Row")

df = (
    src
    | CastTypes({"Amount": float})
    | FilterRows(field="Status", op="==", value="active")
).to_dataframe()

print(df)
#     Name  Amount  Status
# 0  Alice   150.0  active
# 1  Carol   200.0  active
# 2   Dave    50.0  active
```

### From Rust { #from-rust }

```rust
use rypipe_core::{ExecutionPlan, FieldType, Pipeline};
use crxml_core::{CrystalXmlSplitter, CrystalXmlParser}; // adapter crate

let batch = Pipeline::new(CrystalXmlSplitter, CrystalXmlParser)
    .with_plan(
        ExecutionPlan::new()
            .type_as("Amount", FieldType::Float64)
            .filter_eq("Status", "active"),
    )
    .read_path("report.xml", false, false)?;
```

## Guides { #guides }

- [Tutorial](./tutorial/): install, first read, pipeline, stages, sinks, streaming.
- [Writing Adapters](./writing-adapters/): add a new format (Splitter, RecordParser, Source, registration).
- [Architecture](./architecture/): how the engine works internally.
- [Advanced](./advanced/): fusion, execution modes, memory, parallelism, profiling.
- [Reference](./reference/): Python API and Rust API.

## Repository layout { #repository-layout }

```
rypipe/
├── Cargo.toml                 # workspace
├── pyproject.toml             # maturin / Python package
├── README.md
├── LICENSE
├── crates/
│   ├── rypipe-core/           # generic engine
│   └── rypipe-python/         # PyO3 bindings and helper functions
└── docs/
    └── (this directory)
```
