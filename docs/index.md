# rypipe documentation

`rypipe` is a format- and source-agnostic ingestion framework that provides a common
execution runtime for turning arbitrary record-oriented data sources into typed
columnar data. It separates **format-specific** parsing from **format-agnostic**
execution, so the same engine can parse XML, JSON, CSV, HTML, or any other
row-oriented format once you provide a small adapter.

`rypipe` itself does **not** ship parsers for any format. Adapters live in
separate packages. Install the engine plus the adapters you need.

## What rypipe is

- A Rust workspace with two crates:
  - [`rypipe-core`](./architecture/): the generic engine (see [Architecture overview](./architecture/) for crate map).
  - [`rypipe-python`](./architecture/#rypipe-python): PyO3 bindings and helper
    functions for adapter packages.
- Zero-copy friendly: decoders emit borrowed strings; the engine copies only
  when necessary.
- GIL-free parsing: all heavy work runs outside Python's GIL.
- Memory-bounded and parallel by design.

## Why rypipe

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

## What rypipe is not

- **Not a query engine.** It handles projection, renaming, dropping, casting,
  filtering, and dictionary encoding. It does not do joins, aggregations, window
  functions, or SQL.
- **Not a one-size-fits-all parser.** Each format needs a `RecordParser` +
  `Splitter` adapter from a separate package.
- **Not a data warehouse.** It ingests data into Arrow; it does not store it,
  index it, or serve queries over it.
- **Not pure Python.** Adapters are written in Rust for performance. Python
  users consume data through `rypipe.read()` and the pipeline API; they do not
  need to write Rust unless creating a new adapter.

## Quick start

### From Python

```bash
pip install rypipe my-adapter
```

```python
import rypipe
import my_adapter

# Format is inferred from the extension; mode defaults to parallel.
table = rypipe.read(
    "data.myfmt",
    field_types={"amount": "float64"},
    filter={"field": "status", "op": "==", "value": "active"},
)
print(table.num_rows, table.num_columns)
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

### From Rust

```rust
use rypipe_core::{ExecutionPlan, FieldType, Pipeline};
use my_adapter::{MySplitter, MyDecoder}; // separate adapter crate

let batch = Pipeline::new(MySplitter::new(), MyDecoder::new())
    .with_plan(
        ExecutionPlan::new()
            .type_as("amount", FieldType::Float64)
            .filter_eq("status", "active"),
    )
    .read_path("data.myfmt", false, false)?;
```

## Guides

- [Architecture](./architecture/): how the pieces fit together (start with [Overview](./architecture/index.md), then [Engine](./architecture/engine.md), [Columnar](./architecture/columnar.md), [Plan](./architecture/plan.md), [Execution](./architecture/execution.md), [Data flow](./architecture/data-flow.md), [Storage](./architecture/storage.md), [Optimizations](./architecture/optimizations.md)).
- [Why Python?](./why-python.md): why rypipe is Rust core plus Python surface, not pure Rust, the data driven case for the hybrid.
- [Python API](./python-api.md): the `rypipe` package and `_rypipe` helpers.
- [Rust API](./rust-api.md): using `rypipe-core` and writing custom adapters.
- [Writing a format adapter](./writing-adapters/): adding CSV, JSON, etc.
- [Performance](./performance.md): benchmarks and tuning knobs.

## Repository layout

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
