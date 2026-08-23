# rypipe documentation

`rypipe` is a format-agnostic columnar engine that turns byte streams into
Apache Arrow record batches. It separates **format-specific** parsing from
**format-agnostic** execution, so the same engine can parse XML, JSON, CSV,
HTML, or any other row-oriented format once you provide a small adapter.

`rypipe` itself does **not** ship parsers for any format. Adapters live in
separate packages. Install the engine plus the adapters you need.

## What rypipe is

- A Rust workspace with two crates:
  - [`rypipe-core`](./architecture.md#rypipe-core): the generic engine.
  - [`rypipe-python`](./architecture.md#rypipe-python): PyO3 bindings and helper
    functions for adapter packages.
- Zero-copy friendly: decoders emit borrowed strings; the engine copies only
  when necessary.
- GIL-free parsing: all heavy work runs outside Python's GIL.
- Memory-bounded and parallel by design.

## What rypipe is not

- Not a full query engine. It handles projection, renaming, dropping, casting,
  filtering, and dictionary encoding, not joins, aggregations, or SQL.
- Not a one-size-fits-all parser. Each format needs a `RecordParser` + `Splitter`
  adapter from a separate package.

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
    fields={"amount": "float64"},
    filter={"field": "status", "op": "==", "value": "active"},
)
print(table.num_rows, table.num_columns)
```

### Pipeline API

Adapters that expose a `rypipe.Source` subclass give you a chainable pipeline
with automatic fusion of rename, drop, cast, and filter stages into the Rust
parse loop::

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

- [Architecture](./architecture.md): how the pieces fit together.
- [Python API](./python-api.md): the `rypipe` package and `_rypipe` helpers.
- [Rust API](./rust-api.md): using `rypipe-core` and writing custom adapters.
- [Writing a format adapter](./writing-adapters.md): adding CSV, JSON, etc.
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
