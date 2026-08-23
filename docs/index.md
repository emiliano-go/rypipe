# rypipe documentation

`rypipe` is a format-agnostic columnar engine that turns byte streams into
Apache Arrow record batches. It separates **format-specific** parsing from
**format-agnostic** execution, so the same engine can parse XML, JSON, CSV,
HTML, or any other row-oriented format once you provide a small adapter.

## What rypipe is

- A Rust workspace with three crates:
  - [`rypipe-core`](./architecture.md#rypipe-core): the generic engine.
  - [`rypipe-xml`](./architecture.md#rypipe-xml): Crystal Reports XML adapter.
  - [`rypipe-python`](./architecture.md#rypipe-python): PyO3 bindings.
- Zero-copy friendly: decoders emit borrowed strings; the engine copies only
  when necessary.
- GIL-free parsing: all heavy work runs outside Python's GIL.
- Memory-bounded and parallel by design.

## What rypipe is not

- Not a full query engine. It handles projection, renaming, dropping, casting,
  filtering, and dictionary encoding, not joins, aggregations, or SQL.
- Not a one-size-fits-all parser. Each format needs a `RecordParser` + `Splitter`
  adapter.

## Quick start

### From Python

```bash
export PYO3_PYTHON=/path/to/python3.12
maturin develop --release
```

```python
import rypipe

table = rypipe.read(
    "data.xml",
    row_tag="Row",
    fields={"amount": "float64"},
    filter={"field": "status", "op": "==", "value": "active"},
)
print(table.num_rows, table.num_columns)
```

### From Rust

```rust
use rypipe_core::{ExecutionPlan, FieldType, Pipeline};
use rypipe_xml::xml_pipeline;

let batch = xml_pipeline("Row")
    .with_plan(
        ExecutionPlan::new()
            .type_as("amount", FieldType::Float64)
            .filter_eq("status", "active"),
    )
    .read_path("data.xml", false, false)?;
```

## Guides

- [Architecture](./architecture.md): how the pieces fit together.
- [Python API](./python-api.md): `_rypipe` functions and options.
- [Rust API](./rust-api.md): using `rypipe-core` and writing custom adapters.
- [Writing a format adapter](./writing-adapters.md): adding CSV, JSON, etc.
- [Integrating with crxml](./integrating-crxml.md): how crxml consumes rypipe.
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
│   ├── rypipe-xml/            # Crystal XML adapter
│   └── rypipe-python/         # PyO3 bindings
└── docs/
    └── (this directory)
```
