<p align="center">
  <h1 align="center">rypipe</h1>
</p>

<p align="center">
  <strong>Format-agnostic columnar ingestion engine with Rust core and Python bindings.</strong>
</p>

<p align="center">
  Parse row-oriented byte streams into Apache Arrow record batches with parallel
  scheduling, memory-bounded execution, query pushdown, and a crxml-style
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

## Features

- **Zero-copy friendly**: decoders emit borrowed strings; the engine copies only
  when necessary.
- **GIL-free parsing**: heavy work runs outside Python's GIL.
- **Parallel by default**: chunked parsing with `rayon` scales to many cores.
- **Memory bounded**: stream files larger than RAM with a configurable budget.
- **Typed columns**: cast strings to `int64`, `float64`, or `bool` during parse.
- **Pushdown filters**: rename, drop, type, and filter rows while parsing.
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

### Pipeline API (crxml-style)

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
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## Benchmark

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

## Why rypipe

`rypipe` was extracted from a high-performance XML parser. The goal was to keep
that parser's speed while making the same engine available for JSON, CSV, HTML,
and other formats through a small adapter interface. Format-specific code now
lives in separate packages; the rypipe repository contains only the engine and
its Python bindings.

## License

MIT
