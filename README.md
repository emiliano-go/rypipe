<p align="center">
  <h1 align="center">rypipe</h1>
</p>

<p align="center">
  <strong>Format-agnostic columnar engine for XML, JSON, CSV, HTML, and more.</strong>
</p>

<p align="center">
  Parse row-oriented byte streams into Apache Arrow record batches with a Rust
  core, Python bindings, parallel scheduling, memory-bounded execution, and
  query pushdown.
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

`rypipe` separates format-specific parsing (splitting, row extraction) from
format-agnostic execution (typed column builders, filtering, projection,
dictionary encoding, parallel scheduling, memory-bounded execution, and Arrow
export). Add a new format by implementing two small traits: `Splitter` and
`RecordParser`.

The first adapter, `rypipe-xml`, implements the Crystal Reports XML grammar
extracted from [crxml](https://github.com/emiliano-go/crxml).

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
| `rypipe-xml` | Crystal Reports XML adapter: `CrystalXmlDecoder`, `CrystalXmlSplitter`, `xml_pipeline` |
| `rypipe-python` | PyO3 bindings; exposes the `rypipe` package and the reusable `_rypipe` extension |

## Python quick start

```bash
export PYO3_PYTHON=/path/to/python3.12
maturin develop --release
```

```python
import rypipe

# Format is inferred from the extension; mode defaults to parallel.
table = rypipe.read(
    "report.xml",
    row_tag="Row",
    fields={"amount": "float64", "qty": "int64"},
    filter={"field": "status", "op": "==", "value": "active"},
)
print(table.num_rows, table.num_columns)

# Bounded-memory streaming.
table = rypipe.read_stream("huge.xml", memory="256MiB", row_tag="Row")
```

## Rust quick start

```rust
use rypipe_core::{ExecutionPlan, FieldType, Pipeline};
use rypipe_xml::xml_pipeline;

let batch = xml_pipeline("Row")
    .with_plan(
        ExecutionPlan::new()
            .type_as("amount", FieldType::Float64)
            .type_as("qty", FieldType::Int64)
            .filter_eq("status", "active"),
    )
    .read_path("report.xml", false, false)?;
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

## Documentation

Full docs and integration guides are in the `docs/` directory:

- [Overview](docs/index.md)
- [Architecture](docs/architecture.md)
- [Python API](docs/python-api.md)
- [Rust API](docs/rust-api.md)
- [Writing a format adapter](docs/writing-adapters.md)
- [Integrating with crxml](docs/integrating-crxml.md)
- [Performance](docs/performance.md)

## Why rypipe

`rypipe` was extracted from [crxml](https://github.com/emiliano-go/crxml), a
high-performance Crystal Reports XML parser. The goal was to keep crxml's
speed while making the same engine available for JSON, CSV, HTML, and other
formats through a small adapter interface.

## License

MIT
