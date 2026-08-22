# rypipe

[![CI](https://github.com/emiliano-go/rypipe/actions/workflows/test.yml/badge.svg)](https://github.com/emiliano-go/rypipe/actions/workflows/test.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A format-agnostic columnar engine that turns XML, JSON, CSV, HTML, and other
row-oriented byte streams into Apache Arrow record batches. The engine is
written in Rust and exposes both a Rust API and Python bindings via PyO3.

`rypipe` separates format-specific parsing (splitting, row extraction) from
format-agnostic execution (typed column builders, projection, filtering,
dictionary encoding, parallel scheduling, memory-bounded execution, and Arrow
export). Add a new format by implementing two small traits: `Splitter` and
`RecordParser`.

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
| `rypipe-core` | Pure Rust engine: `Value`, `ExecutionPlan`, `TableBuilder`, `ColumnarSink`, `RecordParser`, `Splitter`, parallel/bounded drivers, Arrow export |
| `rypipe-xml` | Crystal Reports XML adapter: `CrystalXmlDecoder`, `CrystalXmlSplitter` |
| `rypipe-python` | PyO3 bindings exposing `read_to_columnar*` entry points and reusable export helpers |

## Python quick start

```bash
export PYO3_PYTHON=/path/to/python3.12
maturin develop --release
```

```python
import _rypipe as rp

table = rp.read_to_columnar_par(
    "report.xml",
    row_tag="Row",
    field_types={"amount": "float64", "qty": "int64"},
    filter={"field": "status", "op": "==", "value": "active"},
)
print(table.num_rows, table.num_columns)
```

## Rust quick start

```rust
use rypipe_core::{InputBuffer, TableBuilder, ExecutionPlan};
use rypipe_xml::{CrystalXmlDecoder, CrystalXmlSplitter};

let input = InputBuffer::open("report.xml".as_ref(), false, false)?;
let mut builder = TableBuilder::with_plan(1024, ExecutionPlan::new());
CrystalXmlDecoder::with_row_tag(b"Row").parse_chunk(input.as_slice(), &mut builder)?;
let batch = builder.finish()?;
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

## Why rypipe?

`rypipe` was extracted from [crxml](https://github.com/emiliano-go/crxml), a
high-performance Crystal Reports XML parser. The goal was to keep crxml's
speed while making the same engine available for JSON, CSV, HTML, and other
formats through a small adapter interface.

## License

MIT
