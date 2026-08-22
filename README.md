# rypipe

Generic columnar engine for XML, JSON, CSV, HTML and other row-oriented formats.

`rypipe` separates **format-specific** parsing (splitting, row extraction) from
**format-agnostic** execution (typed column builders, filtering, projection,
dictionary encoding, parallel scheduling, memory-bounded execution, and Arrow export).

The first adapter, `rypipe-xml`, implements the Crystal Reports XML grammar
extracted from [crxml](https://github.com/emiliano-go/crxml).

## Crates

| Crate | Purpose |
|-------|---------|
| `rypipe-core` | Pure Rust engine: `Value`, `ExecutionPlan`, `TableBuilder`, `ColumnarSink`, `RecordParser`, `Splitter`, parallel/bounded drivers, Arrow export |
| `rypipe-xml` | Crystal Reports XML adapter: `CrystalXmlDecoder`, `CrystalXmlSplitter` |
| `rypipe-python` | PyO3 bindings exposing `read_to_columnar*` entry points and reusable export helpers |

## Building

```bash
export PYO3_PYTHON=/path/to/python3.12
maturin develop --release
```

## Testing

```bash
cargo test --workspace
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

## License

MIT
