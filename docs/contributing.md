# Contributing { #contributing }

Thank you for your interest in contributing to **rypipe**!

## Getting started { #getting-started }

### Prerequisites { #prerequisites }

* Rust toolchain (1.78+) — [rustup.rs](https://rustup.rs)
* Python 3.10+
* `maturin` (`pip install maturin`)
* `pytest` (`pip install pytest`)

### Clone and build { #clone-and-build }

```bash
git clone https://github.com/emiliano-go/rypipe.git
cd rypipe

# Build the Rust extension in development mode
maturin develop

# Run the tests
pytest crates/rypipe-python/tests/
```

## Project structure { #project-structure }

```
rypipe/
├── rypipe/                    # Python package
│   ├── __init__.py            # Public API, adapter registry
│   ├── source.py              # Source, Adapter base classes
│   ├── pipeline.py            # Pipeline (| operator)
│   ├── fusion.py              # Plan fusion
│   ├── batchpipe.py           # Batch pipeline
│   ├── sinks.py               # Terminal sinks
│   └── stages/                # Pipeline stages
│       ├── cast.py            # CastTypes
│       ├── filter.py          # FilterRows + combinators
│       ├── rename.py          # RenameFields
│       └── drop.py            # DropFields
├── crates/
│   ├── rypipe-core/           # Rust engine
│   │   ├── src/               # 18 modules
│   │   └── tests/             # Integration tests
│   └── rypipe-python/         # PyO3 bindings
│       ├── src/
│       └── tests/             # Python tests
├── docs/                      # Documentation
└── benchmarks/                # Performance benchmarks
```

## How to contribute { #how-to-contribute }

### Bug reports { #bug-reports }

Open an issue on [GitHub](https://github.com/emiliano-go/rypipe/issues)
with:

* A minimal reproducible example
* Expected vs actual behavior
* Python version, OS, and `rypipe` version

### Code contributions { #code-contributions }

1. Fork the repository.
2. Create a feature branch: `git checkout -b my-feature`.
3. Make your changes with tests.
4. Run the test suite: `pytest crates/rypipe-python/tests/`.
5. Run the Rust tests: `cargo test --workspace`.
6. Submit a pull request.

### Writing an adapter { #writing-an-adapter }

If you've built an adapter for a new format, we'd love to list it! Open a
PR adding your adapter to the table in `docs/index.md`.

See the [Writing Adapters](./writing-adapters/index.md) guide for how to
build an adapter package.

### Improving documentation { #improving-documentation }

Documentation lives in `docs/`. We use zensical for the docs site. To
preview locally:

```bash
pip install zensical
zensical serve
```

Then open `http://127.0.0.1:8000` in your browser.

## Code style { #code-style }

### Rust { #rust }

* Use `cargo fmt` before committing.
* Use `cargo clippy` and fix all warnings.
* Prefer `Cow::Borrowed` over `Cow::Owned` when possible.
* Always check `sink.wants()` before extracting field values.

### Python { #python }

* Follow PEP 8.
* Type hints on all public functions.
* No comments unless explaining non-obvious logic.

## Running benchmarks { #running-benchmarks }

```bash
# Rust throughput benchmark
cargo bench --features bench

# Python benchmark wrapper
python benchmarks/bench_throughput.py
```

## License { #license }

By contributing, you agree that your contributions will be licensed under
the MIT License.
