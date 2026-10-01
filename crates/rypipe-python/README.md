# rypipe-python

PyO3 bindings over [`rypipe-core`](https://crates.io/crates/rypipe-core).
This crate is the Rust side of the [`rypipe`](https://pypi.org/project/rypipe/)
Python package: it exposes the engine to Python and provides the helpers that
native adapter crates call across the Python boundary.

It is not a separate Python distribution. The compiled `_rypipe` extension and
the `rypipe` Python package are built from this crate by `maturin` and shipped
as the [`rypipe`](https://pypi.org/project/rypipe/) wheel.

Most users want the Python package:

```bash
pip install rypipe
```

Depend on this crate directly only when you are building a native Python
adapter, where the adapter crate is compiled into its own wheel.

## Usage

```toml
[dependencies]
rypipe_python = { package = "rypipe-python", version = "0.5.0" }
```

```rust
use rypipe_python::{
    execution_plan_from_kwargs, py_err_from_rypipe, record_batches_to_pyarrow_table,
};
```

The dependency alias gives the helper a Rust name independent of its Python
extension name. Errors use the same Python exception classes across adapters,
and `rypipe-core` must be pinned to a compatible version so the engine traits
unify.

## Public API

`execution_plan_from_kwargs`, `record_batches_to_pyarrow_table`,
`record_batch_to_pyarrow`, and `py_err_from_rypipe`, plus the typed exceptions
in `_rypipe.errors`.

## Documentation

Full guides live at [rypipe.emiliano-go.com](https://rypipe.emiliano-go.com/),
including the
[adapter walkthrough](https://rypipe.emiliano-go.com/building-adapters/walkthrough/)
and the
[Rust API](https://rypipe.emiliano-go.com/rust-api/).

## License

MIT
