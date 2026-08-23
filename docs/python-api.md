# Python API

`rypipe-python` builds a mixed Rust/Python package. The public API lives in the
`rypipe` package; `_rypipe` is the low-level Rust extension that adapter
packages build on.

`rypipe` itself does **not** ship any format parsers. Install a separate adapter
package and import it; the adapter registers itself with `rypipe` so the
high-level `read` API works.

## Building the Python module

```bash
export PYO3_PYTHON=/path/to/python3.12
maturin develop --release
```

`maturin` builds `crates/rypipe-python/Cargo.toml` and installs both the `rypipe`
Python package and the `_rypipe` Rust extension.

## Public API (`import rypipe`)

### `rypipe.register_adapter`

Adapter packages call this on import to make themselves available to `rypipe.read`.

```python
import rypipe

class MyAdapter:
    def read(self, path, **kwargs):
        ...

rypipe.register_adapter("myfmt", MyAdapter(), extensions=[".myfmt"])
```

The adapter object must expose a `read(path, **kwargs)` method that returns a
`pyarrow.Table`.

### `rypipe.read`

Single entry point for all registered adapters.

```python
import rypipe
import my_adapter  # registers the "myfmt" adapter

table = rypipe.read("data.myfmt")

# Same call with all common options:
table = rypipe.read(
    "data.myfmt",
    format="myfmt",          # inferred from extension when omitted
    fields={"amount": "float64", "qty": "int64"},
    dictionary=["status"],
    filter={"field": "status", "op": "==", "value": "active"},
    schema=["id", "status", "amount"],
    auto_dict=False,
    use_mmap=False,
    prefault=False,
)
```

Returns a `pyarrow.Table`.

You can also pass an adapter object directly:

```python
table = rypipe.read("data.myfmt", adapter=my_adapter, row_tag="Row")
```

### `rypipe.read_par`

Convenience wrapper that passes `chunks` to the adapter.

```python
table = rypipe.read_par("data.myfmt", chunks=8, fields={"amount": "float64"})
```

### `rypipe.read_stream`

Convenience wrapper that passes a memory budget to the adapter. `memory`
accepts an int (bytes) or a human-readable string such as `"128MiB"`.

```python
table = rypipe.read_stream("huge.myfmt", memory="500MiB", row_tag="Row")
```

### Format auto-detection

`rypipe.read` infers the adapter from the file extension when `format` is not
provided, but only for extensions registered by an installed adapter package.
If no adapter is registered, pass `format=` explicitly or install the adapter.

### Exceptions

| Exception | Meaning |
|-----------|---------|
| `rypipe.ParseError` | Malformed input or parse failure (including invalid UTF-8). |
| `rypipe.XmlError` | Backward-compatible alias of `ParseError`. |
| `rypipe.PlanError` | Invalid pushdown plan (unknown field type, bad filter op). |
| `rypipe.MergeError` | Chunk-merge conflict (e.g. type mismatch across chunks). |
| `rypipe.RypipeError` | Invalid API usage (bad memory string, unknown extension). |

## Low-level API (`import _rypipe`)

`_rypipe` is the Rust extension that adapter packages build on. It exposes the
shared exceptions and Rust helper functions; adapter packages implement the
actual `read` functions and call these helpers from their own PyO3 code.

### Exceptions

- `_rypipe.ParseError`
- `_rypipe.XmlError`
- `_rypipe.PlanError`
- `_rypipe.MergeError`

Adapter code raises these so users can catch them through `rypipe` as well.

### Rust helpers (used from adapter crates)

Adapter crates written in Rust use `rypipe_python` directly:

```rust
use rypipe_python::{execution_plan_from_kwargs, record_batches_to_pyarrow_table};
```

`execution_plan_from_kwargs` converts Python kwargs into a
`rypipe_core::ExecutionPlan`. `record_batches_to_pyarrow_table` turns a slice of
Arrow `RecordBatch`es into a single `pyarrow.Table`.

## Plan kwargs

All public `read` functions accept the same pushdown kwargs, which are passed
through to the adapter.

| Kwarg | Type | Effect |
|-------|------|--------|
| `rename` / `field_mapping` | `dict[str, str]` | Rename raw fields. |
| `drop` / `drop_fields` | `list[str]` | Drop fields by resolved name. |
| `fields` / `field_types` | `dict[str, str]` | Cast columns to `"int64"`, `"float64"`, `"bool"`, `"dictionary"`, or `"string"`. |
| `dictionary` / `dictionary_columns` | `list[str]` | Explicit dictionary encoding. |
| `filter` | `dict` | Per-row or post-reduce filter (see below). |
| `schema` | `list[str]` | Output column order. |
| `auto_dict` | `bool` | Upgrade low-cardinality string columns to dictionary. |
| `use_mmap` | `bool` | Memory-map the input file. |
| `prefault` | `bool` | `MADV_WILLNEED` when mmap is enabled. |

## Filters

Constant equality/inequality (evaluated per-row during parse):

```python
filter={"field": "status", "op": "==", "value": "active"}
filter={"field": "status", "op": "!=", "value": "archived"}
```

Column-to-column comparison (evaluated after the table is assembled):

```python
filter={"field_a": "amount", "op": ">", "field_b": "threshold"}
```

Supported comparison ops: `>`, `<`, `>=`, `<=`, `==`, `!=`.

## See also

- [Rust API](./rust-api.md): the Rust engine and `Pipeline` API.
- [Writing a format adapter](./writing-adapters.md): adding formats as separate packages.
- [Architecture](./architecture.md): design overview.
