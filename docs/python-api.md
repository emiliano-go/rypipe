# Python API

`rypipe-python` builds a mixed Rust/Python package. The public API lives in the
`rypipe` package; `_rypipe` is the low-level Rust extension and is kept for
backward compatibility.

## Building the Python module

```bash
export PYO3_PYTHON=/path/to/python3.12
maturin develop --release
```

`maturin` builds `crates/rypipe-python/Cargo.toml` and installs both the `rypipe`
Python package and the `_rypipe` Rust extension.

## Public API (`import rypipe`)

### `rypipe.read`

Single entry point for all formats and execution modes.

```python
import rypipe

table = rypipe.read("data.xml")

# Same call with all common options:
table = rypipe.read(
    "data.xml",
    format="xml",            # inferred from extension when omitted
    row_tag="Row",
    mode="par",              # "sync" | "multi" | "par" | "stream"
    chunks=4,
    rename={"old_name": "new_name"},
    drop=["internal_id"],
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

### `rypipe.read_par`

Convenience wrapper for parallel mode.

```python
table = rypipe.read_par("data.xml", chunks=8, fields={"amount": "float64"})
```

### `rypipe.read_stream`

Convenience wrapper for bounded-memory streaming. `memory` accepts an int
(bytes) or a human-readable string such as `"128MiB"`.

```python
table = rypipe.read_stream("huge.xml", memory="500MiB", row_tag="Row")
```

### Format auto-detection

`rypipe.read` infers the format from the file extension when `format` is not
provided:

| Extension | Format |
|-----------|--------|
| `.xml` | `xml` |

Pass `format="xml"` explicitly for extensionless paths.

### Exceptions

| Exception | Meaning |
|-----------|---------|
| `rypipe.XmlError` | Malformed input or parse failure (including invalid UTF-8). |
| `rypipe.PlanError` | Invalid pushdown plan (unknown field type, bad filter op). |
| `rypipe.MergeError` | Chunk-merge conflict (e.g. type mismatch across chunks). |
| `rypipe.RypipeError` | Invalid API usage (bad memory string, unknown extension). |

## Low-level API (`import _rypipe`)

`_rypipe` exposes the same columnar entry points that crxml historically used,
plus a new generic `_rypipe.read` dispatch function.

### `_rypipe.read`

Format-agnostic dispatch used by the `rypipe` wrapper.

```python
import _rypipe

table = _rypipe.read(
    "data.xml",
    "xml",
    format_options={"row_tag": "Row"},
    mode="par",
    num_chunks=4,
    memory=64_000_000,
    field_types={"amount": "float64"},
)
```

### Legacy entry points

| Function | Mode |
|----------|------|
| `_rypipe.read_to_columnar` | sync |
| `_rypipe.read_to_columnar_multi` | sequential multi-chunk |
| `_rypipe.read_to_columnar_par` | parallel |
| `_rypipe.read_to_columnar_bounded` | bounded memory |

These accept the original crxml-style kwargs (`field_mapping`, `drop_fields`,
`field_types`, etc.).

## Plan kwargs

All public functions accept the same pushdown kwargs.

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

## Reusing the Rust helpers from another extension

If you are building a separate PyO3 crate (like crxml), you can depend on
`rypipe-python` for the plan/Export helpers, or just call `rypipe-core` and
`rypipe-xml` directly and copy the small export logic. See
[Integrating with crxml](./integrating-crxml.md).
