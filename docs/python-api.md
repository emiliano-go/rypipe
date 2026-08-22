# Python API

`rypipe-python` compiles to an extension module named `_rypipe`. It exposes the
same columnar entry points that crxml historically used, plus typed exceptions.

## Building the Python module

```bash
export PYO3_PYTHON=/path/to/python3.12
maturin develop --release
```

`maturin` builds `crates/rypipe-python/Cargo.toml` and installs the module as
`_rypipe`.

## Exceptions

| Exception | Meaning |
|-----------|---------|
| `_rypipe.XmlError` | Malformed input or parse failure (including invalid UTF-8). |
| `_rypipe.PlanError` | Invalid pushdown plan (unknown field type, bad filter op). |
| `_rypipe.MergeError` | Chunk-merge conflict (e.g. type mismatch across chunks). |

## `read_to_columnar`

Single-threaded, whole-file parse.

```python
import _rypipe as rp

table = rp.read_to_columnar(
    "data.xml",
    row_tag="Row",
    field_mapping={"old_name": "new_name"},
    drop_fields=["internal_id"],
    field_types={"amount": "float64", "qty": "int64"},
    dictionary_columns=["status"],
    filter={"field": "status", "op": "==", "value": "active"},
    schema=["id", "status", "amount"],
    auto_dict=False,
    use_mmap=False,
    prefault=False,
)
```

Returns a `pyarrow.Table`.

## `read_to_columnar_multi`

Sequential chunked parse + merge. Useful when you want deterministic chunking
without rayon overhead.

```python
table = rp.read_to_columnar_multi(
    "data.xml",
    row_tag="Row",
    num_chunks=4,
    field_types={"amount": "float64"},
)
```

## `read_to_columnar_par`

Parallel chunked parse via rayon.

```python
table = rp.read_to_columnar_par(
    "data.xml",
    row_tag="Row",
    num_chunks=8,
    field_types={"amount": "float64"},
)
```

Use more chunks than CPU cores (crxml uses `threads * 4`) for better load
balancing.

## `read_to_columnar_bounded`

Memory-bounded parse. Reads the file in batches sized to fit within `memory`
bytes of intermediate builder storage.

```python
table = rp.read_to_columnar_bounded(
    "huge.xml",
    memory=500_000_000,  # 500 MB
    row_tag="Row",
    field_types={"amount": "float64"},
)
```

## Plan kwargs

All four functions accept the same pushdown kwargs.

| Kwarg | Type | Effect |
|-------|------|--------|
| `row_tag` | `str` | XML row element name (default `"Row"`). |
| `field_mapping` | `dict[str, str]` | Rename raw fields. |
| `drop_fields` | `list[str]` | Drop fields by resolved name. |
| `field_types` | `dict[str, str]` | Cast columns to `"int64"`, `"float64"`, `"bool"`, `"dictionary"`, or `"string"`. |
| `dictionary_columns` | `list[str]` | Explicit dictionary encoding. |
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
