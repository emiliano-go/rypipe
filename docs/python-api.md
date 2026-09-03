# Python API

`rypipe-python` builds a mixed Rust/Python package. The public API lives in the
`rypipe` package; `_rypipe` is the low-level Rust extension that adapter
packages build on.

`rypipe` itself does **not** ship any format parsers. Install a separate adapter
package and import it; the adapter registers itself with `rypipe` so the
high-level `read` API works. Adapters can also expose a `Source` subclass and
get the pipeline/stage/sink API for free.

!!! note
    **Adapters are written in Rust** for performance. Python users consume
    data through `rypipe.read()` and the pipeline API without writing Rust.
    Only adapter authors need to implement the Rust `Splitter` and
    `RecordParser` traits (see [Writing a format adapter](./writing-adapters.md)).

## Building the Python module

```bash
export PYO3_PYTHON=/path/to/python3.12
maturin develop --release
```

`maturin` builds `crates/rypipe-python/Cargo.toml` and installs both the `rypipe`
Python package and the `_rypipe` Rust extension.

## Public API (`import rypipe`)

### `rypipe.Source`

Abstract base class for row-oriented file sources. Adapter packages subclass it
and implement `_read_arrow`. Once they do, users get pipelines, stages, and
sinks with no extra work.

```python
from rypipe import Source

class MySource(Source):
    def _read_arrow(self, plan_overrides=None):
        # Build plan from construction kwargs + overrides, call the parser,
        # return a pyarrow.Table.
        ...
```

### `rypipe.Adapter`

Even simpler: subclass `Adapter` and implement only ``read(path, **kwargs)``.
Plan kwargs are merged and passed through automatically.

```python
from rypipe import Adapter

class CsvAdapter(Adapter):
    def read(self, path, **kwargs):
        return _rypipe_csv.read_csv(path, **kwargs)

source = CsvAdapter("data.csv")
```

A `Source` exposes:

- Row iteration: `for row in source`
- Table export: `source.to_arrow()`, `source.to_pandas()`, `source.to_polars()`,
  `source.to_parquet(path)`
- Pipeline operator: `source | RenameFields(...)`
- Caching: `source.clear_cache()`

### Pipeline stages

`rypipe` ships fusable pipeline stages. Stages that rename, drop, cast, or
filter constants are pushed into the Rust parse loop when the source supports
plan kwargs.

```python
from rypipe import RenameFields, DropFields, CastTypes, FilterRows

pipeline = (
    source
    | RenameFields({"old_name": "new_name"})
    | DropFields(["internal_id"])
    | CastTypes({"amount": float, "qty": int})
    | FilterRows(field="status", op="==", value="active")
)
```

`CastTypes` accepts Python callables (`int`, `float`, `bool`, `str`). When the
callable maps to a Rust type (`int64`, `float64`, `bool`), it is fused into the
Rust parse loop.

`FilterRows` supports both constant filters and column-to-column comparisons:

```python
FilterRows(field="status", op="==", value="active")
FilterRows(field_a="amount", op=">", field_b="threshold")
```

Supported ops: `==`, `!=`, `>`, `<`, `>=`, `<=`.

Boolean combinators (`FilterRowsAny`, `FilterRowsAll`, `FilterRowsNot`) are
fusable and build predicate trees that are evaluated per-row inside the Rust
parse loop:

```python
from rypipe import FilterRowsAny, FilterRowsAll, FilterRowsNot

# OR, AND, NOT (keyword-form leaves only; callables are not fusable)
FilterRowsAny(
    FilterRows(field="status", op="==", value="active"),
    FilterRows(field_a="age", op=">=", field_b="min_age"),
)
FilterRowsAll(
    FilterRows(field="region", op="==", value="us"),
    FilterRows(field="status", op="==", value="active"),
)
FilterRowsNot(FilterRows(field="flag", op="==", value="deleted"))

# Chaining FilterRows stages is an implicit AND:
pipeline = src | FilterRows(field="a", op="==", value="1") | FilterRows(field="b", op="==", value="2")
# equivalent to FilterRowsAll(...)

# Arbitrary nesting via the filter dict:
filter={"or": [{"field": "status", "op": "==", "value": "active"}, {"not": {"field": "flag", "op": "==", "value": "deleted"}}]}
filter={"and": [{"field": "a", "op": "==", "value": "1"}, {"field_a": "x", "op": ">", "field_b": "y"}]}
```

### Pipeline sinks

```python
from rypipe import collect, to_arrow, to_dataframe, to_csv, to_parquet

rows = collect(pipeline)
table = to_arrow(pipeline)
df = to_dataframe(pipeline)
to_csv(pipeline, "out.csv")
to_parquet(pipeline, "out.parquet")
```

Sinks try the fused Arrow path first and fall back to dict iteration when the
pipeline ends with a generic stage.

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

You can also pass an adapter object directly:

```python
table = rypipe.read("data.myfmt", adapter=my_adapter, row_tag="Row")
```

### `rypipe.read_par`

Convenience wrapper that passes `chunks` to the adapter.

```python
table = rypipe.read_par("data.myfmt", chunks=8, field_types={"amount": "float64"})
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
Arrow `RecordBatch`es into a single `pyarrow.Table`;
`record_batches_to_pyarrow_batches` exports them as a list of individual
`pyarrow.RecordBatch` objects for streaming-style APIs.

## Plan kwargs

All public `read` functions and `Source` constructors accept the same pushdown
kwargs, which are passed through to the adapter.

| Kwarg | Type | Effect |
|-------|------|--------|
| `rename` / `field_mapping` | `dict[str, str]` | Rename raw fields. |
| `drop` / `drop_fields` | `list[str]` | Drop fields by resolved name. |
| `field_types` | `dict[str, str]` | Cast columns to `"int64"`, `"float64"`, `"bool"`, `"dictionary"`, `"string"`, `"date32"`, or `"timestamp"` (also `"timestamp[s]"`, `"timestamp[ms]"`, `"timestamp[us]"`, `"timestamp[ns]"`). |
| `dictionary_columns` | `list[str]` | Explicit dictionary encoding. |
| `filter` | `dict` | Per-row filter (see below). |
| `schema` | `list[str]` | Output column order. |
| `auto_dict` | `bool` | Upgrade low-cardinality string columns to dictionary. |
| `auto_dict_threshold` | `float` | Max distinct/row ratio for auto-dict (default `0.05`). |
| `auto_dict_max_size` | `int` | Max dictionary entries for auto-dict (default `256`). |
| `use_mmap` | `bool` | Memory-map the input file. |
| `prefault` | `bool` | `MADV_WILLNEED` when mmap is enabled. |

## Filters

Constant equality/inequality (evaluated per-row during parse):

```python
filter={"field": "status", "op": "==", "value": "active"}
filter={"field": "status", "op": "!=", "value": "archived"}
```

Column-to-column comparison (evaluated per-row during parse with native-typed
comparison; numeric columns promote Int64/Float64):

```python
filter={"field_a": "amount", "op": ">", "field_b": "threshold"}
```

Supported comparison ops: `>`, `<`, `>=`, `<=`, `==`, `!=`. Mismatched
non-numeric types or null operands fail the row.

Boolean combinators use the same leaf shapes above with
`{"and": [spec, ...]}`, `{"or": [spec, ...]}`, and `{"not": spec}` and may be
nested arbitrarily; evaluation is per-row with short-circuiting, so missing-
field and type-mismatch behaviour of leaves is preserved (a negated missing
field, for example, keeps the row).

## Streaming batches

```python
import rypipe

# Bounded-memory read yielding pyarrow.RecordBatch objects.
for batch in rypipe.read_batches("huge.csv", memory="256MiB"):
    process(batch)

# Same, from a Source or Pipeline.
src = MySource("huge.csv")
for batch in src.iter_arrow_batches(batch_size=10_000):
    process(batch)
```

`read_batches` and `iter_arrow_batches` yield Arrow record batches one at a
time. Memory is bounded during parsing; batches are currently produced from a
materialized table, so peak memory is not yet fully incremental.

## See also

- [Rust API](./rust-api.md): the Rust engine and `Pipeline` API.
- [Writing a format adapter](./writing-adapters.md): adding formats as separate packages.
- [Architecture](./architecture/): design overview.
