# Python API

`rypipe` is the engine; adapters are separate packages. This page covers the
Python API for both **end users** (consuming data) and **adapter authors**
(subclassing `Source`/`Adapter`).

!!! note
    `rypipe` itself does **not** ship any format parsers. Install an adapter
    package (e.g. `pip install crxml`) and import it. The adapter registers
    itself with `rypipe` so `rypipe.read()` works.

## End users

### `rypipe.read` -- single entry point

```python
import rypipe
import crxml  # registers the "crxml" adapter

table = rypipe.read("report.xml", row_tag="Details")
```

All common options are passed through to the adapter:

```python
table = rypipe.read(
    "report.xml",
    format="crxml",                    # inferred from extension when omitted
    row_tag="Details",                 # adapter-specific
    field_types={"amount": "float64", "qty": "int64"},
    dictionary_columns=["status"],
    schema=["id", "status", "amount"],  # column order + skip discovery
    filter={"field": "status", "op": "==", "value": "active"},
    auto_dict=False,
    use_mmap=False,
    prefault=False,
)
```

Returns a `pyarrow.Table`.

You can also pass an adapter object directly:

```python
table = rypipe.read("report.xml", adapter=my_crxml_instance, row_tag="Details")
```

### `rypipe.read_par` -- parallel read

Convenience wrapper that passes `chunks` to the adapter:

```python
table = rypipe.read_par("report.xml", chunks=8, row_tag="Details")
```

### `rypipe.read_stream` -- bounded-memory streaming

Convenience wrapper that passes a memory budget. `memory` accepts an int
(bytes) or a human-readable string:

```python
table = rypipe.read_stream("huge.xml", memory="500MiB", row_tag="Details")
```

### Streaming batches

```python
for batch in rypipe.read_batches("huge.xml", memory="256MiB", row_tag="Details"):
    process(batch)
```

Yields `pyarrow.RecordBatch` objects one at a time. Parsing memory is bounded
independently of input-file size.

### Pipeline API

When an adapter exposes a `Source` subclass, you get the pipeline `|` operator:

```python
from crxml import CrystalXMLSource
from rypipe import RenameFields, DropFields, CastTypes, FilterRows

src = CrystalXMLSource("report.xml", row_tag="Details")

table = (
    src
    | RenameFields({"{Report.InvoiceNo}": "invoice"})
    | DropFields(["{Report.TaxRate}"])
    | CastTypes({"amount": float})
    | FilterRows(field="status", op="==", value="active")
).to_arrow()
```

#### Stages

| Stage | Effect |
|-------|--------|
| `RenameFields({"old": "new"})` | Rename fields |
| `DropFields(["field"])` | Drop fields |
| `CastTypes({"col": int})` | Cast to `int64`, `float64`, `bool`, or `str` |
| `FilterRows(field="col", op="==", value="x")` | Filter rows |
| `FilterRows(field_a="a", op=">", field_b="b")` | Column-to-column comparison |
| `FilterRowsAny(...)`, `FilterRowsAll(...)`, `FilterRowsNot(...)` | Boolean combinators |

Stages that rename, drop, cast, or filter constants are fused into the Rust
parse loop when the adapter supports plan kwargs.

#### Sinks

```python
from rypipe import collect, to_arrow, to_dataframe, to_csv, to_parquet

rows = collect(pipeline)
table = to_arrow(pipeline)
df = to_dataframe(pipeline)
to_csv(pipeline, "out.csv")
to_parquet(pipeline, "out.parquet")
```

Or use `Source` methods directly:

```python
table = src.to_arrow()
df = src.to_pandas()
df = src.to_polars()
src.to_parquet("out.parquet")
```

### Plan kwargs

All `read` functions and `Source` constructors accept these pushdown kwargs:

| Kwarg | Type | Effect |
|-------|------|--------|
| `rename` / `field_mapping` | `dict[str, str]` | Rename raw fields |
| `drop` / `drop_fields` | `list[str]` | Drop fields by resolved name |
| `field_types` | `dict[str, str]` | Cast columns to `"int64"`, `"float64"`, `"bool"`, `"dictionary"`, `"string"`, `"date32"`, `"timestamp"` |
| `dictionary_columns` | `list[str]` | Explicit dictionary encoding |
| `filter` | `dict` | Per-row filter (see Filters below) |
| `schema` | `list[str]` | Output column order (skips discovery) |
| `auto_dict` | `bool` | Upgrade low-cardinality string columns to dictionary |
| `auto_dict_threshold` | `float` | Max distinct/row ratio for auto-dict (default `0.05`) |
| `auto_dict_max_size` | `int` | Max dictionary entries for auto-dict (default `256`) |
| `use_mmap` | `bool` | Memory-map the input file |
| `prefault` | `bool` | `MADV_WILLNEED` when mmap is enabled |

### Filters

Constant equality/inequality (evaluated per-row during parse):

```python
filter={"field": "status", "op": "==", "value": "active"}
filter={"field": "status", "op": "!=", "value": "archived"}
```

Column-to-column comparison (native-typed, with numeric promotion):

```python
filter={"field_a": "amount", "op": ">", "field_b": "threshold"}
```

Boolean combinators:

```python
filter={"or": [{"field": "status", "op": "==", "value": "active"},
               {"not": {"field": "flag", "op": "==", "value": "deleted"}}]}
filter={"and": [{"field": "a", op: "==", "value": "1"},
                {"field_a": "x", "op": ">", "field_b": "y"}]}
```

### Format auto-detection

`rypipe.read` infers the adapter from the file extension when `format` is not
provided. Only extensions registered by an installed adapter package work.
If no adapter is registered, pass `format=` explicitly or install the adapter.

### Exceptions

| Exception | Meaning |
|-----------|---------|
| `rypipe.ParseError` | Malformed input or parse failure |
| `rypipe.PlanError` | Invalid pushdown plan (unknown field type, bad filter op) |
| `rypipe.MergeError` | Chunk-merge conflict (type mismatch across chunks) |
| `rypipe.RypipeError` | Invalid API usage (bad memory string, unknown extension) |

## Adapter authors

### `rypipe.Source`

Abstract base class for row-oriented file sources. Implement `_read_arrow`
to get pipelines, stages, and sinks for free:

```python
from rypipe import Source

class MySource(Source):
    def _read_arrow(self, *, plan_overrides=None, **kwargs):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        # Pass merged plan to your Rust reader:
        return my_rust_read(str(self._path), **plan)
```

Once implemented, `MySource` exposes:

- Row iteration: `for row in source`
- Table export: `source.to_arrow()`, `source.to_pandas()`, `source.to_polars()`
- Pipeline operator: `source | RenameFields(...)`
- Batch iteration: `source.iter_arrow_batches(batch_size=10_000)`
- Caching: `source.clear_cache()`

### `rypipe.Adapter`

Simpler alternative: subclass `Adapter` and implement only `read(path, **kwargs)`.
Plan kwargs are merged automatically:

```python
from rypipe import Adapter

class CsvAdapter(Adapter):
    def read(self, path, **kwargs):
        return _rypipe_csv.read_csv(path, **kwargs)

source = CsvAdapter("data.csv")
```

### Registration

```python
import rypipe

rypipe.register_adapter("csv", CsvAdapter(), extensions=[".csv"])
```

Now `rypipe.read("data.csv")` works automatically.

### Pure-Python adapters

You can write an adapter entirely in Python:

```python
import rypipe, pyarrow as pa

class JSONAdapter(rypipe.Adapter):
    def read(self, path, **kwargs):
        import json
        with open(path) as f:
            data = json.load(f)
        return pa.table({col: [row[col] for row in data] for col in data[0]})

rypipe.register_adapter("json", JSONAdapter(), extensions=[".json"])
```

> **Performance warning:** Pure-Python adapters run at Python speed. The Rust
> `Splitter`/`RecordParser` traits deliver 4+ GB/s via SIMD scanning and
> zero-copy export. Pure Python is typically 10-50x slower. Use it for
> correctness and prototyping; use Rust for throughput.

## Low-level API (`import _rypipe`)

`_rypipe` is the Rust extension that adapter packages build on. It exposes
shared exceptions and Rust helpers; adapter crates implement the actual
`read` functions.

### Rust helpers (used from adapter crates)

```rust
use rypipe_python::{execution_plan_from_kwargs, record_batches_to_pyarrow_table};
```

- `execution_plan_from_kwargs`: Python kwargs to `ExecutionPlan`
- `record_batches_to_pyarrow_table`: `&[RecordBatch]` to `pyarrow.Table`
- `record_batches_to_pyarrow_batches`: streaming export as `list[RecordBatch]`

## See also

- [Rust API](./rust-api.md): the Rust engine and `Pipeline` API
- [Writing a format adapter](./writing-adapters/): adding formats as separate packages
- [Architecture](./architecture/): design overview
