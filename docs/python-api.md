# Python API { #python-api }

`rypipe` provides the engine; adapters are separate packages. End users
**import the adapter** (e.g. `crxml`), not `rypipe`. Adapter creators
import `rypipe` to subclass `Source`/`Adapter` and register with the engine.

## End users { #end-users }

### Import the adapter, not rypipe { #import-the-adapter-not-rypipe }

```python
from crxml import CrystalXMLSource

src = CrystalXMLSource("report.xml", row_tag="Details")
df = src.to_dataframe()
```

Each adapter exports its own `Source` class and pipeline stages. You do not
need to import `rypipe` directly.

### Two APIs: Source class vs rypipe.read { #two-apis-source-class-vs-rypiperead }

**Source class (recommended):** Direct access to the adapter's parser, with
full pipeline support:

```python
from crxml import CrystalXMLSource, RenameFields, DropFields, CastTypes, FilterRows

src = CrystalXMLSource("report.xml", row_tag="Details")

table = (
    src
    | RenameFields({"{Report.InvoiceNo}": "invoice"})
    | DropFields(["{Report.TaxRate}"])
    | CastTypes({"amount": float})
    | FilterRows(field="status", op="==", value="active")
).to_arrow()
```

**rypipe.read (optional):** If `rypipe` is also installed, adapters register
themselves automatically. This provides a generic entry point:

```python
import rypipe  # optional: only needed for the generic API

table = rypipe.read("report.xml", format="crxml", row_tag="Details")
```

Most users should prefer the Source class API. `rypipe.read` is useful for
generic ETL scripts that handle multiple formats.

### Pipeline stages { #pipeline-stages }

| Stage | Effect |
|-------|--------|
| `RenameFields({"old": "new"})` | Rename fields |
| `DropFields(["field"])` | Drop fields |
| `CastTypes({"col": int})` | Cast to `int64`, `float64`, `bool`, or `str` |
| `FilterRows(field="col", op="==", value="x")` | Filter rows |
| `FilterRows(field_a="a", op=">", field_b="b")` | Column-to-column comparison |
| `FilterRowsAny(...)`, `FilterRowsAll(...)`, `FilterRowsNot(...)` | Boolean combinators |

Stages are fused into the Rust parse loop when the adapter supports plan
kwargs. Without fusion, they fall back to Python execution over a full table.

### Sinks { #sinks }

Sinks convert pipeline results to various output formats. Source methods
(`to_arrow`, `to_pandas`, etc.) materialize the full result set. Pipeline
sinks (`to_dataframe`, `to_csv`, `collect`) work on the pipeline object
directly and try the fused Arrow path first, falling back to dict iteration.

```python
from crxml import to_dataframe, to_csv, collect

# Source methods: materialize the full result { #source-methods-materialize-the-full-result }
table = src.to_arrow()
df = src.to_pandas()
df = src.to_polars()
src.to_parquet("out.parquet")

# Pipeline sinks: work on pipeline objects { #pipeline-sinks-work-on-pipeline-objects }
df = to_dataframe(pipeline)
to_csv(pipeline, "out.csv")
rows = collect(pipeline)
```

### Streaming batches { #streaming-batches }

For files larger than available RAM, use `iter_record_batches` with a memory
budget. The engine parses chunks sequentially, keeping only the current chunk
in memory. This is slower than the full-table path but allows processing
files of any size.

```python
for batch in src.iter_record_batches(memory="256MiB"):
    process(batch)
```

Yields `pyarrow.RecordBatch` objects one at a time. The `memory` parameter
accepts an int (bytes) or a human-readable string like `"64MB"` or `"500MiB"`.

### Plan kwargs { #plan-kwargs }

These kwargs control how the engine parses and transforms data. They are
passed to the adapter's `read` method. When the adapter supports fusion,
these kwargs are pushed into the Rust parse loop for maximum performance.

| Kwarg | Type | Effect |
|-------|------|--------|
| `field_types` | `dict[str, str]` | Cast columns to `"int64"`, `"float64"`, `"bool"`, `"dictionary"`, `"string"`, `"date32"`, `"timestamp"` |
| `dictionary_columns` | `list[str]` | Explicit dictionary encoding |
| `schema` | `list[str]` | Output column order (skips discovery) |
| `filter` | `dict` | Per-row filter (see below) |
| `auto_dict` | `bool` | Upgrade low-cardinality strings to dictionary |
| `use_mmap` | `bool` | Memory-map the input file |
| `prefault` | `bool` | `MADV_WILLNEED` when mmap is enabled |

### Filters { #filters }

Filters are evaluated per-row during parsing. Rows that fail the filter are
discarded before any Python objects are created. Use constant filters for
simple equality checks, column-to-column comparisons for range queries, and
boolean combinators for complex logic.

Constant equality/inequality:

```python
FilterRows(field="status", op="==", value="active")
```

Column-to-column comparison:

```python
FilterRows(field_a="amount", op=">", field_b="threshold")
```

Boolean combinators:

```python
from crxml import FilterRowsAny, FilterRowsAll, FilterRowsNot

FilterRowsAny(
    FilterRows(field="status", op="==", value="active"),
    FilterRows(field_a="age", op=">=", field_b="min_age"),
)
```

### Exceptions { #exceptions }

These exceptions are raised by the adapter and can be caught with
`try/except`:

| Exception | Meaning |
|-----------|---------|
| `crxml.XmlError` | Malformed input or parse failure |
| `crxml.PlanError` | Invalid pushdown plan |
| `crxml.MergeError` | Chunk-merge conflict |

### Schema performance { #schema-performance }

When the column set is known, pass `schema=` to skip discovery and hit the
fast path. This is the single largest performance lever:

```python
schema = src.schema()  # discover once
fast = CrystalXMLSource("report.xml", row_tag="Details", schema=schema)
df = fast.to_dataframe()
```

Or use `discover_schema` for batch workloads:

```python
from crxml import discover_schema

schema = discover_schema("sample.xml")  # 5 ms once
for f in files:
    src = CrystalXMLSource(f, row_tag="Details", schema=schema)
    for batch in src.iter_record_batches(memory="64MB"):
        writer.write_batch(batch)
```

See [Schema](./advanced/schema-and-types.md) for details.

## Adapter authors { #adapter-authors }

Adapter authors implement the Rust `Splitter` and `RecordParser` traits for
a new format, then expose a Python class that registers with `rypipe`. See
[Writing a format adapter](./writing-adapters/) for the full guide.

### `rypipe.Source` { #rypipesource }

Abstract base class for adapters. Implement `_read_arrow` to get pipelines,
stages, and sinks for free:

```python
from rypipe import Source

class MySource(Source):
    def _read_arrow(self, *, plan_overrides=None, **kwargs):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        # Pass merged plan to your Rust reader
        return my_rust_read(str(self._path), **plan)
```

### `rypipe.Adapter` { #rypipeadapter }

Simpler alternative. Implement only `read(path, **kwargs)`:

```python
from rypipe import Adapter

class CsvAdapter(Adapter):
    def read(self, path, **kwargs):
        return _rypipe_csv.read_csv(path, **kwargs)
```

### Registration { #registration }

Register your adapter so `rypipe.read()` can find it:

```python
import rypipe

rypipe.register_adapter("csv", CsvAdapter(), extensions=[".csv"])
```

### Pure-Python adapters { #pure-python-adapters }

You can write an adapter entirely in Python without Rust:

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

## See also { #see-also }

- [Rust API](./rust-api.md): the Rust engine and `Pipeline` API
- [Writing a format adapter](./writing-adapters/): adding formats as separate packages
- [Architecture](./architecture/): design overview
