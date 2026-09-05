# First Steps { #first-steps }

!!! note

    The examples on this page use the **crxml** adapter. Other adapters may
    accept different parameters. Check your adapter's docs.

This page covers the core **rypipe** concepts: the Source abstraction, the
pipeline operator, stages, and sinks. **rypipe** provides these interfaces
as a framework, adapter packages implement them for each format.

## The Source { #the-source }

A **Source** is a handle over one input file. It parses the file into a
`pyarrow.Table` and provides caching, pipeline support, and multiple output
formats:

```python
from crxml import CrystalXMLSource

source = CrystalXMLSource("report.xml", row_tag="Details")

# Parse and get a table (cached on first call)
table = source.to_arrow()

# Convert to pandas
df = source.to_pandas()

# Convert to Polars
df_pl = source.to_polars()
```

The Source parses the file once and caches the result. Subsequent calls to
`to_arrow()`, `to_pandas()`, etc. reuse the cached table.

### What the Source gives you { #what-the-source-gives-you }

| Method | Returns | Description |
|--------|---------|-------------|
| `.to_arrow()` | `pyarrow.Table` | Parse and cache the table |
| `.to_pandas()` | `pd.DataFrame` | Convert to pandas |
| `.to_dataframe()` | `pd.DataFrame` | Alias for `.to_pandas()` |
| `.to_polars()` | `pl.DataFrame` | Convert to Polars |
| `.to_parquet(path)` | — | Write to Parquet |
| `.clear_cache()` | — | Drop cached table |
| `.schema()` | `list[str]` | Column names from first row |
| `.__iter__()` | `Iterator[dict]` | Iterate rows as dicts |

## The pipeline operator { #the-pipeline-operator}

The `|` operator chains transformation stages on a Source:

```python
from crxml import CrystalXMLSource
from crxml import RenameFields, CastTypes, FilterRows

source = CrystalXMLSource("report.xml", row_tag="Details")

result = (
    source
    | RenameFields({"Name": "name"})
    | CastTypes({"Amount": float})
    | FilterRows(field="Status", op="==", value="Active")
)

# Materialize to a table
table = result.to_arrow()

# Or to a DataFrame
df = result.to_dataframe()
```

Each `|` returns a new `Pipeline` — the original Source is not modified.

## Stages { #stages }

Stages transform streams of dicts. **rypipe** provides these stages:

| Stage | Purpose |
|-------|---------|
| `RenameFields` | Rename columns |
| `DropFields` | Remove columns |
| `CastTypes` | Cast column types |
| `FilterRows` | Filter rows by predicate |

All stages are fusable: **rypipe** pushes them into the Rust parse loop
for maximum performance. See [Stages](stages.md#stages) for details.

## Sinks { #sinks}

Sinks materialize pipeline results. You can use them as methods on a Source
or as standalone functions:

```python
from crxml import CrystalXMLSource, collect, to_dataframe

source = CrystalXMLSource("report.xml", row_tag="Details")

# As Source methods
table = source.to_arrow()
df = source.to_pandas()

# As standalone functions (on pipelines)
pipeline = source | FilterRows(field="Status", op="==", value="Active")
rows = collect(pipeline)
df = to_dataframe(pipeline)
```

See [Sinks](sinks.md#sinks) for the full reference.

## Streaming { #streaming}

For large files, use `iter_record_batches()` to process data in bounded
chunks:

```python
from crxml import CrystalXMLSource

source = CrystalXMLSource("huge_report.xml", row_tag="Details")

for batch in source.iter_record_batches(memory="64MiB"):
    # Each batch is a pyarrow.RecordBatch
    process(batch)
```

See [Streaming](streaming.md#streaming) for details.

## Error handling { #error-handling }

Adapters raise specific exceptions for different error types:

```python
from crxml import CrystalXMLSource, XmlError, PlanError, MergeError

try:
    source = CrystalXMLSource("bad_file.xml", row_tag="Details")
    table = source.to_arrow()
except XmlError as e:
    # The file could not be parsed (malformed XML, encoding errors)
    print(f"Parse error: {e}")
except PlanError as e:
    # Invalid plan kwargs (bad filter spec, unknown field type)
    print(f"Invalid plan: {e}")
except MergeError as e:
    # Schema mismatch between chunks
    print(f"Schema merge error: {e}")
```

!!! note

    Exception names vary by adapter. Check your adapter's docs for the
    specific exception types it raises.

## Recap { #recap }

* A **Source** parses a file into a `pyarrow.Table` with caching.
* The `|` operator chains stages into a **Pipeline**.
* **Stages** (`RenameFields`, `CastTypes`, `FilterRows`, `DropFields`)
  transform data with Rust-speed fusion.
* **Sinks** materialize results to tables, DataFrames, or files.
* **Streaming** processes large files with bounded memory.

**Next:** [Pipeline](pipeline.md#pipeline) — stages in depth.
