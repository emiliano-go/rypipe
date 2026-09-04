# Python API Reference { #python-api }

This page is a reference for the **rypipe** Python API. For a tutorial,
see the [Tutorial](../tutorial/index.md).

## rypipe.read() { #rypipe-read }

Read a file into a `pyarrow.Table` using a registered adapter.

```python
rypipe.read(
    path,                     # str | PathLike — file path
    *,
    format=None,              # str | None — adapter name
    adapter=None,             # object with read() method
    **kwargs,                 # forwarded to the adapter
) -> pyarrow.Table
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | `str \| PathLike` | *(required)* | Path to the input file. |
| `format` | `str \| None` | `None` | Adapter name. Inferred from extension when omitted. |
| `adapter` | `Any \| None` | `None` | Adapter object. Overrides `format`. |
| `**kwargs` | — | — | Forwarded to the adapter's `read()` method. |

**Returns:** `pyarrow.Table`

**Raises:** `rypipe.RypipeError` if no adapter is registered for the format.

```python
import rypipe
import crxml  # registers the crxml adapter

# Format inferred from extension
table = rypipe.read("report.xml", row_tag="Details")

# Format specified explicitly
table = rypipe.read("data.txt", format="log")

# Adapter passed directly
from rypipe_log import LogAdapter
table = rypipe.read("app.log", adapter=LogAdapter())
```

## rypipe.read_par() { #rypipe-read-par }

Read a file in parallel using a registered adapter.

```python
rypipe.read_par(
    path,
    *,
    chunks=4,                 # int — number of parallel chunks
    **kwargs,
) -> pyarrow.Table
```

## rypipe.read_stream() { #rypipe-read-stream }

Read a file with bounded memory using a registered adapter.

```python
rypipe.read_stream(
    path,
    *,
    memory="64MiB",           # int | str — memory budget
    **kwargs,
) -> pyarrow.Table
```

## rypipe.read_batches() { #rypipe-read-batches }

Read a file and yield `pyarrow.RecordBatch` objects incrementally.

```python
rypipe.read_batches(
    path,
    *,
    memory="64MiB",           # int | str — memory budget
    batch_size=None,          # int | None — rows per batch
    **kwargs,
) -> Iterator[pyarrow.RecordBatch]
```

## rypipe.iter_record_batches() { #rypipe-iter-record-batches }

Stream a file into Arrow `RecordBatch` objects with constant memory.

```python
rypipe.iter_record_batches(
    path,
    *,
    format=None,              # str | None — adapter name
    adapter=None,             # object with read() method
    memory="64MiB",           # int | str — memory budget
    batch_size=None,          # int | None — rows per batch
    **kwargs,
) -> Iterator[pyarrow.RecordBatch]
```

## rypipe.register_adapter() { #rypipe-register-adapter }

Register a format adapter with **rypipe**.

```python
rypipe.register_adapter(
    name,                     # str — adapter name
    adapter,                  # object with read() method
    extensions=None,          # Iterable[str] | None — file extensions
) -> None
```

After registration, `rypipe.read("file.ext")` auto-detects the extension.

## Source { #source }

Abstract base class for row-oriented file sources.

```python
class Source(ABC):
    def __init__(
        self,
        path,                          # str | Path
        *,
        field_mapping=None,            # dict[str, str]
        drop_fields=None,              # list[str]
        filter=None,                   # dict | None
        field_types=None,              # dict[str, str]
        dictionary_columns=None,       # list[str]
        schema=None,                   # list[str]
        auto_dict=False,               # bool
        use_mmap=True,                 # bool
        batch_size=1024,               # int
    )
```

**Abstract method:**

```python
@abstractmethod
def _read_arrow(self, plan_overrides: dict | None = None) -> pyarrow.Table:
    ...
```

**Public methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `.to_arrow()` | `pyarrow.Table` | Parse and cache the table. |
| `.to_pandas(dtype_backend="pyarrow")` | `pd.DataFrame` | Convert to pandas. |
| `.to_dataframe(dtype_backend="pyarrow")` | `pd.DataFrame` | Alias for `.to_pandas()`. |
| `.to_polars()` | `pl.DataFrame` | Convert to Polars. |
| `.to_parquet(path, **kwargs)` | `None` | Write to Parquet. |
| `.schema()` | `list[str]` | Column names from first row. |
| `.clear_cache()` | `None` | Drop cached table. |
| `.iter_arrow_batches(batch_size=None)` | `Iterator[RecordBatch]` | Yield batches. |
| `.iter_record_batches(memory="64MiB", batch_size=None)` | `Iterator[RecordBatch]` | Stream batches. |
| `.__iter__()` | `Iterator[dict]` | Iterate rows as dicts. |
| `.__or__(stage)` | `Pipeline` | Pipe operator for stages. |

## Adapter { #adapter }

Convenience base class. Subclasses implement `read()` instead of
`_read_arrow()`.

```python
class Adapter(Source):
    def read(self, path: str, **kwargs) -> pyarrow.Table:
        raise NotImplementedError
```

## Pipeline { #pipeline }

A chain of stages applied to a Source.

```python
class Pipeline:
    def __or__(self, stage) -> Pipeline:
        """Append a stage and return a new Pipeline."""

    def __iter__(self) -> Iterator[dict]:
        """Iterate rows as dicts."""

    def iter_arrow_batches(self, batch_size=None) -> Iterator[RecordBatch]:
        """Yield Arrow RecordBatch objects."""

    def iter_record_batches(self, memory="64MiB", batch_size=None) -> Iterator[RecordBatch]:
        """Stream batches with constant memory."""
```

## Stages { #stages }

Pipeline stages transform streams of dicts. Import from the adapter package.

### RenameFields { #renamefields }

```python
RenameFields(mapping: dict[str, str])
```

Rename columns. Fields not in the mapping pass through unchanged.

### DropFields { #dropfields }

```python
DropFields(fields: list[str])
```

Remove columns. Passing a bare string raises `TypeError`.

### CastTypes { #casttypes }

```python
CastTypes(mapping: dict[str, Callable])
```

Cast column values. Supported callables: `int`, `float`, `str`, `bool`.

### FilterRows { #filterrows }

```python
FilterRows(
    predicate=None,            # Callable — arbitrary filter
    *,
    field=None,                # str — column name (constant filter)
    op=None,                   # str — operator
    value=None,                # str — value (constant filter)
    field_a=None,              # str — left column (comparison)
    field_b=None,              # str — right column (comparison)
)
```

**Constant filter operators:** `==`, `eq`, `!=`, `ne`

**Comparison operators:** `>`, `<`, `>=`, `<=`, `==`, `!=`, `gt`, `lt`,
`ge`, `le`, `eq`, `ne`

### FilterRowsAny { #filterrowsany }

```python
FilterRowsAny(*filters: FilterRows)  # requires >= 2 filters
```

Keep rows matching **any** filter (OR).

### FilterRowsAll { #filterrowsall }

```python
FilterRowsAll(*filters: FilterRows)  # requires >= 2 filters
```

Keep rows matching **all** filters (AND).

### FilterRowsNot { #filterrowsnot }

```python
FilterRowsNot(inner: FilterRows)  # exactly 1 filter
```

Negate a filter.

## Sinks { #sinks }

Standalone functions for materializing pipeline results.

| Function | Returns | Description |
|----------|---------|-------------|
| `rypipe.collect(pipeline)` | `list[dict]` | Collect all rows. |
| `rypipe.to_arrow(pipeline)` | `pyarrow.Table` | Materialize to table. |
| `rypipe.to_pandas(pipeline)` | `pd.DataFrame` | Materialize to pandas. |
| `rypipe.to_polars(pipeline)` | `pl.DataFrame` | Materialize to Polars. |
| `rypipe.to_csv(pipeline, path, ...)` | `None` | Write to CSV. |
| `rypipe.to_parquet(pipeline, path, ...)` | `None` | Write to Parquet. |

### to_csv() { #to-csv }

```python
rypipe.to_csv(
    pipeline,                  # Iterable[dict]
    path,                      # str | Path
    encoding="utf-8",          # str
    delimiter=",",             # str
    fieldnames=None,           # list[str] | None
) -> None
```

## Exceptions { #exceptions }

| Exception | Parent | Meaning |
|-----------|--------|---------|
| `rypipe.RypipeError` | `RuntimeError` | General API error. |
| `rypipe.ParseError` | `Exception` | File could not be parsed. |
| `rypipe.XmlError` | `ParseError` | XML-specific parse error. |
| `rypipe.PlanError` | `Exception` | Invalid plan kwargs. |
| `rypipe.MergeError` | `Exception` | Schema mismatch between chunks. |
