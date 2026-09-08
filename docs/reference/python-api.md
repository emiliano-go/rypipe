# Python API Reference { #python-api }

This page is a reference for the **rypipe** Python API. For a tutorial,
see the [Tutorial](../tutorial/index.md).

## rypipe.read() { #rypipe-read }

Read a file into a `pyarrow.Table` using a registered adapter.

```python
rypipe.read(
    path,                     # str | PathLike: file path
    *,
    format=None,              # str | None: adapter name
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
| `**kwargs` | : | : | Forwarded to the adapter's `read()` method. |

**Returns:** `pyarrow.Table`

**Raises:** `rypipe.RypipeError` if no adapter is registered for the format.

```python
import rypipe
import crxml  # registers the crxml adapter

# Format inferred from extension
table = rypipe.read("report.xml", row_tag="Details")

# Format specified explicitly
table = rypipe.read("data.txt", format="crxml")

# Adapter passed directly
from crxml import CrystalXMLAdapter
table = rypipe.read("report.xml", adapter=CrystalXMLAdapter(), row_tag="Details")
```

## rypipe.read_par() { #rypipe-read-par }

Read a file in parallel using a registered adapter.

```python
rypipe.read_par(
    path,
    *,
    chunks=4,                 # int: number of parallel chunks
    **kwargs,
) -> pyarrow.Table
```

## rypipe.read_stream() { #rypipe-read-stream }

Read a file with bounded memory using a registered adapter.

```python
rypipe.read_stream(
    path,
    *,
    memory="64MiB",           # int | str: memory budget
    **kwargs,
) -> pyarrow.Table
```

## rypipe.read_batches() { #rypipe-read-batches }

Read a file and yield `pyarrow.RecordBatch` objects incrementally.

```python
rypipe.read_batches(
    path,
    *,
    memory="64MiB",           # int | str: memory budget
    batch_size=None,          # int | None: rows per batch
    **kwargs,
) -> Iterator[pyarrow.RecordBatch]
```

## rypipe.iter_record_batches() { #rypipe-iter-record-batches }

Stream a file into Arrow `RecordBatch` objects with constant memory.

```python
rypipe.iter_record_batches(
    path,
    *,
    format=None,              # str | None: adapter name
    adapter=None,             # object with read() method
    memory="64MiB",           # int | str: memory budget
    batch_size=None,          # int | None: rows per batch
    **kwargs,
) -> Iterator[pyarrow.RecordBatch]
```

## rypipe.register_adapter() { #rypipe-register-adapter }

Register a format adapter with **rypipe**.

```python
rypipe.register_adapter(
    name,                     # str: adapter name
    adapter,                  # object with read() method
    extensions=None,          # Iterable[str] | None: file extensions
) -> None
```

## rypipe.resolve_engine() { #rypipe-resolve-engine }

Resolve the best engine mode based on file characteristics and user options.
Adapters use this for `engine="auto"` selection.

```python
rypipe.resolve_engine(
    file_size,                # int: file size in bytes (required)
    *,                        # keyword-only from here
    memory=None,              # int | str | None: memory budget (e.g. "64MiB")
    threads=None,             # int | None: number of threads
    schema=None,              # list[str] | None: explicit column names
    has_parallel=True,        # bool: adapter has parallel support
    has_columnar=True,        # bool: adapter has columnar support
) -> str
```

Returns one of: `"columnar"`, `"parallel"`, `"stream"`, `"parallel_streaming"`.

### Heuristic rules { #resolve-engine-heuristic }

1. **`memory=` provided**: streaming mode.
   - `threads > 1` -> `"parallel_streaming"`
   - else -> `"stream"`

2. **`threads > 1`**: parallel mode.
   - file >= 100 MB -> `"parallel_streaming"`
   - else -> `"parallel"`

3. **`schema=` provided and file >= 100 MB**: streaming is 11% faster.
   - -> `"stream"`

4. **Default**:
   - file < 8 MB and `has_columnar` -> `"columnar"`
   - file >= 8 MB and `has_parallel` -> `"parallel"`
   - else -> `"stream"`

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
| `.to_pandas(memory=None, dtype_backend="pyarrow")` | `pd.DataFrame` | Convert to pandas. Pass `memory=` for streaming. |
| `.to_polars(memory=None)` | `pl.DataFrame` | Convert to Polars. Pass `memory=` for streaming. |
| `.to_parquet(path, memory=None, **kwargs)` | `None` | Write to Parquet. Pass `memory=` for streaming. |
| `.schema()` | `list[str]` | Column names from first row. |
| `.clear_cache()` | `None` | Drop cached table. |
| `.iter_arrow_batches(batch_size=None)` | `Iterator[RecordBatch]` | Yield batches. |
| `.iter_record_batches(memory="64MiB", batch_size=None)` | `Iterator[RecordBatch]` | Stream batches. |
| `.__iter__()` | `Iterator[dict]` | Iterate rows as dicts. |
| `.__or__(stage)` | `Pipeline` | Pipe operator for stages. |

### to_pandas() details { #to-pandas-details }

```python
# Standard (materializes full table)
source.to_pandas(dtype_backend="pyarrow")  # Arrow-backed dtypes (default)
source.to_pandas(dtype_backend="numpy")    # NumPy-backed dtypes

# Streaming (bounded memory)
source.to_pandas(memory="64MiB")
source.to_pandas(memory="64MiB", threads=16)  # parallel streaming
```

When `dtype_backend="pyarrow"` (default), string columns use
`pd.ArrowDtype(pa.string())`, zero-copy from Arrow. When
`dtype_backend="numpy"`, string columns use `pd.StringDtype()`, standard
pandas strings.

### to_parquet() details { #to-parquet-details }

Passes Parquet kwargs through to `pyarrow.parquet.ParquetWriter`:

```python
source.to_parquet("output.parquet")
source.to_parquet("output.parquet", compression="snappy")
source.to_parquet("output.parquet", compression="zstd", compression_level=9)
source.to_parquet("output.parquet", row_group_size=100_000)

# Streaming (bounded memory)
source.to_parquet("output.parquet", memory="64MiB")
source.to_parquet("output.parquet", memory="64MiB", threads=16)  # parallel
```

Common options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `compression` | `str` | `"snappy"` | Compression codec: `"snappy"`, `"gzip"`, `"zstd"`, `"lz4"`, `"none"` |
| `compression_level` | `int` | codec default | Compression level (codec-dependent) |
| `row_group_size` | `int` | `64*1024` | Rows per row group |
| `use_dictionary` | `bool \| list` | `True` | Enable/disable dictionary encoding |
| `write_statistics` | `bool` | `True` | Write column statistics |

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

    def to_pandas(self, memory=None, dtype_backend="pyarrow", **kwargs) -> pd.DataFrame:
        """Convert to pandas. Pass memory= for streaming."""

    def to_polars(self, memory=None, **kwargs) -> pl.DataFrame:
        """Convert to Polars. Pass memory= for streaming."""

    def to_parquet(self, path, memory=None, **kwargs) -> None:
        """Write to Parquet. Pass memory= for streaming."""
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
    predicate=None,            # Callable: arbitrary filter
    *,
    field=None,                # str: column name (constant/null/type filter)
    op=None,                   # str: operator
    value=None,                # str: value (constant filter)
    field_a=None,              # str: left column (comparison)
    field_b=None,              # str: right column (comparison)
    is_null=False,             # bool: keep rows where field is null/missing
    is_type=None,              # str: keep rows where field has this type
)
```

**Constant filter operators:** `==`, `!=`, `>`, `<`, `>=`, `<=` (with aliases
`eq`, `ne`, `gt`, `lt`, `ge`, `le`)

**Null check:** `FilterRows(field="Status", is_null=True)` keeps rows where
`Status` is null or missing.

**Type check:** `FilterRows(field="Amount", is_type="int64")` keeps rows
where `Amount` has the given type. Valid types: `string`, `int64`,
`float64`, `bool`/`boolean`, `dictionary`, `date32`, `timestamp`,
`decimal128`.

**Comparison operators:** `==`, `!=`, `>`, `<`, `>=`, `<=` (see
[Filter spec format](#filter-spec-format))

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

## Filter spec format { #filter-spec-format }

The `filter` option (accepted by `Source(...)` constructors,
`rypipe.read(**kwargs)`, and produced by fusable `FilterRows` stages) is a
dict with these forms:

### Constant filter { #filter-constant }

```python
{"field": "Status", "op": "==", "value": "Active"}
```

Constant filters support `==`, `!=`, `>`, `<`, `>=`, `<=` (aliases `eq`,
`ne`, `gt`, `lt`, `ge`, `le`).

### Null and type checks { #filter-null-type }

```python
{"field": "Status", "op": "is_null"}
{"field": "Amount", "op": "is_type", "value": "int64"}
```

`is_null` keeps rows where the field is null or missing. `is_type` keeps
rows where the field has the given type (`string`, `int64`, `float64`,
`bool`, `dictionary`, `date32`, `timestamp`, `decimal128`).

### Column comparison { #filter-compare }

Compares two columns in the same row:

```python
{"field_a": "price", "op": ">", "field_b": "cost"}
```

### Compound filters { #filter-compound }

```python
# AND
{"and": [spec1, spec2]}

# OR
{"or": [spec1, spec2]}

# NOT
{"not": spec1}
```

!!! note

    Compound forms are produced by `FilterRowsAny` / `FilterRowsAll` /
    `FilterRowsNot`. Not every adapter's parser accepts compound pushdown;
    check your adapter's documentation.

### Supported operators { #filter-operators }

| Operator | Aliases | Meaning | Constant | Column comparison |
|----------|---------|---------|----------|-------------------|
| `"=="` | `"eq"` | Equal | Yes | Yes |
| `"!="` | `"ne"` | Not equal | Yes | Yes |
| `">"` | `"gt"` | Greater than | Yes | Yes |
| `"<"` | `"lt"` | Less than | Yes | Yes |
| `">="` | `"ge"` | Greater or equal | Yes | Yes |
| `"<="` | `"le"` | Less or equal | Yes | Yes |
| `"is_null"` | | Field is null or missing | Yes (`is_null=True`) | No |
| `"is_type"` | | Field has the given type | Yes (`is_type="..."`) | No |

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
