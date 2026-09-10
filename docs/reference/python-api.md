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
    schema=None,              # list[str] | None: projected column names, in order
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
        schema=None,                   # list[str]: project exactly these columns, in this order
        auto_dict=False,               # bool
        strict_types=False,            # bool: reject malformed data instead of nulling
        observer=None,                 # dict[str, callable] | None: row observer hooks
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
| `.to_pandas(memory=None, dtype_backend="pyarrow", **kwargs)` | `pd.DataFrame` | Convert to pandas. Pass `memory=` for streaming, `threads` in `**kwargs`. |
| `.to_polars(memory=None, **kwargs)` | `pl.DataFrame` | Convert to Polars. Pass `memory=` for streaming, `threads` in `**kwargs`. |
| `.to_parquet(path, memory=None, **kwargs)` | `None` | Write to Parquet. Pass `memory=` for streaming, Parquet options in `**kwargs`. |
| `.schema()` | `list[str]` | Column names from first row. |
| `.clear_cache()` | `None` | Drop cached table. |
| `.iter_arrow_batches(batch_size=None)` | `Iterator[RecordBatch]` | Yield batches. |
| `.iter_record_batches(memory="64MiB", batch_size=None)` | `Iterator[RecordBatch]` | Stream batches. |
| `.__iter__()` | `Iterator[dict]` | Iterate rows as dicts. All values are strings; use `CastTypes` or `field_types` for real types. |
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

## discover_schema() { #discover-schema }

Discover column names for a file without a full parse. Scans the file once
and returns the column names after applying `field_mapping`, `drop_fields`,
etc.

```python
crxml.discover_schema(
    source,                     # str | Path: file path
    *,
    row_tag="Details",          # str: row element name
    field_mapping=None,         # dict[str, str] | None
    drop_fields=None,           # list[str] | None
    filter=None,                # dict | None
    field_types=None,           # dict[str, str] | None
    dictionary_columns=None,    # list[str] | None
    schema=None,                # list[str] | None
    auto_dict=False,            # bool
) -> list[str]
```

**Returns:** `list[str]` — column names in output order.

## CrystalXMLSource { #crystalxmlsource }

Concrete `Source` subclass for Crystal Reports XML files (provided by the
`crxml` adapter). Extends `Source.__init__` with adapter-specific params:

```python
class CrystalXMLSource(Source):
    def __init__(
        self,
        source,                          # str | Path
        *,
        row_tag="Row",                   # str: XML element name for one row
        engine="auto",                   # "auto" | "stream" | "columnar" | "parallel"
        threads=0,                       # int: parser threads (0 = all cores)
        memory=None,                     # str | int | None: memory bound
        chunks=None,                     # int | None: number of parallel chunks
        max_split_chunks=None,           # int | None: max split chunks
        # ... plus all Source.__init__ kwargs ...
    )
```

Additional methods beyond `Source`:

| Method | Returns | Description |
|--------|---------|-------------|
| `.to_arrow(combine=False)` | `pyarrow.Table` | Parse and cache. `combine=True` merges chunked columns. |
| `.iter_record_batches(memory="64MiB", batch_size=None, threads=None)` | `Iterator[RecordBatch]` | Stream batches with parallel support. |

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
    def __init__(self, source, stages=None, *, batch_size=1024):
        """Create a pipeline. Usually built via source | stage, not directly."""

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
    predicate=None,            # Callable or rypipe.expr.Predicate
    *,
    field=None,                # str: column name (constant filter)
    op=None,                   # str: operator
    value=None,                # str: value (constant filter)
    field_a=None,              # str: left column (comparison)
    field_b=None,              # str: right column (comparison)
    is_null=None,              # bool: True keeps null/missing rows, False drops them
    is_type=None,              # str: keep rows where field matches this type
)
```

**Constant filter operators:** `==`, `!=`, `>`, `<`, `>=`, `<=`, `regex`,
`starts_with`, `ends_with`, `contains`
(the value is a regular expression for `regex`, searched against the string
form of the field; invalid patterns raise at construction time)

**Comparison operators:** `==`, `!=`, `>`, `<`, `>=`, `<=`

**Type check values:** `string`, `int64`, `float64`, `bool`, `date32`, `timestamp`, `decimal128`

`predicate` may also be an expression predicate built with
[`col()`](#expression-api); those fuse into the Rust parse loop just like the
keyword forms. A plain callable (lambda or function) runs in Python as a
fallback and is not fusable.

### Expression API { #expression-api }

```python
import crxml

crxml.FilterRows(crxml.col("amount") > 100)
crxml.FilterRows((crxml.col("age") >= 18) & crxml.col("name").startswith("A"))
```

Adapters re-export `col` (crxml does), so users never import `rypipe.expr`.
`col(name)` references a column. Comparisons (`==`, `!=`, `>`, `<`, `>=`,
`<=`) accept a literal or another `col(...)`. Methods:

| Method | Meaning |
|--------|---------|
| `isin(values)` / `not_in(values)` | Membership test |
| `startswith(s)` / `endswith(s)` / `contains(s)` | String prefix/suffix/substring |
| `matches(pattern)` | Regex search (validated at construction) |
| `between(lo, hi)` | Inclusive range, `lo <= x <= hi` |
| `is_null()` / `is_not_null()` | Null presence checks |
| `is_type(t)` | Type check (same values as `is_type=`) |

Predicates compose with `&` (and), `|` (or), `~` (not) into arbitrarily
nested trees. Everything an expression builds is fusable; anything the spec
language cannot express raises at construction time. See
[Expression filters](../architecture/expressions.md).

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

### ObservedStage { #observedstage }

Base class for stages whose side effects must survive fusion. Override
`observer_hooks()` to return a hook dict:

```python
from rypipe.stages import ObservedStage

class CountRejected(ObservedStage):
    def __init__(self):
        self.count = 0

    def observer_hooks(self):
        return {"on_row_rejected": self._count}

    def _count(self, row_index):
        self.count += 1
```

Valid hook keys: `on_begin_row(row_index)`, `on_put_field(row_index, name,
slot, value)`, `on_row_accepted(row_index)`, `on_row_rejected(row_index)`,
`on_chunk_finished(total, accepted, rejected)`. Hooks fire from parse
threads; they must be thread-safe, and exceptions in hooks are printed and
swallowed. The same dict can be passed to a source as `observer=`.
See [Observer hooks](../advanced/stage-protocol.md#observer-hooks).

### FilterRowsNot { #filterrowsnot }

```python
FilterRowsNot(inner: FilterRows)  # exactly 1 filter
```

Negate a filter.

## Sinks { #sinks }

Standalone functions for materializing pipeline results. All functions
accept `memory=` for bounded-memory streaming when the pipeline supports
`iter_record_batches()`.

| Function | Returns | Description |
|----------|---------|-------------|
| `rypipe.collect(pipeline, memory=None)` | `list[dict]` | Collect all rows. |
| `rypipe.to_arrow(pipeline)` | `pyarrow.Table` | Materialize to table. |
| `rypipe.to_pandas(pipeline, memory=None, dtype_backend="pyarrow")` | `pd.DataFrame` | Materialize to pandas. |
| `rypipe.to_polars(pipeline, memory=None)` | `pl.DataFrame` | Materialize to Polars. |
| `rypipe.to_csv(pipeline, path, ...)` | `None` | Write to CSV. |
| `rypipe.to_parquet(pipeline, path, memory=None, ...)` | `None` | Write to Parquet. |

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
