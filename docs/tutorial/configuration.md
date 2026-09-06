# Configuration { #configuration }

!!! note

    The kwargs shown here are for the **rypipe_log** adapter. Other adapters may
    accept different parameters or have different defaults.

This page is a reference for all **rypipe** options and kwargs.

## rypipe.read() options { #rypipe-read-options}

```python
rypipe.read(
    path,                          # str or Path: file path (required)
    *,                             # keyword-only from here
    format=None,                   # str: adapter name (e.g. "crxml")
    adapter=None,                  # adapter object with read() method
    **kwargs,                      # forwarded to the adapter
)
```

### Parameters { #read-parameters}

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | `str \| PathLike` | *(required)* | Path to the input file. |
| `format` | `str \| None` | `None` | Adapter name. When omitted, inferred from the file extension. |
| `adapter` | `Any \| None` | `None` | Adapter object with a `read(path, **kwargs)` method. Overrides `format`. |
| `**kwargs` | : | : | Forwarded to the adapter. Each adapter defines its own kwargs. |

### Common adapter kwargs { #common-adapter-kwargs}

These kwargs are defined by **rypipe** and forwarded to adapters that support
them:

| Kwarg | Type | Description |
|-------|------|-------------|
| `field_mapping` | `dict[str, str]` | Rename columns: `{"old_name": "new_name"}`. |
| `drop_fields` | `list[str]` | Columns to skip entirely. |
| `filter` | `dict` | Pushdown filter spec (see [Pipeline](pipeline.md#pipeline)). |
| `field_types` | `dict[str, str]` | Type hints: `{"col": "int64"}`. |
| `dictionary_columns` | `list[str]` | Columns to dictionary-encode. |
| `schema` | `list[str]` | Expected column names and order. |
| `auto_dict` | `bool` | Auto-dictionary-encode low-cardinality string columns. |

!!! note

    Not all adapters support all kwargs. Check your adapter's documentation
    for supported options. Unsupported kwargs are silently ignored.


## Source constructor options { #source-constructor-options}

When using a Source class directly:

```python
from rypipe_log import LogSource

src = LogSource(
    path,                          # str or Path: file path (required)
    *,
    field_mapping=None,            # dict[str, str]: rename columns
    drop_fields=None,              # list[str]: columns to skip
    filter=None,                   # dict: pushdown filter spec
    field_types=None,              # dict[str, str]: type hints
    dictionary_columns=None,       # list[str]: dict-encode columns
    schema=None,                   # list[str]: expected column order
    auto_dict=False,               # bool: auto-dictionary encoding
    use_mmap=True,                 # bool: use memory-mapped I/O
    batch_size=1024,               # int: rows per iteration batch
)
```

### Parameters { #source-parameters}

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | `str \| Path` | *(required)* | Path to the input file. Must exist. |
| `field_mapping` | `dict[str, str]` | `None` | Rename columns during parsing. |
| `drop_fields` | `list[str]` | `None` | Skip these columns entirely. |
| `filter` | `dict \| None` | `None` | Pushdown filter applied during parsing. |
| `field_types` | `dict[str, str]` | `None` | Type hints for columns. |
| `dictionary_columns` | `list[str]` | `None` | Dictionary-encode these columns. |
| `schema` | `list[str]` | `None` | Expected column names and order. |
| `auto_dict` | `bool` | `False` | Auto-dict for low-cardinality string columns. |
| `use_mmap` | `bool` | `True` | Use memory-mapped file I/O. |
| `batch_size` | `int` | `1024` | Rows per batch during iteration. |

## Filter spec format { #filter-spec-format}

The `filter` parameter accepts a dictionary with these forms:

### Constant filter { #filter-constant}

```python
{"field": "status", "op": "==", "value": "active"}
```

### Column comparison { #filter-compare}

```python
{"field_a": "price", "op": ">", "field_b": "cost"}
```

### Compound filters { #filter-compound}

```python
# AND
{"and": [spec1, spec2]}

# OR
{"or": [spec1, spec2]}

# NOT
{"not": spec1}
```

### Supported operators { #filter-operators}

| Operator | Meaning |
|----------|---------|
| `"=="` | Equal |
| `"!="` | Not equal |
| `">"` | Greater than |
| `"<"` | Less than |
| `">="` | Greater or equal |
| `"<="` | Less or equal |

## Streaming options { #streaming-options}

Pass `memory=` to any sink for bounded-memory streaming:

```python
from rypipe_log import LogSource

src = LogSource("huge.log")

# DataFrame streaming
df = src.to_pandas(memory="64MiB")
df = src.to_pandas(memory="64MiB", threads=16)  # parallel streaming

# Parquet streaming
src.to_parquet("output.parquet", memory="64MiB")

# Polars streaming
df = src.to_polars(memory="64MiB")

# Collect with bounded memory
rows = collect(src, memory="64MiB")
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `memory` | `int \| str` | `None` | Memory budget per chunk (e.g. `"64MiB"`). `None` = materialize full table. |
| `threads` | `int \| None` | `None` | Threads for parallel streaming. `None` = single-threaded. Pass `threads=16` for higher throughput. |

For batch-level control, use `iter_record_batches` directly:

```python
for batch in src.iter_record_batches(memory="64MiB", threads=16):
    process(batch)
```

## register_adapter() { #register-adapter}

Adapter packages call this at import time:

```python
import rypipe

rypipe.register_adapter(
    "log",                         # str: adapter name
    LogAdapter(),                  # object with read() method
    extensions=[".log"],           # list[str]: file extensions
)
```

After registration, `rypipe.read("data.log")` auto-detects the extension.

## Exceptions { #exceptions}

| Exception | Meaning |
|-----------|---------|
| `rypipe.RypipeError` | General API error (no adapter, file not found). |
| `rypipe.ParseError` | File could not be parsed (malformed data). |
| `rypipe.PlanError` | Invalid plan kwargs (bad filter, unknown type). |
| `rypipe.MergeError` | Schema mismatch between chunks. |

## Adapter creator reference { #adapter-creator-reference }

When building an adapter, you need to decide which kwargs your Source
accepts and how to forward them to your Rust backend.

### Common kwargs (recommended) { #common-kwargs }

These are defined by rypipe and forwarded to adapters that support them.
Implementing them gives users a consistent experience across adapters:

| Kwarg | Rust type | Purpose |
|-------|-----------|---------|
| `field_mapping` | `HashMap<String, String>` | Rename columns during parsing |
| `drop_fields` | `Vec<String>` | Skip columns entirely |
| `filter` | `HashMap<String, String>` | Pushdown filter predicate |
| `field_types` | `HashMap<String, String>` | Type hints for columns |
| `dictionary_columns` | `Vec<String>` | Dictionary-encode columns |
| `schema` | `Vec<String>` | Expected column names and order |
| `auto_dict` | `bool` | Auto-dictionary low-cardinality strings |

### Forwarding kwargs to Rust { #forwarding-kwargs }

Your Source's `_read_arrow` must merge construction-time kwargs with
pipeline overrides from fused stages:

```python
class LogSource(Source):
    def _read_arrow(self, plan_overrides=None):
        plan = self._build_plan_kwargs()  # construction-time kwargs
        if plan_overrides:
            plan.update(plan_overrides)   # fused pipeline stages
        return _rypipe_log.read(str(self._path), **plan)
```

If you ignore `plan_overrides`, fused stages silently fall back to
Python execution (10-50x slower).

### Streaming kwargs { #streaming-kwargs }

Your Source's `iter_record_batches` should accept `memory` and forward
it to your Rust streaming entry point. Pass `**kwargs` to support
`threads` for parallel streaming:

```python
class LogSource(Source):
    def iter_record_batches(self, memory="64MiB", batch_size=None, **kwargs):
        plan = self._build_plan_kwargs()
        return _rypipe_log.iter_batches(
            str(self._path), memory=memory, batch_size=batch_size, **plan
        )
```

### Custom kwargs { #custom-kwargs }

Add adapter-specific kwargs to your Source constructor and store them
as instance attributes. Always call `super().__init__()`:

```python
class LogSource(Source):
    def __init__(self, path, *, row_tag="Row", threads=0, **kwargs):
        self._row_tag = row_tag
        self._threads = threads
        super().__init__(path, **kwargs)  # handles common kwargs
```

### Engine selection { #engine-selection }

rypipe-core provides four execution modes. Your adapter wires two
entry points:

| Entry point | Modes | Purpose |
|-------------|-------|---------|
| `_read_arrow()` | columnar, parallel, stream | Full-table reads (to_arrow, to_pandas without memory=) |
| `iter_record_batches()` | stream, parallel_streaming | Bounded-memory reads (to_pandas(memory=...), to_parquet(memory=...)) |

`resolve_engine` picks the optimal mode based on file size, memory
budget, and threads:

```python
from rypipe import resolve_engine

class LogSource(Source):
    def __init__(self, path, *, engine="auto", **kwargs):
        self._engine = engine
        super().__init__(path, **kwargs)

    def _resolve_engine(self) -> str:
        if self._engine != "auto":
            return self._engine
        return resolve_engine(
            file_size=self._path.stat().st_size,
            memory=self._memory,
            threads=self._threads,
            schema=self._schema or None,
            has_parallel=_HAS_PARALLEL,
            has_columnar=_HAS_COLUMNAR,
        )

    def _read_arrow(self, plan_overrides=None):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)

        engine = self._resolve_engine()
        if engine == "parallel":
            return _rypipe_log.read_par(str(self._path), **plan)
        elif engine == "stream":
            return _rypipe_log.read_stream(str(self._path), **plan)
        else:  # columnar (default)
            return _rypipe_log.read(str(self._path), **plan)

    def iter_record_batches(self, memory="64MiB", batch_size=None, **kwargs):
        plan = self._build_plan_kwargs()
        return _rypipe_log.iter_batches(
            str(self._path), memory=memory, batch_size=batch_size, **plan
        )
```

The [Streaming](streaming.md#streaming) tutorial showed a simple `_read_arrow` calling one
mode. For adapters with multiple modes, use `resolve_engine` to
dispatch in `_read_arrow`. `iter_record_batches` always streams
regardless of the engine mode.

## Recap { #recap }

* `rypipe.read()` infers the adapter from the file extension.
* Source constructors accept schema hints, filters, and type overrides.
* Filter specs support constant, column comparison, and compound forms.
* Streaming uses `memory` to bound per-chunk memory usage, `threads` for parallel.
* Register adapters with `rypipe.register_adapter()`.
* Adapter creators: implement common kwargs, forward `plan_overrides`, use `resolve_engine` for auto mode.
