# Configuration { #configuration }

This page is a reference for all **rypipe** options and kwargs.

## rypipe.read() options { #rypipe-read-options}

```python
rypipe.read(
    path,                          # str or Path — file path (required)
    *,                             # keyword-only from here
    format=None,                   # str — adapter name (e.g. "crxml")
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
| `**kwargs` | — | — | Forwarded to the adapter. Each adapter defines its own kwargs. |

### Common adapter kwargs { #common-adapter-kwargs}

These kwargs are defined by **rypipe** and forwarded to adapters that support
them:

| Kwarg | Type | Description |
|-------|------|-------------|
| `field_mapping` | `dict[str, str]` | Rename columns: `{"old_name": "new_name"}`. |
| `drop_fields` | `list[str]` | Columns to skip entirely. |
| `filter` | `dict` | Pushdown filter spec (see [Pipeline](#pipeline)). |
| `field_types` | `dict[str, str]` | Type hints: `{"col": "int64"}`. |
| `dictionary_columns` | `list[str]` | Columns to dictionary-encode. |
| `schema` | `list[str]` | Expected column names and order. |
| `auto_dict` | `bool` | Auto-dictionary-encode low-cardinality string columns. |

/// note

Not all adapters support all kwargs. Check your adapter's documentation
for supported options. Unsupported kwargs are silently ignored.

///

## Source constructor options { #source-constructor-options}

When using a Source class directly:

```python
from crxml import CrystalXMLSource

src = CrystalXMLSource(
    path,                          # str or Path — file path (required)
    *,
    field_mapping=None,            # dict[str, str] — rename columns
    drop_fields=None,              # list[str] — columns to skip
    filter=None,                   # dict — pushdown filter spec
    field_types=None,              # dict[str, str] — type hints
    dictionary_columns=None,       # list[str] — dict-encode columns
    schema=None,                   # list[str] — expected column order
    auto_dict=False,               # bool — auto-dictionary encoding
    use_mmap=True,                 # bool — use memory-mapped I/O
    batch_size=1024,               # int — rows per iteration batch
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
| `"=="`, `"eq"` | Equal |
| `"!="`, `"ne"` | Not equal |
| `">"`, `"gt"` | Greater than |
| `"<"`, `"lt"` | Less than |
| `">="`, `"ge"` | Greater or equal |
| `"<="`, `"le"` | Less or equal |

## Streaming options { #streaming-options}

```python
src.iter_record_batches(
    memory="64MiB",               # str or int — memory budget per chunk
    batch_size=None,              # int or None — rows per batch
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `memory` | `int \| str` | `"64MiB"` | Maximum memory for each parsing chunk. |
| `batch_size` | `int \| None` | `None` | Rows per batch. Auto-sized when `None`. |

## register_adapter() { #register-adapter}

Adapter packages call this at import time:

```python
import rypipe

rypipe.register_adapter(
    "myfmt",                       # str — adapter name
    MyAdapter(),                   # object with read() method
    extensions=[".myfmt", ".fmt"], # list[str] — file extensions
)
```

After registration, `rypipe.read("file.myfmt")` auto-detects the extension.

## Exceptions { #exceptions}

| Exception | Meaning |
|-----------|---------|
| `rypipe.RypipeError` | General API error (no adapter, file not found). |
| `rypipe.ParseError` | File could not be parsed (malformed data). |
| `rypipe.PlanError` | Invalid plan kwargs (bad filter, unknown type). |
| `rypipe.MergeError` | Schema mismatch between chunks. |

## Recap { #recap }

* `rypipe.read()` infers the adapter from the file extension.
* Source constructors accept schema hints, filters, and type overrides.
* Filter specs support constant, column comparison, and compound forms.
* Streaming uses `memory` to bound per-chunk memory usage.
* Register adapters with `rypipe.register_adapter()`.
