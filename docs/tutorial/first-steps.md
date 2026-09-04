# First Steps { #first-steps }

This page covers `rypipe.read()` in depth: format inference, keyword
arguments, and schema hints.

## The basics { #the-basics }

```python
import rypipe
import crxml  # registers the adapter

table = rypipe.read("report.xml", row_tag="Details")
```

The `row_tag="Details"` argument is passed through to the **crxml** adapter.
Each adapter accepts its own kwargs; check the adapter's documentation for
what it supports.

## Format inference { #format-inference }

When you call `rypipe.read("file.xml")`, **rypipe** looks at the file
extension and selects the registered adapter:

```python
# These are equivalent:
table = rypipe.read("report.xml", row_tag="Details")
table = rypipe.read("report.xml", format="crxml", row_tag="Details")
```

You can also pass an adapter object directly:

```python
from rypipe_log import LogAdapter

table = rypipe.read("app.log", adapter=LogAdapter())
```

!!! tip

    If no adapter is registered for the extension, **rypipe** raises a
    `RypipeError` with a helpful message suggesting which adapter to install.


## Passing adapter kwargs { #passing-adapter-kwargs }

Any keyword argument you pass to `rypipe.read()` is forwarded to the adapter.
This means you do not need to learn a separate **rypipe** API: just learn
your adapter's options:

```python
# crxml options
table = rypipe.read(
    "report.xml",
    row_tag="Details",           # which XML element is a row
    field_types={"Amount": "float64"},  # type hints for the engine
    drop_fields=["InternalId"],  # skip this column entirely
)

# rypipe-log options
table = rypipe.read(
    "app.log",
    schema=["name", "age", "active"],  # declare column order
)
```

## Schema hints { #schema-hints }

You can speed up parsing and control column types by passing schema hints.
These are **adapter-optional**: not all adapters use them, but when they do,
the engine can skip schema discovery:

```python
table = rypipe.read(
    "report.xml",
    row_tag="Details",
    # Declare expected column names and order
    schema=["Name", "Amount", "Status", "Date"],
    # Override types (default is string for all columns)
    field_types={
        "Amount": "float64",
        "Date": "date32",
    },
)
```

### Supported field types { #supported-field-types }

| Type string | Arrow type | Python equivalent |
|------------|------------|-------------------|
| `"string"` | `string` | `str` (default) |
| `"int64"` | `int64` | `int` |
| `"float64"` | `float64` | `float` |
| `"bool"` or `"boolean"` | `bool` | `bool` |
| `"date32"` | `date32` | — |
| `"timestamp"` | `timestamp` | — |
| `"dictionary"` | `dictionary` | — |

## Filtering at read time { #filtering-at-read-time }

Some adapters support pushdown filters: the adapter applies the filter
during parsing, so rejected rows never reach Python:

```python
table = rypipe.read(
    "report.xml",
    row_tag="Details",
    filter={"field": "Status", "op": "==", "value": "Active"},
)
```

The filter spec is a dictionary with:

* `field`: column name
* `op`: comparison operator (`"=="`, `"!="`, `">"`, `"<"`, `">="`, `"<="`)
* `value`: the value to compare against

!!! note

    Pushdown filters are adapter-dependent. Check your adapter's documentation
    to see which filters are supported.


## Error handling { #error-handling }

**rypipe** raises specific exceptions for different error types:

```python
import rypipe
import rypipe_log

try:
    table = rypipe.read("bad_file.log")
except rypipe.ParseError as e:
    # The file could not be parsed (malformed data, encoding errors)
    print(f"Parse error: {e}")
except rypipe.PlanError as e:
    # Invalid plan kwargs (bad filter spec, unknown field type)
    print(f"Invalid plan: {e}")
except rypipe.MergeError as e:
    # Schema mismatch between chunks (different columns in different parts)
    print(f"Schema merge error: {e}")
except rypipe.RypipeError as e:
    # General API error (no adapter registered, file not found, etc.)
    print(f"API error: {e}")
```

## Recap { #recap }

* `rypipe.read(path)` infers the adapter from the file extension.
* Pass adapter-specific kwargs directly to `rypipe.read()`.
* Schema hints (`schema`, `field_types`) speed up parsing.
* Pushdown filters (`filter`) apply during parsing for better performance.
* Use `rypipe.ParseError`, `rypipe.PlanError`, `rypipe.MergeError`, or
  `rypipe.RypipeError` for error handling.

**Next:** [Pipeline](pipeline.md#pipeline): chain stages with the `|` operator.
