# First Steps { #first-steps }

!!! note

    The kwargs and options shown here are for the **crxml** adapter. Other
    adapters may accept different parameters. Check your adapter's docs.

This page covers reading files in depth: format inference, keyword
arguments, and schema hints.

## The basics { #the-basics }

```python
from crxml import CrystalXMLSource

source = CrystalXMLSource("report.xml", row_tag="Details")
table = source.to_arrow()
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
from crxml import CrystalXMLAdapter

table = rypipe.read("report.xml", adapter=CrystalXMLAdapter(), row_tag="Details")
```

!!! tip

    If no adapter is registered for the extension, **rypipe** raises a
    `RypipeError`.


## Passing adapter kwargs { #passing-adapter-kwargs }

Any keyword argument you pass to the Source is forwarded to the adapter.
This means you do not need to learn a separate API: just learn
your adapter's options:

```python
from crxml import CrystalXMLSource

source = CrystalXMLSource(
    "report.xml",
    row_tag="Details",           # which XML element is a row
    field_types={"Amount": "float64"},  # type hints for the engine
    drop_fields=["InternalId"],  # skip this column entirely
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

The adapter raises specific exceptions for different error types:

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
    # Schema mismatch between chunks (different columns in different parts)
    print(f"Schema merge error: {e}")
```

## Recap { #recap }

* `rypipe.read(path)` infers the adapter from the file extension.
* Pass adapter-specific kwargs directly to `rypipe.read()`.
* Schema hints (`schema`, `field_types`) speed up parsing.
* Pushdown filters (`filter`) apply during parsing for better performance.
* Use `rypipe.ParseError`, `rypipe.PlanError`, `rypipe.MergeError`, or
  `rypipe.RypipeError` for error handling.

**Next:** [Pipeline](pipeline.md#pipeline): chain stages with the `|` operator.
