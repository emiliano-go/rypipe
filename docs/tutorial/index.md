# Tutorial { #tutorial }

This tutorial teaches you how to use **rypipe** to read files into Arrow tables
and DataFrames, and how to build your own adapter. You do not need to write
Rust or build anything to get started.

## What is **rypipe**? { #what-is-rypipe }

**rypipe** is a format-agnostic columnar ingestion engine. It reads
row-oriented files (XML, CSV, JSONL, logs, etc.) and produces
<abbr title="Apache Arrow is a cross-language columnar memory format">Apache
Arrow</abbr> tables with near-zero Python overhead.

**rypipe** itself does not ship parsers. Instead, adapter packages provide
format-specific parsing. You install the adapter you need:

| Format | Adapter package | Extension |
|--------|----------------|-----------|
| Crystal Reports XML | `crxml` | `.xml` |

## Quick example { #quick-example }

Here is a complete example that reads a Crystal Reports XML file, renames
columns, filters rows, and produces a pandas DataFrame:

```python
from crxml import CrystalXMLSource, RenameFields, CastTypes, FilterRows

source = CrystalXMLSource("report.xml", row_tag="Details")

df = (
    source
    | RenameFields({"Name": "name"})
    | CastTypes({"Amount": float})
    | FilterRows(field="Status", op="==", value="Active")
).to_pandas()

print(df)
#     name  Amount  Status
# 0  Alice   150.0  Active
# 2  Carol   200.0  Active
```

Five lines of code. **rypipe** handled parallel parsing, schema discovery,
type coercion, filtering, and Arrow export automatically.

**We will explain every line of this in the following pages.**

## Recap { #recap }

* **rypipe** is a format-agnostic engine. Install an adapter for your format.
* Sources (`CrystalXMLSource`, etc.) give you caching and the pipeline
  operator.
* Convert to pandas with `.to_pandas()` or to Polars with `.to_polars()`.

**Next:** [First Steps](first-steps.md#first-steps), the Source abstraction
in depth.
