# Tutorial { #tutorial }

!!! note

    The examples on this page use the **crxml** adapter. Other adapters may
    have different kwargs, options, or behavior. Check your adapter's
    documentation for specifics.

This tutorial teaches you how to use **rypipe** to read files into Arrow tables
and DataFrames. You do not need to write Rust or build anything.

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

## Installation { #installation }

```bash
pip install crxml
```

This installs **crxml** (the Crystal Reports XML adapter) and its
dependency **rypipe** (the engine). For other formats, install the
corresponding adapter.

## Your first read { #your-first-read }

```python
from crxml import CrystalXMLSource

source = CrystalXMLSource("report.xml", row_tag="Details")
table = source.to_arrow()

print(table.schema)
# Name: string
# Department: string
# Amount: string
# Status: string
# Date: string

print(table.num_rows)
# 15
```

That's it. The adapter parsed the file in parallel and returned a
`pyarrow.Table`.

### What **rypipe** does automatically { #what-rypipe-does }

With just that one call, **rypipe**:

* Splits the file into chunks for parallel parsing.
* Discovers the schema from the data.
* Builds Arrow column arrays with near-zero copy.
* Returns a `pyarrow.Table` you can use directly.

## Getting a DataFrame { #getting-a-dataframe }

The table is already a `pyarrow.Table`, but you can convert it to pandas or
Polars with one call:

```python
from crxml import CrystalXMLSource

source = CrystalXMLSource("report.xml", row_tag="Details")
table = source.to_arrow()

# Convert to pandas
df = table.to_pandas()
print(df.head())

# Or convert to Polars
import polars as pl
df_pl = pl.from_arrow(table)
```

## Using the Source API { #using-the-source-api }

For more control, use a **Source** class directly. Sources give you the
pipeline `|` operator, caching, and streaming:

```python
from crxml import CrystalXMLSource

# Create a source — this does not parse yet
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

## Recap { #recap }

* **rypipe** is a format-agnostic engine. Install an adapter for your format.
* Sources (`CrystalXMLSource`, etc.) give you caching and the pipeline
  operator.
* Convert to pandas with `.to_pandas()` or to Polars with `.to_polars()`.

**Next:** [First Steps](first-steps.md#first-steps) — the Source abstraction
in depth.
