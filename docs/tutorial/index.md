# Tutorial { #tutorial }

This tutorial teaches you how to use **rypipe** to read files into Arrow tables
and DataFrames. You do not need to write Rust or build anything; just install
**rypipe** and an adapter package, and start reading data.

!!! tip

    If you are in a hurry, jump to [First Steps](first-steps.md#first-steps) for a 5-minute
    quickstart. If you want to write your own format adapter, see the
    [Writing Adapters](../writing-adapters/index.md) guide instead.


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
| Your custom format | Write your own | Any |

## Installation { #installation }

```bash
pip install crxml
```

This installs **rypipe** (the engine) and **crxml** (the Crystal Reports XML
adapter). For other formats, install the corresponding adapter.

!!! note

    **rypipe** requires Python 3.10 or later. The engine is written in Rust and
    ships as a compiled extension: no Rust toolchain needed for installation.


## Your first read { #your-first-read }

```python
from crxml import CrystalXMLSource

# Read a Crystal Reports XML file into a PyArrow table
source = CrystalXMLSource("report.xml", row_tag="Details")
table = source.to_arrow()

print(table.schema)
# name: string
# amount: double
# status: string

print(table.num_rows)
# 1247
```

That's it. The adapter parsed the file in parallel and returned a
`pyarrow.Table`.

### What **rypipe** does automatically { #what-rypipe-does }

With just that one call, **rypipe**:

* Infers the adapter from the file extension (`.xml` → `crxml`).
* Splits the file into chunks for parallel parsing.
* Discovers the schema from the data.
* Builds Arrow column arrays with near-zero copy.
* Returns a `pyarrow.Table` you can use directly.

## Getting a DataFrame { #getting-a-dataframe }

The table is already a `pyarrow.Table`, but you can convert it to pandas or
Polars with one call:

```python
from crxml import CrystalXMLSource

# Read into a PyArrow table
source = CrystalXMLSource("report.xml", row_tag="Details")
table = source.to_arrow()

# Convert to pandas
df = table.to_pandas()
print(df.head())
#        name  amount   status
# 0     Alice   150.0   active
# 1       Bob    75.0  inactive
# ...

# Or convert to Polars
import polars as pl
df_pl = pl.from_arrow(table)
```

## Using the Source API { #using-the-source-api }

For more control, use a **Source** class directly. Sources give you the
pipeline `|` operator, caching, and streaming:

```python
from crxml import CrystalXMLSource

# Create a source: this does not parse yet
src = CrystalXMLSource("report.xml", row_tag="Details")

# Parse and get a table (cached on first call)
table = src.to_arrow()

# Convert to pandas
df = src.to_pandas()

# Convert to Polars
df_pl = src.to_polars()
```

The Source parses the file once and caches the result. Subsequent calls to
`to_arrow()`, `to_pandas()`, etc. reuse the cached table.

## Recap { #recap }

* **rypipe** is a format-agnostic engine. Install an adapter for your format.
* `rypipe.read("file.ext")` reads a file and returns a `pyarrow.Table`.
* Source classes (`CrystalXMLSource`, etc.) give you caching and the pipeline
  operator.
* Convert to pandas with `.to_pandas()` or to Polars with `.to_polars()`.

**Next:** [First Steps](first-steps.md#first-steps): learn about `rypipe.read()` in depth.
