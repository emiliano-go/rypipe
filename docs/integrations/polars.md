---
title: Polars
description: Load rypipe output into Polars DataFrames
---

# Polars { #polars }

Polars reads Arrow tables natively through `pl.from_arrow`, so rypipe
output lands in a Polars DataFrame without an intermediate pandas step.

## Setup { #setup }

```bash
pip install "crxml[all]"
```

The `all` extra includes polars, pandas, and pyarrow.

## Basic usage { #basic-usage }

Use the built-in `to_polars` sink:

```python
from crxml import CrystalXMLSource, CastTypes
from rypipe import to_polars

df = to_polars(
    CrystalXMLSource("report.xml", row_tag="Details")
    | CastTypes({"Amount": float})
)

df.group_by("Department").agg(pl.col("Amount").sum())
```

`to_polars` accepts a Source, a Pipeline, or any iterable of dicts.

## Streaming construction { #streaming-construction }

For large inputs, pass a `memory` budget. Batches are produced via
`iter_record_batches` and concatenated incrementally, so the parser never
holds the whole file in memory:

```python
from crxml import CrystalXMLSource
from rypipe import to_polars

df = to_polars(
    CrystalXMLSource("big-report.xml", row_tag="Details"),
    memory="64MiB",
)
```

All output still resides in memory; the budget bounds the parsing side
only. Adapters without streaming support materialize the input table
first.

## Direct Arrow { #direct-arrow }

If you already have an Arrow table, pass it straight to Polars:

```python
from crxml import CrystalXMLSource
import polars as pl

table = CrystalXMLSource("report.xml", row_tag="Details").to_arrow()
df = pl.from_arrow(table)
```

Polars reads Arrow memory directly (zero copy for most types via the
Arrow C Data Interface).

## Why this works { #why-this-works }

Polars is built on Arrow. `pl.from_arrow` accepts `pyarrow.Table`,
`pyarrow.RecordBatch`, and any object implementing the Arrow PyCapsule
interface, so rypipe's columnar output moves into Polars without
row-by-row conversion.
