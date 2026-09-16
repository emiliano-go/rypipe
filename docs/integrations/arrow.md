---
title: Apache Arrow ecosystem
description: Use rypipe output anywhere Arrow is accepted
---

# Apache Arrow ecosystem { #arrow-ecosystem }

rypipe's output is an Arrow table. Any library that speaks the Arrow C
Data Interface or the Arrow PyCapsule protocol can consume it directly:
DuckDB, Polars, pandas, DataFusion, ADBC drivers, Delta Lake, Iceberg,
Ray, Dask, and more.

## The handoff pattern { #handoff-pattern }

```python
from crxml import CrystalXMLSource

table = CrystalXMLSource("report.xml", row_tag="Details").to_arrow()
```

From there:

```python
import duckdb
duckdb.sql("SELECT * FROM table").df()          # SQL

import polars as pl
pl.from_arrow(table)                             # Polars

import pandas as pd
table.to_pandas(types_mapper=pd.ArrowDtype)      # pandas

import pyarrow.dataset as ds
ds.dataset(table)                                # Arrow datasets
```

## Streaming batches { #streaming-batches }

When a consumer accepts record batches rather than a whole table, use
`iter_record_batches` to stay within a memory budget:

```python
source = CrystalXMLSource("big-report.xml", row_tag="Details")

for batch in source.iter_record_batches(memory="64MiB"):
    ...  # hand each batch to the consumer
```

ADBC drivers and `pyarrow.parquet.ParquetWriter` both work this way;
see the Direct SQL and Parquet pages for complete examples.

## Interchange with other DataFrames { #interchange }

For DataFrame libraries without direct Arrow support, the dataframe
interchange protocol (`__dataframe__`) is a fallback:

```python
from rypipe import to_polars

df = to_polars(pipeline)
other = some_lib.from_dataframe(df)   # via the interchange protocol
```

Prefer the Arrow route where available; it is zero-copy and better
specified for nested types.

## Why this works { #why-this-works }

Arrow is designed as a common in-memory language for columnar data.
Because rypipe never leaves Arrow, integration with the broader
ecosystem is usually a single function call, and often zero copies.
