---
title: Parquet files
description: Write rypipe output to Parquet files and datasets
---

# Parquet files { #parquet }

Parquet is Arrow's on-disk companion format. `to_parquet` writes a
pipeline directly, with optional streaming for large inputs.

## Setup { #setup }

```bash
pip install "crxml[all]"
```

The `all` extra includes pyarrow.

## Basic usage { #basic-usage }

```python
from crxml import CrystalXMLSource, CastTypes
from rypipe import to_parquet

to_parquet(
    CrystalXMLSource("report.xml", row_tag="Details")
    | CastTypes({"Amount": float}),
    "sales.parquet",
)
```

Extra keyword arguments are forwarded to
`pyarrow.parquet.ParquetWriter`, for example `compression="zstd"` or
`row_group_size=100_000`.

## Streaming writes { #streaming-writes }

Pass a `memory` budget to parse and write incrementally through a
`ParquetWriter`, so neither parsing nor writing holds the whole dataset:

```python
to_parquet(
    CrystalXMLSource("big-report.xml", row_tag="Details"),
    "sales.parquet",
    memory="64MiB",
    compression="zstd",
)
```

Each parsed batch becomes one or more row groups, bounded by the budget.
Adapters without streaming support materialize the input table first.

## Reading back { #reading-back }

```python
import pyarrow.parquet as pq

table = pq.read_table("sales.parquet")
```

For multi-file datasets, use `pyarrow.dataset` or any Parquet-aware
engine (DuckDB, Polars, Spark); see the other integration pages.

## Why this works { #why-this-works }

rypipe's parser produces Arrow record batches, and Parquet is a columnar
serialization of Arrow-compatible data. Writing is a direct encoding
step; no row-by-row conversion is involved.
