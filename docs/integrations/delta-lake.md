---
title: Delta Lake
description: Load rypipe output into Delta Lake tables
---

# Delta Lake { #delta-lake }

Delta Lake tables are Parquet files plus a transaction log. The
`deltalake` Python package accepts Arrow tables directly, so rypipe
output can be written without Spark.

## Setup { #setup }

```bash
pip install "crxml[all]" deltalake
```

## Basic usage { #basic-usage }

```python
from crxml import CrystalXMLSource, CastTypes
from deltalake import write_deltalake

table = (
    CrystalXMLSource("report.xml", row_tag="Details")
    | CastTypes({"Amount": float})
).to_arrow()

write_deltalake("s3://bucket/sales", table, mode="append")
```

Modes are `"append"`, `"overwrite"`, and `"error"` (default). The table
can live on local disk, S3, GCS, or Azure Blob Storage.

## Creating a partitioned table { #partitioned }

```python
write_deltalake(
    "s3://bucket/sales",
    table,
    mode="overwrite",
    partition_by=["Department"],
)
```

## Reading back { #reading-back }

```python
from deltalake import DeltaTable

dt = DeltaTable("s3://bucket/sales")
df = dt.to_pandas()
```

`DeltaTable.to_pyarrow_table()` and `.to_polars()` are also available,
and the table can be registered with DuckDB or Spark for SQL queries.

## Why this works { #why-this-works }

`write_deltalake` consumes any Arrow-compatible object through the Arrow
C Data Interface. rypipe produces Arrow tables, so the data moves into
the Delta writer as columnar batches with no intermediate format.
