---
title: Apache Iceberg
description: Load rypipe output into Apache Iceberg tables
---

# Apache Iceberg { #iceberg }

PyIceberg accepts Arrow tables for appends and overwrites, so rypipe
output loads into an Iceberg table directly.

## Setup { #setup }

```bash
pip install "crxml[all]" "pyiceberg[sql-sqlite]"
```

The example below uses a local SQLite-backed catalog; swap the catalog
config for REST, Glue, Hive, or Nessie in production.

## Basic usage { #basic-usage }

```python
from crxml import CrystalXMLSource, CastTypes
from pyiceberg.catalog import load_catalog

table = (
    CrystalXMLSource("report.xml", row_tag="Details")
    | CastTypes({"Amount": float})
).to_arrow()

catalog = load_catalog(
    "default",
    **{
        "type": "sql",
        "uri": "sqlite:///catalog.db",
        "warehouse": "file:///tmp/warehouse",
    },
)

catalog.create_namespace_if_not_exists("sales")
tbl = catalog.create_table_if_not_exists("sales.details", schema=table.schema)
tbl.append(table)
```

## Overwrites and upserts { #overwrites }

```python
tbl.overwrite(table)                    # replace all data
tbl.upsert(df, join_cols=["Id"])        # merge on key columns
```

## Reading back { #reading-back }

```python
arrow_table = tbl.scan().to_arrow()
```

Iceberg tables are also readable by DuckDB, Polars, Spark, and Trino, so
the same table can serve SQL queries without extra connectors.

## Why this works { #why-this-works }

PyIceberg's `append`, `overwrite`, and `upsert` take `pyarrow.Table`
input and write Parquet data files through Arrow's columnar layout.
rypipe already produces Arrow, so ingestion is a schema-checked transfer,
not a conversion.
