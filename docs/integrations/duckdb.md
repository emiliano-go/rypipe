---
title: DuckDB
description: Query rypipe output with DuckDB
---

# DuckDB { #duckdb }

DuckDB reads Arrow tables and pandas DataFrames natively via the Arrow C
Data Interface. No adapter or connector needed; pass the table directly.

## Setup { #setup }

```bash
pip install "crxml[all]" duckdb
```

## Basic usage { #basic-usage }

```python
from crxml import CrystalXMLSource, CastTypes, to_pandas
import duckdb

df = to_pandas(
    CrystalXMLSource("report.xml", row_tag="Details")
    | CastTypes({"Amount": float})
)

con = duckdb.connect()
con.execute("CREATE TABLE sales AS SELECT * FROM df")
con.execute("SELECT Department, SUM(Amount) FROM sales GROUP BY Department").fetchdf()
```

!!! tip

    All values come out of the parser as strings. Use `CastTypes` in the
    pipeline to convert numeric columns before passing to DuckDB;
    otherwise `SUM()`, `AVG()`, etc. will fail with a type error.

## With pipelines { #with-pipelines }

```python
from crxml import CrystalXMLSource, CastTypes, FilterRows, to_pandas
import duckdb

df = to_pandas(
    CrystalXMLSource("report.xml", row_tag="Details")
    | CastTypes({"Amount": float})
    | FilterRows(field="Status", op="==", value="Active")
)

con = duckdb.connect()
result = con.execute("SELECT * FROM df").fetchdf()
```

## Direct Arrow (no pandas) { #direct-arrow }

If you only installed `crxml[pyarrow]` without pandas, use the Arrow
table directly:

```python
from crxml import CrystalXMLSource
import duckdb

table = CrystalXMLSource("report.xml", row_tag="Details").to_arrow()

con = duckdb.connect()
result = con.execute("SELECT * FROM table").fetchdf()
```

DuckDB reads PyArrow tables without conversion (zero copy via the Arrow
C Data Interface).

## Why this works { #why-this-works }

rypipe produces Arrow tables. DuckDB's Python client accepts any
Arrow-compatible object (`pyarrow.Table`, `pandas.DataFrame`,
`polars.DataFrame`) as a table reference in SQL. The data never leaves
Arrow's columnar format until DuckDB materializes a result.
