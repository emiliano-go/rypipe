# Sinks { #sinks }

Sinks materialize pipeline results into tables, DataFrames, or files.
You can use them as methods on a Source or as standalone functions on a
Pipeline.

## Source methods { #source-methods }

Every Source has built-in sink methods:

```python
from crxml import CrystalXMLSource

src = CrystalXMLSource("report.xml", row_tag="Details")
```

### to_arrow() { #to-arrow }

Returns a `pyarrow.Table`. This is the default materialization:

```python
table = src.to_arrow()
print(table.schema)
# name: string
# amount: double
```

### to_pandas() { #to-pandas }

Returns a pandas DataFrame with PyArrow-backed dtypes by default:

```python
df = src.to_pandas()
print(df.dtypes)
# name      string[pyarrow]
# amount    double[pyarrow]
```

You can disable PyArrow backing with `dtype_backend="numpy"`:

```python
df = src.to_pandas(dtype_backend="numpy")
```

### to_dataframe() { #to-dataframe }

Alias for `to_pandas()`:

```python
df = src.to_dataframe()  # same as src.to_pandas()
```

### to_polars() { #to-polars }

Returns a Polars DataFrame:

```python
import polars as pl

df = src.to_polars()
print(df.columns)
# ['name', 'amount']
```

### to_parquet() { #to-parquet}

Writes the table to a Parquet file:

```python
src.to_parquet("output.parquet")

# Pass additional pyarrow.parquet options
src.to_parquet("output.parquet", compression="snappy")
```

### clear_cache() { #clear-cache}

Drops the cached Arrow table to free memory:

```python
src.clear_cache()
# Next to_arrow() call will re-parse the file
```

## Pipeline functions { #pipeline-functions }

When working with a Pipeline (the result of `src | stage`), use the
standalone sink functions from **rypipe**:

```python
import rypipe
from rypipe import collect, to_arrow, to_pandas, to_polars, to_csv, to_parquet

from crxml import CrystalXMLSource, FilterRows

src = CrystalXMLSource("report.xml", row_tag="Details")
pipeline = src | FilterRows(field="status", op="==", value="active")
```

### collect() { #collect }

Collects all rows into a list of dicts:

```python
rows = rypipe.collect(pipeline)
print(rows[0])
# {"name": "Alice", "amount": 150.0, "status": "active"}
```

### to_arrow() (function) { #to-arrow-function }

Materializes a pipeline to a `pyarrow.Table`:

```python
table = rypipe.to_arrow(pipeline)
```

### to_pandas() (function) { #to-pandas-function}

Materializes a pipeline to a pandas DataFrame:

```python
df = rypipe.to_pandas(pipeline)
```

### to_polars() (function) { #to-polars-function}

Materializes a pipeline to a Polars DataFrame:

```python
df = rypipe.to_polars(pipeline)
```

### to_csv() (function) { #to-csv-function}

Writes pipeline results to a CSV file:

```python
rypipe.to_csv(pipeline, "output.csv")

# Custom delimiter and encoding
rypipe.to_csv(pipeline, "output.tsv", delimiter="\t", encoding="utf-8")
```

**Parameters:**

* `pipeline` — iterable of dicts.
* `path` — output file path.
* `encoding` — file encoding (default: `"utf-8"`).
* `delimiter` — column delimiter (default: `","`).
* `fieldnames` — optional list of column names. If omitted, uses the keys
  from the first row.

### to_parquet() (function) { #to-parquet-function}

Writes pipeline results to a Parquet file:

```python
rypipe.to_parquet(pipeline, "output.parquet")
```

## Which sink should I use? { #which-sink}

| Goal | Method |
|------|--------|
| Get a PyArrow table | `.to_arrow()` or `rypipe.to_arrow()` |
| Get a pandas DataFrame | `.to_pandas()` or `rypipe.to_pandas()` |
| Get a Polars DataFrame | `.to_polars()` or `rypipe.to_polars()` |
| Write to Parquet | `.to_parquet(path)` or `rypipe.to_parquet(pipeline, path)` |
| Write to CSV | `rypipe.to_csv(pipeline, path)` |
| Get a list of dicts | `rypipe.collect(pipeline)` |

/// tip

When you have a Source, prefer the Source methods (`.to_pandas()`, etc.)
over the standalone functions. Source methods reuse the cached table and
avoid re-parsing.

///

## Recap { #recap }

* Source methods: `.to_arrow()`, `.to_pandas()`, `.to_polars()`,
  `.to_parquet()`, `.clear_cache()`.
* Standalone functions: `rypipe.collect()`, `rypipe.to_arrow()`,
  `rypipe.to_pandas()`, `rypipe.to_polars()`, `rypipe.to_csv()`,
  `rypipe.to_parquet()`.
* Source methods reuse the cached table. Standalone functions re-parse if
  the pipeline hasn't been materialized yet.

**Next:** [Streaming](streaming.md#streaming) — processing large files with bounded
memory.
