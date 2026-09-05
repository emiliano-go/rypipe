# Sinks { #sinks }

Sinks materialize pipeline results into tables, DataFrames, or files.
You can use them as methods on a Source or as standalone functions on a
Pipeline.

## Source methods { #source-methods }

Every Source has built-in sink methods:

```python
from rypipe_log import LogSource

src = LogSource("test.log")
```

### to_arrow() { #to-arrow }

Returns a `pyarrow.Table`. This is the default materialization:

```python
table = src.to_arrow()
```

### to_pandas() { #to-pandas }

Returns a pandas DataFrame with PyArrow-backed dtypes by default:

```python
df = src.to_pandas()
```

### to_polars() { #to-polars }

Returns a Polars DataFrame:

```python
df = src.to_polars()
```

### to_parquet() { #to-parquet}

Writes the table to a Parquet file:

```python
src.to_parquet("output.parquet")
```

### clear_cache() { #clear-cache}

Drops the cached Arrow table to free memory:

```python
src.clear_cache()
# Next to_arrow() call will re-parse the file
```

## Pipeline functions { #pipeline-functions }

When working with a Pipeline (the result of `src | stage`), use the
standalone sink functions from the adapter:

```python
from rypipe_log import LogSource, FilterRows, collect

src = LogSource("test.log")
pipeline = src | FilterRows(field="status", op="==", value="active")
```

### collect() { #collect }

Collects all rows into a list of dicts:

```python
from rypipe_log import collect

rows = collect(pipeline)
```

### to_arrow() (function) { #to-arrow-function }

Materializes a pipeline to a `pyarrow.Table`:

```python
from rypipe_log import to_arrow

table = to_arrow(pipeline)
```

### to_pandas() (function) { #to-pandas-function }

Materializes a pipeline to a pandas DataFrame:

```python
from rypipe_log import to_pandas

df = to_pandas(pipeline)
```

### to_polars() (function) { #to-polars-function }

Materializes a pipeline to a Polars DataFrame:

```python
from rypipe_log import to_polars

df = to_polars(pipeline)
```

### to_csv() (function) { #to-csv-function }

Writes pipeline results to a CSV file:

```python
from rypipe_log import to_csv

to_csv(pipeline, "output.csv")
```

### to_parquet() (function) { #to-parquet-function }

Writes pipeline results to a Parquet file:

```python
from rypipe_log import to_parquet

to_parquet(pipeline, "output.parquet")
```

## Which sink should I use? { #which-sink}

| Goal | Method |
|------|--------|
| Get a PyArrow table | `.to_arrow()` or `to_arrow()` |
| Get a pandas DataFrame | `.to_pandas()` or `to_pandas()` |
| Get a Polars DataFrame | `.to_polars()` or `to_polars()` |
| Write to Parquet | `.to_parquet(path)` or `to_parquet(pipeline, path)` |
| Write to CSV | `to_csv(pipeline, path)` |
| Get a list of dicts | `collect(pipeline)` |

!!! tip

    When you have a Source, prefer the Source methods (`.to_pandas()`, etc.)
    over the standalone functions. Source methods reuse the cached table and
    avoid re-parsing.

## Repacking sinks for your adapter { #repacking-sinks-for-your-adapter }

Adapters include their own copies of the sink functions. This makes the
adapter self-contained: users never import from **rypipe**.

### `rypipe_log/sinks.py` { #sinks-py }

```python
from rypipe.sinks import to_pandas as _rypipe_to_pandas
from rypipe.sinks import to_csv as _rypipe_to_csv
from rypipe.sinks import collect as _rypipe_collect
from rypipe.sinks import to_arrow as _rypipe_to_arrow
from rypipe.sinks import to_polars as _rypipe_to_polars
from rypipe.sinks import to_parquet as _rypipe_to_parquet


# Re-export from rypipe with the adapter's namespace
collect = _rypipe_collect
to_pandas = _rypipe_to_pandas
to_arrow = _rypipe_to_arrow
to_polars = _rypipe_to_polars
to_parquet = _rypipe_to_parquet
to_csv = _rypipe_to_csv
```

Or reimplement them from scratch for full control.

## Recap { #recap }

* Source methods: `.to_arrow()`, `.to_pandas()`, `.to_polars()`,
  `.to_parquet()`, `.clear_cache()`.
* Standalone functions: `collect()`, `to_arrow()`, `to_pandas()`,
  `to_polars()`, `to_csv()`, `to_parquet()`.
* Source methods reuse the cached table. Standalone functions re-parse if
  the pipeline hasn't been materialized yet.

**Next:** [Streaming](streaming.md#streaming), processing large files with bounded
memory.
