# Sinks { #sinks }

A sink is where your data ends up: an Arrow table, a DataFrame, a file, or
a list of dicts. Sources have sink *methods*; pipelines are materialized
with sink *functions*. This page shows one short example of each.

## Which sink do I want? { #which-sink }

| Goal | Call |
|------|------|
| Get a `pyarrow.Table` | `src.to_arrow()` |
| Get a pandas DataFrame | `src.to_pandas()` |
| Get a Polars DataFrame | `src.to_polars()` |
| Write a Parquet file | `src.to_parquet(path)` |
| Get rows from a pipeline | `collect(pipeline)` |
| Get a DataFrame from a pipeline | `to_pandas(pipeline)` |
| Write a pipeline to CSV | `to_csv(pipeline, path)` |
| Free the cached table | `src.clear_cache()` |

Source methods and pipeline functions differ in what they re-run. Source
methods parse the file once and cache the Arrow table, so repeated calls
are cheap; they are the right choice when you do several things with the
same file. Pipeline functions re-run the pipeline on every call, which is
fine for small filtered subsets but wasteful in a loop. Between the
DataFrame sinks, pick the library your downstream code already uses:
`to_arrow()` is the zero-copy baseline, while `to_pandas()` and
`to_polars()` pay a conversion. For files too large to hold in memory,
skip all of these and stream with `iter_record_batches()` (see
[Streaming](streaming.md#streaming)).

All examples assume:

```python
from crxml import CrystalXMLSource, FilterRows

src = CrystalXMLSource("report.xml", row_tag="Details")
pipeline = src | FilterRows(field="Status", op="==", value="Active")
```

## Source methods { #source-methods }

### to_arrow() { #to-arrow }

```python
table = src.to_arrow()
print(table.num_rows, table.num_columns)  # 15 5
```

The table is cached after the first call, so `to_pandas()` and friends
reuse it without re-parsing.

### to_pandas() { #to-pandas }

```python
df = src.to_pandas()
```

Requires `pandas`.

### to_polars() { #to-polars }

```python
df = src.to_polars()
```

Requires `polars`.

### to_parquet() { #to-parquet }

```python
src.to_parquet("output.parquet")
```

Extra keyword arguments are passed to `pyarrow.parquet.write_table`, for
example `compression="zstd"`. See the
[reference](../reference/python-api.md#to-parquet-details) for the common
options.

### clear_cache() { #clear-cache }

```python
src.clear_cache()
```

Drops the cached Arrow table. The next sink call re-parses the file.

A Source keeps its parsed table in memory so repeated sinks
(`to_pandas()`, then `to_parquet()`, ...) do not re-parse. That is great
while you work with one file, but in an ETL flow the table is dead weight
once the file is done. **Good practice: after the final sink for a file,
call `clear_cache()` before moving to the next one.**

```python
import glob
from crxml import CrystalXMLSource

for path in sorted(glob.glob("exports/*.xml")):
    src = CrystalXMLSource(path, row_tag="Details")
    src.to_parquet(path.replace(".xml", ".parquet"))
    src.clear_cache()  # free the table before the next file
```

Without the `clear_cache()` call, each Source in a long-running job holds
its table until it is garbage collected, so peak memory grows with the
number of live Sources. (If your files are large, see
[Streaming](streaming.md#streaming): processing in batches with
`iter_record_batches` never holds the whole table at all.)

## Pipeline functions { #pipeline-functions }

Pipelines are streams of row dicts, so you materialize them with the
functions from `crxml`:

### collect() { #collect }

Runs the pipeline and returns all rows as a `list` of `dict`s:

```python
from crxml import collect

rows = collect(pipeline)
print(len(rows), rows[0]["Name"])  # 12 Alice Johnson
```

### to_pandas() (function) { #to-pandas-function }

```python
from crxml import to_pandas

df = to_pandas(pipeline)
```

### to_csv() { #to-csv-function }

```python
from crxml import to_csv

to_csv(pipeline, "active.csv")
```

!!! tip

    Reading the same pipeline twice re-runs it. If you need the rows more
    than once, `collect()` them into a list first.

!!! tip "Streaming sinks"

    To write Parquet or build a DataFrame without holding the whole file in
    memory, use `iter_record_batches()`. See
    [Streaming](streaming.md#streaming).

## Recap { #recap }

* Source methods: `.to_arrow()`, `.to_pandas()`,
  `.to_polars()`, `.to_parquet(path)`, `.clear_cache()`.
* Pipeline functions: `collect()`, `to_pandas()`, `to_csv()`.
* Source results are cached; pipelines re-run each time you consume them.
* In ETL loops over many files, call `.clear_cache()` after each file's
  final sink to free its table.
* Parameter details live in the
  [Python API reference](../reference/python-api.md#sinks).

**Next:** [Streaming](streaming.md#streaming), processing large files with
bounded memory.
