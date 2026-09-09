# Streaming { #streaming }

By default, `to_arrow()` parses the whole file into memory. For files
larger than your RAM, stream instead: **rypipe** reads the file in bounded
chunks and yields one Arrow `RecordBatch` at a time, so peak memory stays
roughly constant no matter how big the file is.

The entry point is `iter_record_batches()`:

```python
from crxml import CrystalXMLSource

src = CrystalXMLSource("report.xml", row_tag="Details")

for batch in src.iter_record_batches(memory="64MB"):
    print(batch.num_rows)
```

The examples below run on `report.xml` unchanged; the patterns are
identical for a 50 GB file.

## The memory parameter { #memory-parameter }

`memory=` sets the budget per chunk. It accepts a string or an integer
number of bytes:

```python
src.iter_record_batches(memory="64MB")
src.iter_record_batches(memory="512KB")
src.iter_record_batches(memory=67_108_864)  # 64 MB in bytes
```

Supported units: `B`, `KB`, `MB`, `GB`, `TB` (1024-based, case-insensitive,
no space between the number and the unit). The `KiB`/`MiB` binary forms are
not accepted; `"64MiB"` raises `invalid memory`. Peak memory is
approximately the budget plus one batch and the export buffer.

!!! note

    Batch sizes are derived automatically from the memory budget and the
    estimated row size. You do not need to tune them.

## Streaming to a DataFrame { #streaming-to-a-dataframe }

Concatenate the batches into one pandas DataFrame:

```python
import pandas as pd
from crxml import CrystalXMLSource

src = CrystalXMLSource("report.xml", row_tag="Details")

df = pd.concat(b.to_pandas() for b in src.iter_record_batches(memory="64MB"))
print(df.shape)  # (15, 5)
```

The whole DataFrame still ends up in memory, of course. The win is that the
*parser* never holds the whole file at once, and you can process or write
out each batch as it arrives (see
[Writing Parquet](#writing-to-parquet) and
[Batch-level control](#advanced-batch-control)).

## Streaming with pipelines { #streaming-with-pipelines }

Pipelines stream too. Fusable stages are pushed into the streaming parse
loop, so they run at full parsing speed:

```python
from crxml import CrystalXMLSource, CastTypes, FilterRows

src = CrystalXMLSource("report.xml", row_tag="Details")

pipeline = (
    src
    | CastTypes({"Amount": float})
    | FilterRows(field="Status", op="==", value="Active")
)

total = sum(b.num_rows for b in pipeline.iter_record_batches(memory="64MB"))
print(total)  # 12
```

## Writing Parquet with constant memory { #writing-to-parquet }

The classic large-file pattern: stream batches straight into a Parquet
file. Only one batch is in memory at a time:

```python
import pyarrow.parquet as pq
from crxml import CrystalXMLSource

src = CrystalXMLSource("report.xml", row_tag="Details")

batches = src.iter_record_batches(memory="64MB")
first = next(batches)

with pq.ParquetWriter("output.parquet", first.schema) as writer:
    writer.write_batch(first)
    for batch in batches:
        writer.write_batch(batch)
```

## Streaming to Polars { #writing-to-polars }

```python
import pyarrow as pa
import polars as pl
from crxml import CrystalXMLSource

src = CrystalXMLSource("report.xml", row_tag="Details")

table = pa.Table.from_batches(list(src.iter_record_batches(memory="64MB")))
df = pl.from_arrow(table)
```

## Parallel streaming { #parallel-streaming }

Pass `threads=` to parse chunks in parallel while keeping the same memory
bound. This gives throughput close to full-RAM parallel parsing:

```python
total = 0
for batch in src.iter_record_batches(memory="64MB", threads=16):
    total += batch.num_rows
```

!!! tip

    Use streaming when the file is larger than about half of your available
    RAM. For smaller files the default parallel mode is faster. See
    [Performance](../performance.md) for measured numbers.

## Batch-level control { #advanced-batch-control }

Because you get one batch at a time, you can stop early. Here we search for
one record without parsing the rest of the file:

```python
from crxml import CrystalXMLSource

src = CrystalXMLSource("report.xml", row_tag="Details")

for batch in src.iter_record_batches(memory="64MB"):
    df = batch.to_pandas()
    matches = df[df["Name"] == "Ivy Anderson"]
    if not matches.empty:
        print(matches.iloc[0].to_dict())
        break
```

```console
{'Name': 'Ivy Anderson', 'Department': 'Sales', 'Amount': '6300.75', 'Status': 'Inactive', 'Date': '2026-01-13'}
```

!!! note

    You can also pass `memory=` to the `CrystalXMLSource` constructor. Then
    even `to_arrow()` stays within the budget. See
    [Configuration](configuration.md#source-constructor-options).

## Recap { #recap }

* `iter_record_batches(memory="64MB")` yields Arrow batches within a fixed
  memory budget, from Sources and from pipelines.
* Build a DataFrame with `pd.concat`, write Parquet incrementally with
  `pq.ParquetWriter`, or process batches one at a time and stop early.
* `threads=` enables parallel streaming at the same memory bound.
* Units are `B`/`KB`/`MB`/`GB`/`TB` (case-insensitive, no spaces), or an
  integer number of bytes.
* Not streaming? Then call `.clear_cache()` after each file's final sink so
  ETL jobs over many files do not accumulate cached tables (see
  [Sinks](sinks.md#clear-cache)).

**Next:** [Configuration](configuration.md#configuration), all available
options.
