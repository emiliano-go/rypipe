# Streaming { #streaming }

!!! note

    Streaming options (`memory`, `batch_size`) are available in most adapters
    but may have different defaults. Check your adapter's docs.

When processing files larger than available memory, use streaming to process
data in bounded chunks.

## The problem { #the-problem }

By default, `.to_arrow()` parses the entire file into memory at once.
This works for files up to several GB on a machine with enough RAM,
but fails for larger files:

```python
from crxml import CrystalXMLSource

# This loads the entire file into memory
source = CrystalXMLSource("huge_report.xml", row_tag="Details")
table = source.to_arrow()  # may OOM
```

## Streaming with iter_record_batches { #streaming-with-iter-record-batches }

The `iter_record_batches()` method yields `pyarrow.RecordBatch` objects one
at a time, processing only a bounded amount of data at once:

```python
from crxml import CrystalXMLSource

src = CrystalXMLSource("huge_report.xml", row_tag="Details")

# Process in chunks of ~64 MiB
for batch in src.iter_record_batches(memory="64MiB"):
    # Each batch is a pyarrow.RecordBatch
    print(f"Processing {batch.num_rows} rows")
    process(batch)
```

### How it works { #how-it-works }

1. **rypipe** reads a chunk of the file into memory (bounded by `memory`).
2. Parses the chunk into a `RecordBatch`.
3. Yields the batch to your code.
4. Drops the batch from memory after your code returns.
5. Repeats for the next chunk.

Peak memory is `memory` + one batch + export buffer. A 10 GB file with
`memory="256MiB"` uses at most ~300 MB of parsing memory at any time.

### Memory parameter { #memory-parameter}

The `memory` parameter accepts a string or integer:

```python
# String formats
for batch in src.iter_record_batches(memory="64MiB"):
    ...

for batch in src.iter_record_batches(memory="256MB"):
    ...

# Integer (bytes)
for batch in src.iter_record_batches(memory=67_108_864):  # 64 MiB
    ...
```

Supported units: `B`, `KB`, `MB`, `GB`, `TB`, `KiB`, `MiB`, `GiB`, `TiB`.

### Batch size { #batch-size}

Control the number of rows per batch with `batch_size`:

```python
# Smaller batches = lower memory, more Python overhead
for batch in src.iter_record_batches(memory="64MiB", batch_size=1000):
    process(batch)

# Larger batches = higher memory, less overhead
for batch in src.iter_record_batches(memory="256MiB", batch_size=100_000):
    process(batch)
```

When `batch_size` is `None` (the default), **rypipe** sizes batches based
on the `memory` budget and estimated row size.

## Streaming with pipelines { #streaming-with-pipelines}

Pipelines also support streaming:

```python
from crxml import CrystalXMLSource
from crxml import CastTypes, FilterRows

src = CrystalXMLSource("huge_report.xml", row_tag="Details")
pipeline = (
    src
    | CastTypes({"amount": float})
    | FilterRows(field="status", op="==", value="active")
)

for batch in pipeline.iter_record_batches(memory="64MiB"):
    process(batch)
```

!!! note

    When all stages are fusable, **rypipe** pushes the entire pipeline into the
    streaming parse loop. Non-fusable stages run after each batch is parsed.


## Writing to Parquet { #writing-to-parquet}

A common pattern is streaming a large file into a Parquet writer:

```python
import pyarrow.parquet as pq
from crxml import CrystalXMLSource
from crxml import CastTypes

src = CrystalXMLSource("huge_report.xml", row_tag="Details")
pipeline = src | CastTypes({"amount": float})

# Write in streaming mode
writer = pq.ParquetWriter("output.parquet", schema=None)

for batch in pipeline.iter_record_batches(memory="64MiB"):
    if writer.schema is None:
        # First batch: initialize the writer with the schema
        writer = pq.ParquetWriter(
            "output.parquet",
            batch.schema,
        )
    writer.write_batch(batch)

writer.close()
```

## Performance { #performance}

Streaming has slightly lower throughput than full-table parsing because
batches are processed one at a time rather than in parallel. Typical
numbers for the [`crxml`](../crxml-adapter.md) adapter:

| Mode | Throughput | Peak memory |
|------|-----------|-------------|
| Parallel (default) | ~4 GB/s | File size |
| Single-thread | ~1 GB/s | File size |
| Streaming (64 MiB) | ~500 MB/s | ~64 MiB |

!!! tip

    Use streaming when your file is larger than ~50% of available RAM. For
    smaller files, the default parallel mode is faster.


## Adding streaming to your adapter { #adding-streaming-to-your-adapter }

To support bounded-memory streaming, add `iter_record_batches` to your
adapter class. This enables `rypipe.iter_record_batches("file.log",
format="log")`:

### `rypipe_log/rypipe_adapter.py` { #adapter-streaming}

```python
class LogAdapter:
    """rypipe-compatible adapter for newline-delimited key=value logs."""

    def read(self, path: str, **kwargs: Any) -> Any:
        """Parse ``path`` and return a ``pyarrow.Table``."""
        return LogSource(path, **kwargs).to_arrow()

    def iter_record_batches(
        self, path: str, memory: str | int = "64MiB",
        batch_size: int | None = None, **kwargs: Any,
    ):
        """Yield ``pyarrow.RecordBatch`` objects with constant memory."""
        yield from LogSource(path, **kwargs).iter_record_batches(
            memory=memory, batch_size=batch_size
        )
```

This delegates to `LogSource.iter_record_batches()`, which inherits from
`rypipe.Source` and handles the bounded-memory streaming automatically.

## Recap { #recap }

* Use `.iter_record_batches(memory="64MiB")` for bounded-memory processing.
* Peak memory is `memory` + one batch + export buffer.
* Control batch size with `batch_size` for tuning memory vs overhead.
* Streaming works with pipelines: fusable stages run in the parse loop.

**Next:** [Configuration](configuration.md#configuration): all available options.
