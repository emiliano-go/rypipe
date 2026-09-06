# Streaming { #streaming }

!!! note

    Streaming options (`memory`, `threads`) are available in most adapters
    but may have different defaults. Check your adapter's docs.

When processing files larger than available memory, use streaming to process
data in bounded chunks. Here is the complete pattern:

```python
from rypipe_log import LogSource

src = LogSource("huge_report.log")
df = src.to_pandas(memory="64MiB")
```

We will explain every line of this in the following sections.

## The problem { #the-problem }

By default, `.to_arrow()` parses the entire file into memory at once.
This works for files up to several GB on a machine with enough RAM,
but fails for larger files:

```python
from rypipe_log import LogSource

# This loads the entire file into memory
source = LogSource("huge_report.log")
table = source.to_arrow()  # may OOM
```

## Streaming to a DataFrame { #streaming-to-a-dataframe }

Pass `memory=` to any sink for bounded-memory processing:

```python
from rypipe_log import LogSource

src = LogSource("huge_report.log")
df = src.to_pandas(memory="64MiB")
```

Peak memory is `memory` + one batch + export buffer. A 10 GB file with
`memory="256MiB"` uses at most ~300 MB of parsing memory at any time.

### How it works { #how-it-works }

When you pass `memory=` to a sink, **rypipe** reads a chunk of the file into
memory (bounded by `memory`), parses it into a `RecordBatch`, converts it to
a DataFrame, and repeats for the next chunk. The batches are concatenated
into a single DataFrame at the end.

### What **rypipe** does automatically { #what-rypipe-does }

With streaming enabled, **rypipe**:

* Bounds memory per chunk; a 50 GB file with `memory="64MiB"` never
  exceeds ~64 MiB of parsing memory.
* Discovers the schema from the first chunk and reuses it for all subsequent
  chunks, avoiding per-chunk discovery overhead.
* Pushes fusable stages (rename, drop, constant filter, typed cast) into the
  Rust parse loop, so they run at full parsing speed with no Python overhead.
* Sizes batches automatically from the `memory` budget and estimated row
  size, or you can set `batch_size` explicitly for finer control.

### Memory parameter { #memory-parameter}

The `memory` parameter accepts a string or integer:

```python
# String formats
df = src.to_pandas(memory="64MiB")
df = src.to_pandas(memory="256MB")

# Integer (bytes)
df = src.to_pandas(memory=67_108_864)  # 64 MiB
```

Supported units: `B`, `KB`, `MB`, `GB`, `TB`, `KiB`, `MiB`, `GiB`, `TiB`.

## Streaming with pipelines { #streaming-with-pipelines}

Pipelines also support streaming:

```python
from rypipe_log import LogSource
from rypipe_log import CastTypes, FilterRows

src = LogSource("huge_report.log")
df = (
    src
    | CastTypes({"amount": float})
    | FilterRows(field="status", op="==", value="active")
).to_pandas(memory="64MiB")
```

!!! note

    When all stages are fusable, **rypipe** pushes the entire pipeline into the
    streaming parse loop. Non-fusable stages run after each batch is parsed.

## Writing to Parquet { #writing-to-parquet}

A common pattern is streaming a large file into a Parquet file:

```python
from rypipe_log import LogSource
from rypipe_log import CastTypes

src = LogSource("huge_report.log")
pipeline = src | CastTypes({"amount": float})

pipeline.to_parquet("output.parquet", memory="64MiB")
```

## Writing to Polars { #writing-to-polars}

```python
from rypipe_log import LogSource

src = LogSource("huge_report.log")
df = src.to_polars(memory="64MiB")
```

## Parallel streaming { #parallel-streaming }

For higher throughput on multi-core machines, pass `threads=` for parallel
streaming. The engine parses chunks in parallel across multiple threads while
keeping memory bounded:

```python
from rypipe_log import LogSource

src = LogSource("huge_report.log")

# Single-threaded streaming
df = src.to_pandas(memory="64MiB")

# Parallel streaming (faster on multi-core machines)
df = src.to_pandas(memory="64MiB", threads=16)
```

Parallel streaming keeps the same memory bound as single-threaded streaming
but achieves throughput closer to full-RAM parallel parsing.

## Performance { #performance }

Streaming has slightly lower throughput than full-table parsing when
single-threaded. Parallel streaming closes the gap and can exceed
full-RAM parallel by keeping the pipeline feed saturated. Typical
numbers for the [`crxml`](../crxml-adapter.md) adapter:

| Mode | Throughput | Peak memory |
|------|-----------|-------------|
| Parallel (default) | ~4 GB/s | File size |
| Single-thread | ~1 GB/s | File size |
| Streaming (64 MiB, single-thread) | ~700 MB/s | ~64 MiB |
| Parallel streaming (64 MiB, 16 threads) | ~4.5 GB/s | ~64 MiB |

!!! tip

    Use streaming when your file is larger than ~50% of available RAM. For
    smaller files, the default parallel mode is faster.

## Advanced: batch-level control { #advanced-batch-control }

For cases where you need to stop early or process batches individually, use
`iter_record_batches()` directly:

```python
from rypipe_log import LogSource

src = LogSource("huge_report.log")

# Stop after finding a specific record (don't parse the full 50 GB file)
for batch in src.iter_record_batches(memory="64MiB"):
    df = batch.to_pandas()
    matches = df[df["order_id"] == "ORD-12345"]
    if not matches.empty:
        print(f"Found: {matches.iloc[0].to_dict()}")
        break
```

!!! note

    `iter_record_batches()` is the low-level API for advanced use cases.
    For most users, `to_pandas(memory=...)`, `to_parquet(path, memory=...)`,
    and `to_polars(memory=...)` are simpler and sufficient.

## Adding streaming to your adapter { #adding-streaming-to-your-adapter }

### Two levels of streaming support { #two-levels }

Most adapters get streaming for free. The engine provides Rust streaming
iterators (`StreamingBatchIterator` and `ParallelStreamingBatchIterator`)
that handle bounded memory, chunking, and parallelism. Your adapter just
needs to override `iter_record_batches()` to call them.

### Python-side wiring (the common path) { #python-wiring }

Two files wire the Rust streaming entry point to Python.

#### Source: `rypipe_log/source.py` { #source-py }

`LogSource` subclasses `Source` and exposes the Rust `iter_batches` function.
`_read_arrow` handles full-table reads; `iter_record_batches` handles
bounded-memory streaming:

```python
# rypipe_log/source.py
from rypipe import Source
import _rypipe_log


class LogSource(Source):
    """Pipeline-capable source for newline-delimited key=value logs."""

    def _read_arrow(self, plan_overrides=None):
        # Merge construction-time kwargs with fused pipeline overrides
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return _rypipe_log.read(str(self._path), **plan)

    def iter_record_batches(self, memory="64MiB", batch_size=None, **kwargs):
        # Forward plan kwargs (field_mapping, drop_fields, filter, ...) to Rust
        plan = self._build_plan_kwargs()
        return _rypipe_log.iter_batches(
            str(self._path), memory=memory, batch_size=batch_size, **plan
        )
```

#### Adapter: `rypipe_log/rypipe_adapter.py` { #adapter-py }

`LogAdapter` is a thin wrapper. `read()` returns a table for `rypipe.read()`;
`iter_record_batches` delegates to `LogSource` for streaming:

```python
# rypipe_log/rypipe_adapter.py
from .source import LogSource


class LogAdapter:
    """rypipe-compatible adapter for newline-delimited key=value logs."""

    def read(self, path: str, **kwargs):
        return LogSource(path, **kwargs).to_arrow()

    def iter_record_batches(self, path, memory="64MiB", batch_size=None, **kwargs):
        yield from LogSource(path, **kwargs).iter_record_batches(
            memory=memory, batch_size=batch_size
        )
```

!!! note

    The adapter's `read()` returns a `pyarrow.Table`, not a Source.
    Users who want pipelines use the Source directly.

### When you need a custom Rust streaming path { #custom-rust-streaming }

If your format has special chunking requirements (e.g., row boundaries span
chunks), implement `iter_batches` in Rust and expose it via PyO3. See
[Python Adapter Wiring: Streaming](../writing-adapters/python-wiring.md#streaming)
for the full Source implementation.

### What users get { #what-users-get }

With the wiring above:

- `source.to_pandas(memory="64MiB")` works automatically.
- `source.to_parquet(path, memory="64MiB")` works automatically.
- `pipeline.to_pandas(memory="64MiB")` works automatically.
- Fusable stages run in the parse loop (no Python overhead).
- Peak memory is bounded by the `memory` parameter.

## Try it { #try-it }

Run the streaming example:

```bash
python -c "
from rypipe_log import LogSource, CastTypes

# Create a test file with multiple rows
with open('huge_report.log', 'w') as f:
    for i in range(10000):
        f.write(f'name=User{i},amount={i*1.5},status=active\n')

# Stream with bounded memory
src = LogSource('huge_report.log')
df = src.to_pandas(memory='64MiB')
print(f'Processed {len(df)} rows')
print(df.head())
"
```

Expected output:

```
Processed 10000 rows
       name  amount  status
0    User0     0.0  active
1    User1     1.5  active
2    User2     3.0  active
3    User3     4.5  active
4    User4     6.0  active
```

## Recap { #recap }

* Pass `memory=` to any sink (`to_pandas`, `to_polars`, `to_parquet`,
  `collect`) for bounded-memory streaming.
* Peak memory is `memory` + one batch + export buffer.
* Pass `threads=` for parallel streaming on multi-core machines.
* Streaming works with pipelines: fusable stages run in the parse loop.
* Use `iter_record_batches()` for advanced batch-level control.

**Next:** [Configuration](configuration.md#configuration): all available options.
