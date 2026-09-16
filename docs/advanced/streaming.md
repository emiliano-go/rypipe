---
title: "Streaming batches"
---

# Streaming batches { #streaming-with-constant-memory }

`rypipe`'s streaming iterator yields `RecordBatch` objects and releases each
batch after the consumer returns when the adapter supplies a streaming
implementation. The configured budget limits the parser's chunk and queue
work; input mappings, decompressed data, and collected output can add memory.
The fallback path materializes the table first.

## Bounded vs streaming { #bounded-vs-streaming }

| Mode | API | Retained output | When to use |
|---|---|---|---|
| `bounded` (collecting) | `BoundedExecutor::run` / `Pipeline::read_path_stream` → `Vec<RecordBatch>` | All returned batches | Build a full table while limiting parsing buffers |
| `streaming` (consuming) | `BoundedExecutor::run_stream` + `BatchConsumer` / `iter_record_batches` | Current batch and queued batches, unless the consumer keeps more | Write batches directly with `ParquetWriter` |

`MemoryBudget` controls batch sizing. The bounded executor reuses its chunk
buffer and resets the table builder between batches. Input storage, oversized
records, buffer capacity, worker queues, and Python allocations mean the budget
is not a hard limit on process RSS or individual Arrow batch allocation.

## Rust API { #rust-api }

```rust
use rypipe_core::{BatchConsumer, BoundedExecutor, MemoryBudget, StreamingBatchIterator};
use arrow::record_batch::RecordBatch;

struct ParquetConsumer { writer: arrow::ipc::FileWriter<File> }
impl BatchConsumer for ParquetConsumer {
    fn consume(&mut self, batch: RecordBatch) -> rypipe_core::Result<()> {
        self.writer.write(&batch).map_err(|e| rypipe_core::Error::Arrow(Box::new(e)))?;
        Ok(())
    }
}

let budget = MemoryBudget::new(64 * 1024);
let splitter = CrystalXmlSplitter::with_row_tag("Details");
let parser = CrystalXmlDecoder::with_row_tag("Details");
let plan = ExecutionPlan::new();
let executor = BoundedExecutor::new(budget);
let mut consumer = ParquetConsumer { writer };
executor.run_stream(path, &splitter, parser, plan, false, &mut consumer)?;

// Or pull-based:
let iter = StreamingBatchIterator::new(path.to_path_buf(), splitter, parser, plan, budget, false);
for batch in iter {
    let batch = batch?;
    // handle batch
}
```

`Pipeline` convenience: `pipeline.read_bytes_stream_consumer(&bytes, budget, &mut consumer)` and `pipeline.read_path_stream_consumer(path, budget, false, &mut consumer)` (`crates/rypipe-core/src/pipeline.rs`).

## Python API { #python-api }

### User-facing (sink-level streaming) { #user-facing }

Users compose `iter_record_batches` with the usual Arrow ecosystem writers; peak stays at `memory + one batch`:

```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from crxml import CrystalXMLSource

src = CrystalXMLSource("50GB.xml", row_tag="Details")

# Streaming DataFrame
df = pd.concat(b.to_pandas() for b in src.iter_record_batches(memory="64MB"))

# Streaming Parquet
batches = src.iter_record_batches(memory="64MB")
first = next(batches)
with pq.ParquetWriter("output.parquet", first.schema) as writer:
    writer.write_batch(first)
    for batch in batches:
        writer.write_batch(batch)

# Streaming Polars
table = pa.Table.from_batches(list(src.iter_record_batches(memory="64MB")))
df = pl.from_arrow(table)

# Parallel streaming (higher throughput)
df = pd.concat(b.to_pandas() for b in src.iter_record_batches(memory="64MB", threads=16))

# Pipeline streaming
from crxml import DropFields, FilterRows
pipe = src | DropFields(["Field22"]) | FilterRows(field="Level", op="==", value="3")
df = pd.concat(b.to_pandas() for b in pipe.iter_record_batches(memory="256MB"))
```

Adapter packages may expose extra sink options, but the framework's `memory=`
sink path is incremental only when the adapter supplies streaming support. Use
`iter_record_batches()` when you need direct control. A DataFrame, list, or
Arrow table returned to the caller still occupies memory for its full result.

Unit note: Both `crxml` and the framework accept `B` / `KB` / `MB` / `GB` / `TB`
and `KiB` / `MiB` / `GiB` / `TiB` (1024-based, case-insensitive, no space
before the unit). The `iB` and non-`iB` forms are equivalent. See
[Memory and chunking](memory-and-chunking.md).

### Advanced (batch-level control) { #advanced }

```python
import pyarrow.parquet as pq
from crxml import CrystalXMLSource

src = CrystalXMLSource("50GB.xml", row_tag="Details")

writer = pq.ParquetWriter("out.parquet", schema)
for batch in src.iter_record_batches(memory="64KB"):
    writer.write_batch(batch)
writer.close()

# Pipeline
pipe = src | DropFields(["Field22"]) | FilterRows(field="Level", op="==", value="3")
for batch in pipe.iter_record_batches(memory="256MB"):
    writer.write_batch(batch)
```

`batch_size` is a Python-only convenience parameter (the Rust executor always
derives `rows_per_batch` from the budget in `bounded.rs`). `crxml`'s
`iter_record_batches` deprecates `batch_size` (it warns and ignores it) and
always derives the batch size from the budget. Set `memory` to control batch
size: smaller budgets produce smaller batches.

## When streaming falls back { #when-streaming-falls-back }

`Pipeline.iter_record_batches` checks `plan_split` (`rypipe/fusion.py`); if `remaining` non-fusable stages exist, it falls back to `iter_arrow_batches` (materialized). The same happens for `Source.iter_record_batches` when `_iter_record_batches_stream` is not implemented: it yields `to_arrow().to_batches()`. For bounded parser memory, keep stages fusable (`RenameFields`, `DropFields`, `CastTypes`, and keyword-form or expression-predicate `FilterRows`).

## Testing { #testing }

Parse a large file (for example `crxml`'s `bench_data/synthetic_1gb.xml`) with
`memory="64KB"` and compare row counts with the columnar path. Measure RSS
with a platform-appropriate profiler and record input mapping, adapter, and
consumer behavior; collected outputs and fallback materialization are outside
the parser budget. `crxml`'s `benchmarks/bench_extended.py` includes
bounded-memory configurations (`bounded64` / `bounded256`) for comparison.

## See also { #see-also }

* `crates/rypipe-core/src/consumer.rs` `BatchConsumer`
* `crates/rypipe-core/src/streaming.rs` `StreamingBatchIterator` (`sync_channel(1)` backpressure, `allow_threads` in `rypipe-python`)
* `crates/rypipe-core/src/bounded.rs` buffer reuse in `run_stream`
