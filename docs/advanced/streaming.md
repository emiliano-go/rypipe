# Streaming with constant memory { #streaming-with-constant-memory }

`rypipe` can stream arbitrarily large files with **constant memory**, even a 50 GB file on a 2 GB Raspberry Pi, by yielding `RecordBatch` objects one at a time and dropping each after the consumer returns.

## Bounded vs streaming { #bounded-vs-streaming }

| Mode | API | Peak memory | When to use |
|---|---|---|---|
| `bounded` (collecting) | `BoundedExecutor::run` / `Pipeline::read_path_stream` → `Vec<RecordBatch>` | `budget + sum(batches)`: still grows with file size if you collect | `source.to_arrow()` with `memory="256MB"` for a single table |
| `streaming` (consuming) | `BoundedExecutor::run_stream` + `BatchConsumer` / `iter_record_batches` | `budget + one batch`: constant | `for batch in src.iter_record_batches(memory="64KB"):` + `ParquetWriter` |

The engine already respects a `MemoryBudget` (`crates/rypipe-core/src/bounded.rs`) and `StreamingBatchIterator` (`crates/rypipe-core/src/streaming.rs`) reuses a single `Vec<u8>` chunk buffer (`chunk_buf.resize(chunk_len)` in `bounded.rs`) and `TableBuilder::reset()` (`crates/rypipe-core/src/engine/table_builder.rs`) to keep RSS at `budget + batch`.

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

Note that `crxml`'s `CrystalXMLSource` sink methods (`to_pandas` / `to_polars` / `to_parquet`) take **no** `memory=` argument; `iter_record_batches` is the bounded-memory entry point. The framework also ships generic sink helpers (`rypipe.to_pandas(src, memory=...)` and friends) that stream through `iter_record_batches` internally; those exist for adapter authors whose sources do not override the sinks, and end users do not need them.

Unit note: `crxml` accepts `B` / `KB` / `MB` / `GB` / `TB` (1024-based, case-insensitive, no space before the unit) in memory strings, while the framework's own memory parser additionally accepts the `KiB` / `MiB` / `GiB` / `TiB` forms. `memory="64MiB"` raises `invalid memory` in crxml; use `"64MB"`. See [Memory and chunking](memory-and-chunking.md).

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

`batch_size` overrides the budget-derived `rows_per_batch = budget / estimate_bytes_per_row` (`crates/rypipe-core/src/decoder.rs`). Default derives from `memory`; pass `batch_size=1` for minimal per-batch memory. `crxml`'s `iter_record_batches` deprecates `batch_size` (it warns and ignores it) and always derives the batch size from the budget.

## When streaming falls back { #when-streaming-falls-back }

`Pipeline.iter_record_batches` checks `plan_split` (`rypipe/fusion.py`); if `remaining` non-fusable stages exist, it falls back to `iter_arrow_batches` (materialized). The same happens for `Source.iter_record_batches` when `_iter_record_batches_stream` is not implemented: it yields `to_arrow().to_batches()`. For constant memory, keep stages fusable (`RenameFields`, `DropFields`, `CastTypes`, `FilterRows` with the keyword form or a compilable lambda).

## Testing { #testing }

Parse a large file (for example `crxml`'s `bench_data/synthetic_1gb.xml`) with `memory="64KB"` against the full columnar path and assert the same row count, then assert `RSS < budget*2` via `resource.getrusage`. `crxml`'s `benchmarks/bench_extended.py` includes bounded-memory configurations (`bounded64` / `bounded256`) you can diff against the columnar baseline.

## See also { #see-also }

* `crates/rypipe-core/src/consumer.rs` `BatchConsumer`
* `crates/rypipe-core/src/streaming.rs` `StreamingBatchIterator` (`sync_channel(1)` backpressure, `allow_threads` in `rypipe-python`)
* `crates/rypipe-core/src/bounded.rs` buffer reuse in `run_stream`
