# Streaming with constant memory

`rypipe` can stream arbitrarily large files with **constant memory**: even a 50 GB file on a 2 GB Raspberry Pi: by yielding `RecordBatch` objects one at a time and dropping each after the consumer returns.

## Bounded vs streaming

| Mode | API | Peak memory | When to use |
|---|---|---|---|
| `bounded` (collecting) | `BoundedExecutor::run` / `Pipeline::read_path_stream` → `Vec<RecordBatch>` | `budget + sum(batches)`: still grows with file size if you collect | `source.to_arrow()` with `memory="256MB"` for a single table |
| `streaming` (consuming) | `BoundedExecutor::run_stream` + `BatchConsumer` / `iter_record_batches` | `budget + one batch`: constant | `for batch in rypipe.iter_record_batches(..., memory="64KB"):` + `ParquetWriter` |

The engine already respects a `MemoryBudget` (`crates/rypipe-core/src/bounded.rs:19`) and `StreamingBatchIterator` (`crates/rypipe-core/src/streaming.rs:30`) reuses a single `Vec<u8>` chunk buffer (`chunk_buf.resize(chunk_len)`) and `TableBuilder::reset()` (`crates/rypipe-core/src/engine/table_builder.rs:316`) to keep RSS at `budget + batch`.

## Memory guarantee

* **Rust-only:** `budget + batch + export buffer`. With `batch_size=1` and `memory="64KB"` and small rows (~1 KB for `crxml` `Details`), peak is a few tens of KB plus the `mmap` mapping (dropped after `plan_chunks` `bounded.rs:52`). This is the **64 KB** target in the spec.
* **Python:** `pyarrow.RecordBatch` + interpreter overhead make true 64 KB impossible, but `iter_record_batches` is still bounded: `benchmarks/bench_extended.py` `bounded 64MB` 494 MB/s vs `bounded 64KB` 607 MB/s on 1 GB, and `50 GB` extrapolates to `~84s` at `64KB` vs `18s` `par32` full-RAM.

## Rust API

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

## Python API

```python
import pyarrow.parquet as pq, rypipe

# High-level: rypipe handles adapter lookup + streaming
writer = pq.ParquetWriter("out.parquet", schema)
for batch in rypipe.iter_record_batches("50GB.xml", format="crxml", memory="64KB", batch_size=1, row_tag="Details"):
    writer.write_batch(batch)
writer.close()

# Direct via crxml
from crxml import CrystalXMLSource
src = CrystalXMLSource("50GB.xml", row_tag="Details")
for batch in src.iter_record_batches(memory="64KB"):
    writer.write_batch(batch)

# Pipeline
from crxml import DropFields, FilterRows
pipe = CrystalXMLSource("50GB.xml", row_tag="Details") | DropFields(["Field22"]) | FilterRows(field="Level", op="==", value="3")
for batch in pipe.iter_record_batches(memory="256MB"):
    writer.write_batch(batch)
```

`batch_size` overrides the budget-derived `rows_per_batch = budget / estimate_bytes_per_row` (`crates/rypipe-core/src/decoder.rs`). Default derives from `memory`; pass `batch_size=1` for minimal per-batch memory.

## When streaming falls back

`Pipeline.iter_record_batches` checks `plan_split` `rypipe/fusion.py:14`; if `remaining` non-fusable stages exist, it falls back to `iter_arrow_batches` (materialized). The same happens for `Source.iter_record_batches` when `_iter_record_batches_stream` is not implemented: it yields `to_arrow().to_batches()`. For constant memory, keep stages fusable (`RenameFields`, `DropFields`, `CastTypes`, `FilterRows` constant).

## Testing

Parse 1 GB `test_1gb.xml` `memory="64KB"` `batch_size=58` vs columnar (`bench_extended.py`): both 926,746 rows, `1.69s 607 MB/s` vs `1.74s 588 MB/s` within 10%. Assert `RSS < budget*2` via `resource.getrusage`.

## See also

* `crates/rypipe-core/src/consumer.rs` `BatchConsumer`
* `crates/rypipe-core/src/streaming.rs` `StreamingBatchIterator` (`sync_channel(1)` backpressure, `allow_threads` in `rypipe-python`)
* `crates/rypipe-core/src/bounded.rs:96` buffer reuse
