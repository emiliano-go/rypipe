# Anti-patterns { #anti-patterns }

These patterns are common, legal, and expensive. Avoid them when throughput or memory matters.

## Iterating a table source row-by-row { #iterating-a-table-source-row-by-row }

```python
for row in pipeline:
    ...
```

This works, but it reconstructs Python dicts from the Arrow table. If the source is table-shaped and you need row access, consider using `to_arrow()` and `pyarrow` vectorized operations instead.

## Chaining Python callables { #chaining-python-callables }

```python
result = (
    source
    | (lambda rows: transform(rows))
    | (lambda rows: another_transform(rows))
).to_pandas()
```

Each callable stage is opaque to fusion: it runs in Python per row (or forces the pipeline onto the dict-stream path) and cannot be pushed into the Rust parse loop. Prefer fused stages (`RenameFields`, `DropFields`, `CastTypes`, keyword-form or `col()`-expression `FilterRows`) or move the logic into Rust.

## Repeated sinks on a pipeline { #repeated-to_pandas-to_arrow }

```python
t1 = pipeline.to_pandas()
t2 = pipeline.to_pandas()
```

Pipeline sinks cache their materialized result. Materialize once and reuse the result when sharing it across code paths:

```python
df = pipeline.to_pandas()
t1 = df
t2 = df
```

The Pipeline cache is separate from the Source cache, so the original source
table remains available for other pipelines.

## Ignoring `plan_overrides` { #ignoring-plan_overrides }

```python
class MySource(Source):
    def _read_arrow(self, *, plan_overrides=None, **kwargs):
        return my_rust_read(self.path, **kwargs)  # plan_overrides lost!
```

If an adapter ignores `plan_overrides`, fused stage transformations are lost.
Readers that reject the unexpected keywords raise `TypeError`. Always forward
`plan_overrides` to the Rust reader.

## Wrong engine choice { #wrong-engine-choice }

```python
from crxml import CrystalXMLSource

# Tiny file, over-parallelized: coordination costs more than the parse
table = CrystalXMLSource("tiny.xml", row_tag="Row", threads=64).to_arrow()
```

For small files, columnar mode is usually fastest. For huge files, stream mode keeps memory flat. Parallel mode only wins for large, CPU-bound, cached files. When in doubt, let `resolve_engine` pick.

## Misusing `auto_dict` { #using-auto_dict-in-parallel-mode-for-throughput }

```python
source = MySource("data.log", auto_dict=True)  # on a high-cardinality file
```

`auto_dict` tracks distinct-value counts for every string column. On high-cardinality data that tracking is pure overhead and nothing upgrades. Use explicit `dictionary_columns` for the columns you know are low-cardinality, or tighten `auto_dict_threshold` / `auto_dict_max_size`. Dictionaries no longer force the parallel merge path, so the remaining cost is the tracking itself.

## Not declaring types for numeric filters { #not-declaring-types-for-numeric-filters }

```python
FilterRows(field="amount", op=">", value="100.0")
```

Without `field_types={"amount": "float64"}`, the engine stores `amount` as a string and the compare runs with string ordering (`"9" > "100"` is true for strings). Declare the type so the filter compares numbers.

## Adapter anti-patterns { #adapter-anti-patterns }

### Overriding `apply()` on fusable stages { #overriding-apply-on-fusable-stages }

```python
class LoggingFilter(FilterRows):
    def apply(self, record):
        result = super().apply(record)
        if result is None:
            logger.debug("dropped: %s", record)
        return result  # ← this never runs when fusion succeeds
```

When fusion succeeds, the stage is pushed into the Rust parse loop and `apply()` is never called. Your override is silently dropped. Put side effects in observer hooks instead:

```python
class LoggingFilter(FilterRows):
    def _plan_kwargs(self):
        kwargs = super()._plan_kwargs() or {}
        kwargs["observer"] = {"on_row_rejected": self._log_rejected}
        return kwargs

    def _log_rejected(self, row_index):
        logger.debug("dropped row %d", row_index)
```

### Overriding `find_split_points` without measurement { #overriding-find_split_points }

```rust
fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize> {
    // "simpler" custom implementation
    ...
}
```

The default `find_split_points` handles nominal offsets, serial boundary search,
skip-region rejection, dedup, sort, and a 2 MiB chunk-size target heuristic
with thread and 1024-chunk caps. Parsing workers remain parallel. Override only
with a measured reason; most adapters benefit from the default.

### Emitting `Value::Null` for missing fields { #emitting-value-null-for-missing-fields }

```rust
sink.begin_row();
for col in &self.columns {
    let value = extract_field(col, record);
    sink.put_field(col, value);  // value is Value::Null when missing
}
sink.end_row();
```

Do not emit `Value::Null` for every missing field. The engine null-fills missing columns at `end_row()` via the dirty bitmask; emitting explicit nulls wastes a push per missing field per row. Just skip the field:

```rust
if let Some(value) = extract_field(col, record) {
    sink.put_field(col, value);
}
```

### Not checking `wants()` before expensive extraction { #not-checking-wants-before-expensive-extraction }

```rust
let id = deep_xml_path(bytes, "/Record/Header/Id");  // expensive
sink.put_field("id", Value::Str(id));
```

If `id` is dropped or projected out, the extraction runs for nothing. Always check first:

```rust
if sink.wants("id") {
    let id = deep_xml_path(bytes, "/Record/Header/Id");
    sink.put_field("id", Value::Str(id));
}
```

For expensive extractions (deep XML paths, regex captures), this is a major win.

### Panicking in `parse_chunk` { #panicking-in-parse_chunk }

```rust
fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
    let value = extract_value(bytes).unwrap();  // ← panics on malformed input
    ...
}
```

Panics are caught by `catch_unwind` in the parallel executor, but they abort the entire parse and produce a hard-to-debug `Error::Parser`. Return `Err` instead:

```rust
let value = extract_value(bytes)
    .ok_or_else(|| rypipe_core::Error::Plan("malformed record".into()))?;
```

## Configuration anti-patterns { #configuration-anti-patterns }

### Not declaring `schema` when columns are known { #not-declaring-schema }

```python
source = MyAdapter("data.log")  # no schema: triggers discovery pass
```

When the columns are known, always provide `schema`:

```python
source = MyAdapter("data.log", schema=["id", "ts", "amount", "status"])
```

Without `schema`, the engine runs a discovery pass that doubles I/O and delays the first row. With a declared schema, the engine skips discovery, stabilizes column order, and enables the `row_satisfied` byte-jump. In the crxml reference adapter, declaring `schema` lifts throughput from 4.2 GB/s to 7.6 GB/s (+80%).

### Memory budget too small { #memory-budget-too-small }

```python
source = MyAdapter("huge.log", memory="1KB")  # extremely tight budget
```

The budget is a target, not a hard limit. A budget much smaller than the data causes:

- Many tiny batches with high per-chunk setup overhead.
- Repeated `TableBuilder` allocation and finish cycles.
- RSS may still overshoot when a batch contains an unusually wide row.

Start with 500 MiB for workstations or 128 MiB for embedded workloads. Lower the budget only when you need to constrain RSS, and measure the throughput trade-off.

### Not using `schema_order` for stable column order { #not-using-schema_order }

```python
source = MyAdapter("data.log")  # no schema_order
df1 = source.to_arrow()  # columns in discovery order
df2 = source.to_arrow()  # may differ if file layout changed
```

Without `schema_order`, column order depends on discovery order, which can vary across files or chunks. Declare `schema` to fix the output order:

```python
source = MyAdapter("data.log", schema=["id", "ts", "amount", "status"])
```

## Summary { #summary }

- Reuse materialized tables; call `clear_cache()` when finished.
- Forward `plan_overrides` in adapters.
- Keep Python callables out of the hot path.
- Match the engine mode to the file size and workload.
- Declare types for numeric filters.
- Override `apply()` only on non-fusable stages; use observer hooks for side effects.
- Always check `wants()` before expensive field extraction.
- Never panic in `parse_chunk`; return `Err` instead.
