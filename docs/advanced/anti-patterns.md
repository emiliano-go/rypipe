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
    | (lambda table: transform(table))
    | (lambda table: another_transform(table))
).to_arrow()
```

Each callable materializes a full Python object (usually a `pyarrow.Table` or list of dicts) and breaks fusion. Prefer fused stages or move the logic into Rust.

## Repeated `to_pandas` / `to_arrow` { #repeated-to_pandas-to_arrow }

```python
t1 = pipeline.to_pandas()
t2 = pipeline.to_arrow()
t3 = pipeline.to_pandas()
```

Each call re-runs the pipeline. Cache the table once and reuse it:

```python
table = pipeline.to_arrow()
t1 = table.to_pandas()
t2 = table
```

## Ignoring `plan_overrides` { #ignoring-plan_overrides }

```python
class MySource(Source):
    def _read_arrow(self, *, plan_overrides=None, **kwargs):
        return my_rust_read(self.path, **kwargs)  # plan_overrides lost!
```

If an adapter ignores `plan_overrides`, fused stages silently fall back to Python execution. Always forward `plan_overrides` to the Rust reader.

## Wrong engine choice { #wrong-engine-choice }

```python
source.read_stream(chunks=64)  # tiny file, over-parallelized
```

For small files, columnar mode is usually fastest. For huge files, stream mode keeps memory flat. Parallel mode only wins for large, CPU-bound, cached files.

## Using `auto_dict` in parallel mode for throughput { #using-auto_dict-in-parallel-mode-for-throughput }

```python
source.read_par(auto_dict=True, chunks=32)
```

`auto_dict` forces the merge path in parallel mode. If throughput is the goal, use explicit `dictionary_columns` for only the columns that need it, or switch to columnar mode.

## Not declaring types for numeric filters { #not-declaring-types-for-numeric-filters }

```python
FilterRows(field="amount", op=">", value="100.0")
```

Without `field_types={"amount": "float64"}`, the engine may store `amount` as a string and skip the vectorized compare filter. Declare the type so the filter runs in Arrow.

## Summary { #summary }

- Cache tables; do not re-run pipelines.
- Forward `plan_overrides` in adapters.
- Keep Python callables out of the hot path.
- Match the engine mode to the file size and workload.
- Declare types for numeric filters.
