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

Pipelines do not cache: each sink re-runs the whole chain. Materialize once and reuse the result:

```python
df = pipeline.to_pandas()
t1 = df
t2 = df
```

Sources are different: `source.to_arrow()` caches the table, so repeated
sinks on the same Source parse only once.

## Ignoring `plan_overrides` { #ignoring-plan_overrides }

```python
class MySource(Source):
    def _read_arrow(self, *, plan_overrides=None, **kwargs):
        return my_rust_read(self.path, **kwargs)  # plan_overrides lost!
```

If an adapter ignores `plan_overrides`, fused stages silently fall back to Python execution. Always forward `plan_overrides` to the Rust reader.

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

## Summary { #summary }

- Cache tables; do not re-run pipelines.
- Forward `plan_overrides` in adapters.
- Keep Python callables out of the hot path.
- Match the engine mode to the file size and workload.
- Declare types for numeric filters.
