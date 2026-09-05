# Adapter design patterns { #adapter-design-patterns }

**rypipe** has one adapter API that scales in complexity. You start with a
simple adapter and override methods as needed. There are no separate
"patterns" to choose between.

## The one API: `rypipe.Adapter` { #the-one-api-rypipeadapter}

`rypipe.Adapter` inherits from `rypipe.Source`. It gives you everything:
caching, the pipeline `|` operator, streaming, and all sinks. You just
override methods to add format-specific behavior.

```python
from rypipe import Adapter

class LogAdapter(Adapter):
    def read(self, path, **kwargs):
        return _rypipe_log.read(path, **kwargs)
```

This is all you need for a simple adapter. The `read()` method receives
the merged plan kwargs (rename, drop, filter, etc.) and returns a
`pyarrow.Table`.

## Progressive overrides { #progressive-overrides}

As your adapter grows more complex, override additional methods:

### Simple Override: `read()` { #simple-override-read }

For adapters with a straightforward parsing pipeline:

```python
from rypipe import Adapter

class LogAdapter(Adapter):
    def read(self, path, **kwargs):
        return _rypipe_log.read(path, **kwargs)
```

Plan forwarding is handled automatically by `Adapter._read_arrow()`.
You receive the merged plan kwargs in `**kwargs`.

### Advanced Override: `_read_arrow()` { #advanced-override-read_arrow }

When you need control over engine selection, bounded-memory streaming,
or how the Rust reader is invoked:

```python
from rypipe import Adapter

class LogAdapter(Adapter):
    def _read_arrow(self, plan_overrides=None):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)

        # Choose engine based on file size, memory budget, etc.
        engine = self._resolve_engine(plan)

        if engine == "bounded":
            return _rypipe_log.read_bounded(str(self._path), **plan)
        elif engine == "parallel":
            return _rypipe_log.read_parallel(str(self._path), **plan)
        else:
            return _rypipe_log.read(str(self._path), **plan)
```

Override `_read_arrow()` when you need to:
* Choose between parallel, bounded, and columnar engines
* Pass adapter-specific kwargs (e.g., `row_tag`, `threads`, `memory`)
* Implement custom caching or pre-processing

### Streaming Override: `iter_record_batches()` { #streaming-override-iter_record_batches }

For true bounded-memory streaming (not just materialize-then-split):

```python
class LogAdapter(Adapter):
    def _read_arrow(self, plan_overrides=None):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return _rypipe_log.read(str(self._path), **plan)

    def iter_record_batches(self, memory="64MiB", batch_size=None, **kwargs):
        """Yield RecordBatches with constant memory."""
        yield from _rypipe_log.iter_batches(
            str(self._path), memory=memory, batch_size=batch_size, **kwargs
        )
```

Override `iter_record_batches()` when your Rust reader supports true
streaming (yielding batches without materializing the full table).

## How it works { #how-it-works}

All three levels use the same class (`rypipe.Adapter`). The only difference
is which methods you override:

| Override | Plan forwarding | Engine selection | Streaming |
|----------|----------------|------------------|-----------|
| `read()` | Automatic | No | Fallback |
| `_read_arrow()` | Manual | Yes | Fallback |
| `_read_arrow()` + `iter_record_batches()` | Manual | Yes | True streaming |

The base class (`rypipe.Adapter`) inherits from `rypipe.Source`, which
provides:
* Caching (`to_arrow()` caches the result)
* Pipeline operator (`|`)
* All sinks (`.to_pandas()`, `.to_polars()`, `.to_parquet()`)
* Fallback streaming (`iter_record_batches()` materializes then splits)

## The crxml example { #the-crxml-example}

[`crxml`](../crxml-adapter.md) overrides `_read_arrow()` for engine selection
and `iter_record_batches()` for true streaming:

```python
from rypipe import Adapter

class CrystalXMLSource(Adapter):
    def __init__(self, path, *, row_tag="Row", engine="auto",
                 threads=0, memory=None, **kwargs):
        self._row_tag = row_tag
        self._engine = engine
        self._threads = threads
        self._memory = memory
        super().__init__(path, **kwargs)

    def _read_arrow(self, plan_overrides=None):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)

        engine = self._resolve_engine(plan)
        if engine == "bounded":
            return _core.read_to_columnar_bounded(
                str(self._path), self._row_tag, self._memory, **plan
            )
        elif engine == "parallel":
            return _core.read_to_columnar_par(
                str(self._path), self._row_tag, self._num_threads, **plan
            )
        else:
            return _core.read_to_columnar(
                str(self._path), self._row_tag, **plan
            )

    def iter_record_batches(self, memory="64MiB", batch_size=None, **kwargs):
        yield from _core.iter_record_batches(
            str(self._path), self._row_tag, memory=memory, **kwargs
        )
```

## Which level should I use? { #which-level-should-i-use}

* **Simple Override** for most adapters. It's simple, correct, and sufficient.
* **Advanced Override** when you need engine selection or adapter-specific kwargs.
* **Streaming Override** when you need true bounded-memory streaming.

Start with the simple override. Only move to advanced or streaming when you
have a measured reason. Premature optimization is the root of all evil.

## Summary { #summary }

* **rypipe.Adapter** is the one API. It inherits from Source and gives you
  everything: caching, pipelines, streaming, and sinks.
* Override `read()` for simple adapters. Override `_read_arrow()` for
  advanced adapters that need engine selection.
* Start simple, override more as needed. There are no separate patterns.
