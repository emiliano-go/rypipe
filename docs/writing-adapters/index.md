# Writing a Format Adapter { #writing-adapters }

This guide teaches you how to write a **rypipe** adapter, a package that
lets **rypipe** read your custom format.

!!! tip

    If you just want to **use** an existing adapter, see the
    [Tutorial](../tutorial/index.md) instead. This guide is for adapter authors.


## What you will build { #what-you-will-build }

A complete adapter package that:

1. Parses a newline-delimited `key=value` log format.
2. Registers with **rypipe** so `rypipe.read("file.log")` works.
3. Supports the full pipeline API (`|` operator, fusion, streaming).

```
name=Alice,age=30,active=true
name=Bob,age=25,active=false
```

## The crxml formula { #the-crxml-formula }

The reference adapter (**crxml**) defines the standard pattern. Every adapter
follows this structure:

### Rust layer { #rust-layer }

Two traits that define your format's parsing logic:

| Trait | Purpose | Required methods |
|-------|---------|-----------------|
| [**Splitter**](./splitter.md) | Find row boundaries in the byte stream | `next_record_start`, `estimate_bytes_per_row` |
| [**RecordParser**](./parser.md) | Extract field values from each row | `validate`, `parse_chunk` |

The engine provides `TableBuilder` as the production
[**ColumnarSink**](./sink.md). You rarely implement it yourself.

### Python layer { #python-layer }

| Component | Purpose |
|-----------|---------|
| **`MySource(Source)`** | Pipeline-capable source with `_read_arrow()` and plan forwarding |
| **`my_adapter.stages/`** | Own copies of `CastTypes`, `FilterRows`, `RenameFields`, `DropFields` |
| **Registration** | Adapter registered at import time via side-effect import |

!!! note

    Adapters **repack the API**: they include their own copies of the pipeline
    stage classes (`CastTypes`, `FilterRows`, `RenameFields`, `DropFields`) and
    sink functions (`collect`, `to_arrow`, `to_pandas`, `to_polars`,
    `to_parquet`, `to_dataframe`, `to_csv`) so users never import from
    **rypipe** directly. This makes the adapter self-contained.


## Adapter API contract { #adapter-api-contract }

Every adapter must expose these APIs:

### Source class (required) { #source-class }

```python
from rypipe import Source

class MySource(Source):
    def _read_arrow(self, plan_overrides=None):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return _rypipe_myfmt.read(str(self._path), **plan)
```

The Source class gives users the pipeline `|` operator, caching, and all
sinks (`.to_arrow()`, `.to_pandas()`, `.to_polars()`, `.to_parquet()`).

### Stages (required) { #stages }

Repack or reimplement these stage classes:

- `CastTypes`, cast column types
- `FilterRows`, filter rows by predicate
- `RenameFields`, rename columns
- `DropFields`, remove columns

### Sinks (required) { #sinks }

Repack or reimplement these sink functions:

- `collect(pipeline)`, collect to list of dicts
- `to_arrow(pipeline)`, materialize to pyarrow.Table
- `to_pandas(pipeline)`, convert to pandas DataFrame
- `to_polars(pipeline)`, convert to Polars DataFrame
- `to_parquet(pipeline, path)`, write to Parquet
- `to_dataframe(pipeline)`, alias for to_pandas
- `to_csv(pipeline, path)`, write to CSV

### Registration (required) { #registration }

Register the adapter at import time so `rypipe.read("file.ext")` works:

```python
def _register():
    try:
        import rypipe
    except Exception:
        return
    rypipe.register_adapter("myfmt", MyAdapter(), extensions=[".myfmt"])

_register()
```

### What NOT to implement { #what-not-to-implement }

Do **not** implement a `read()` convenience function. The Source class IS
the primary API. Users write:

```python
from my_adapter import MySource

source = MySource("file.myfmt")
table = source.to_arrow()
```

Not:

```python
from my_adapter import read  # don't do this
table = read("file.myfmt")
```


## User API { #user-api }

End users should only import from the adapter package. Here is what a
user of your adapter sees:

```python
from my_adapter import MySource, CastTypes, FilterRows

source = MySource("file.myfmt")

# One-liner
table = source.to_arrow()

# Pipeline
result = (
    source
    | CastTypes({"age": int})
    | FilterRows(field="active", op="==", value="true")
).to_arrow()
```

Users never write `from rypipe import CastTypes`: they write
`from my_adapter import CastTypes`. This is the **crxml formula**.

## How the engine works { #how-the-engine-works }

Your adapter provides the parsing logic. The engine handles everything else:

```
Input bytes (file or mmap)
  │
  ▼
Splitter::next_record_start    (find safe chunk boundaries)
  │
  ▼  [one chunk]
RecordParser::parse_chunk      (per-chunk, feeds ColumnarSink)
  │  calls: begin_row → put_field × N → end_row
  ▼
ColumnarSink (TableBuilder)    (accumulates typed columns)
  │
  ▼
Arrow RecordBatch              (zero-copy export)
  │
  ▼
pyarrow.Table                  (Python API)
```

**rypipe** handles:

* **Parallel execution**: split the file, parse chunks concurrently on
  multiple threads.
* **Bounded-memory streaming**: process one chunk at a time, keeping only
  the current chunk in memory.
* **Pushdown plans**: rename, drop, filter, type coercion, dictionary
  encoding, all pushed into the Rust parse loop.
* **Zero-copy Arrow export**: column buffers move directly into Arrow arrays.
* **Schema discovery**: find field names from a sample of the file.

## Guide contents { #guide-contents }

| Page | What you learn |
|------|---------------|
| [Quick Start](./quickstart.md) | Build a working adapter in 15 minutes |
| [Python Wiring](./python-wiring.md) | Source, adapter, registration, stages |
| [Rust Creation](./rust-creation.md) | Splitter, RecordParser, ColumnarSink |
| [Schema](./schema.md) | Schema declaration for maximum performance |
| [Techniques](./techniques.md) | Performance optimizations |
| [Anti-patterns](./anti-patterns.md) | Common mistakes to avoid |
| [Examples](./examples.md) | Worked CSV, JSONL, and TSV adapters |

## Performance model { #performance-model }

The hot path is:

```
parse_chunk → begin_row → [put_field × N] → end_row → [repeat]
```

Each `put_field` call goes through:

1. **Scan**: find the field's byte extent in the input (your parser does this).
2. **Resolve**: map raw name to output column name (engine does this).
3. **Push**: write the value into the column builder (engine does this).
4. **Filter**: check if the row passes the predicate (engine does this).

The engine optimizes steps 2–4. Your parser's job is to make step 1 fast.

For a 533 MB file on a Ryzen 5800X:

| Phase | Budget | Your responsibility |
|-------|--------|-------------------|
| Splitting | ~5% | `next_record_start` must be fast |
| Parsing | ~70% | `parse_chunk` is the hot path |
| Column building | ~20% | Engine handles this |
| Export | ~5% | Zero-copy, engine handles this |

## Recap { #recap }

* An adapter is a Rust crate (**Splitter** + **RecordParser**) and a Python
  package (**Source** + **stages** + **sinks**).
* The engine handles parallel execution, memory management, and Arrow export.
* Your parser's job is to make `parse_chunk` fast.
* Follow the **crxml** formula for a consistent user experience.
