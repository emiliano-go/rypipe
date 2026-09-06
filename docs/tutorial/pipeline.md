# Pipeline { #pipeline }

The pipeline operator (`|`) lets you chain transformation stages on a Source.
Each stage transforms the data as it flows through, like a Unix pipe.

## Basic usage { #basic-usage }

```python
from rypipe_log import LogSource, RenameFields, CastTypes, FilterRows

src = LogSource("test.log")

result = (
    src
    | RenameFields({"Name": "name"})
    | CastTypes({"age": int})
    | FilterRows(field="status", op="==", value="active")
)

# Materialize to a table
table = result.to_arrow()
```

Each `|` returns a new `Pipeline`, the original Source is not modified.

### What **rypipe** does automatically { #what-rypipe-does}

When you call `.to_arrow()` on a pipeline, **rypipe**:

1. Splits the stages into **fusable** and **non-fusable** groups, because
   some stages can be pushed into the Rust parse loop while others cannot.
2. Pushes fusable stages (RenameFields, DropFields, CastTypes, constant
   FilterRows, and compiled lambdas) into the Rust parse loop via the plan.
   We do this because fusable stages run at Rust speed during parsing,
   eliminating Python overhead for every row.
3. Runs remaining stages (non-resolvable lambda predicates, complex
   combinators) over Arrow batches in Python. We do this because these
   stages cannot be expressed as plan kwargs, so they must run after parsing
   is complete.

This means fusable stages run at Rust speed during parsing: they never touch
Python. On a 10 MB file, this is the difference between ~2 seconds (all
Python) and ~200 ms (fused into Rust).

## Building the Python wrapper { #building-the-python-wrapper }

To support the pipeline `|` operator, your adapter needs a Source subclass
that forwards plan kwargs to the Rust reader. See
[Building an Adapter](building-an-adapter.md#building-an-adapter) for the
complete implementation.

## Using the Pipeline { #using-the-pipeline}

With the Source and stages in place, users can write:

```python
from rypipe_log import LogSource, CastTypes, FilterRows

src = LogSource("test.log")
result = (
    src
    | CastTypes({"age": int})
    | FilterRows(field="active", op="==", value="true")
)

table = result.to_arrow()
```

## Chaining multiple stages { #chaining-multiple-stages}

Stages are applied in order. Each `|` returns a new `Pipeline`:

```python
from rypipe_log import LogSource
from rypipe_log import RenameFields, DropFields, CastTypes, FilterRows

src = LogSource("test.log")

# All stages are fusable: runs entirely in Rust
table = (
    src
    | RenameFields({"Name": "name"})
    | DropFields(["InternalId"])
    | CastTypes({"age": int})
    | FilterRows(field="active", op="==", value="true")
).to_arrow()
```

## Iterating rows { #iterating-rows}

You can iterate over pipeline results as Python dicts:

```python
from rypipe_log import LogSource, FilterRows

src = LogSource("test.log")
pipeline = src | FilterRows(field="active", op="==", value="true")

for row in pipeline:
    print(row["name"], row["age"])
```

## Collecting to a list { #collecting-to-a-list}

```python
from rypipe_log import collect

rows = collect(src | FilterRows(field="active", op="==", value="true"))
```

## Recap { #recap }

* Use `|` to chain stages on a Source.
* **rypipe** pushes fusable stages into the Rust parse loop automatically.
* The Source's `_read_arrow()` method must forward `plan_overrides` to the
  Rust reader.
* Import stages from the adapter package, not from **rypipe**.
* Call `.to_arrow()`, `.to_pandas()`, or `.to_polars()` to materialize.

**Next:** [Stages](stages.md#stages), implement `CastTypes`, `FilterRows`,
etc.
