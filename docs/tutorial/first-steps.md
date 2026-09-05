# First Steps { #first-steps }

This page explains the complete example from the [Tutorial](index.md#tutorial)
line by line. By the end, you will understand what a **Source** is, how the
pipeline operator works, and what **rypipe** does behind the scenes.

## The example { #the-example }

Here is the complete code we will explain:

```python
from crxml import CrystalXMLSource, RenameFields, CastTypes, FilterRows

source = CrystalXMLSource("report.xml", row_tag="Details")

df = (
    source
    | RenameFields({"Name": "name"})
    | CastTypes({"Amount": float})
    | FilterRows(field="Status", op="==", value="Active")
).to_pandas()

print(df)
```

## Step 1: Create a Source { #step-1-create-a-source }

```python
from crxml import CrystalXMLSource

source = CrystalXMLSource("report.xml", row_tag="Details")
```

A **Source** is a handle over one input file. It does not parse the file yet.
It stores the path and configuration, and waits for you to ask for data.

The `row_tag="Details"` argument is adapter-specific: it tells the **crxml**
adapter which XML element represents a row. Each adapter accepts its own
kwargs; check your adapter's documentation for what it supports.

### What a Source gives you { #what-a-source-gives-you }

| Method | Returns | Description |
|--------|---------|-------------|
| `.to_arrow()` | `pyarrow.Table` | Parse and cache the table |
| `.to_pandas()` | `pd.DataFrame` | Convert to pandas |
| `.to_polars()` | `pl.DataFrame` | Convert to Polars |
| `.to_parquet(path)` | - | Write to Parquet |
| `.clear_cache()` | - | Drop cached table |
| `.schema()` | `list[str]` | Column names from first row |
| `.__iter__()` | `Iterator[dict]` | Iterate rows as dicts |
| `.__or__(stage)` | `Pipeline` | Pipe operator for stages |

## Step 2: Parse with to_arrow() { #step-2-parse-with-to_arrow}

```python
source = CrystalXMLSource("report.xml", row_tag="Details")
table = source.to_arrow()
```

The first time you call `.to_arrow()`, **rypipe** parses the file:

1. The **Splitter** finds row boundaries in the byte stream.
2. The **RecordParser** extracts field values from each row.
3. The **Engine** accumulates values into Arrow columns.
4. The result is a `pyarrow.Table`.

The table is cached. Subsequent calls to `.to_arrow()`, `.to_pandas()`,
etc. reuse the cached table without re-parsing.

### What **rypipe** does automatically { #what-rypipe-does }

With just that one call, **rypipe**:

* Splits the file into chunks for parallel parsing.
* Discovers the schema from the data.
* Builds Arrow column arrays with near-zero copy.
* Returns a `pyarrow.Table` you can use directly.

## Step 3: Chain stages with | { #step-3-chain-stages-with}

```python
result = (
    source
    | RenameFields({"Name": "name"})
    | CastTypes({"Amount": float})
    | FilterRows(field="Status", op="==", value="Active")
)
```

The `|` operator chains transformation stages. Each stage transforms the
data as it flows through, like a Unix pipe:

* `RenameFields` renames the "Name" column to "name".
* `CastTypes` casts the "Amount" column from string to float.
* `FilterRows` keeps only rows where Status equals "Active".

Each `|` returns a new `Pipeline`, the original Source is not modified.

### What **rypipe** does automatically { #what-rypipe-does-pipeline }

When you call `.to_pandas()` on the pipeline, **rypipe**:

1. Splits the stages into **fusable** and **non-fusable** groups.
2. Pushes fusable stages (RenameFields, DropFields, CastTypes, constant
   FilterRows) into the Rust parse loop via the plan.
3. Runs remaining stages (lambda predicates, complex combinators) over Arrow
   batches in Python.

Fusable stages run at Rust speed during parsing: they never touch Python.

## Step 4: Get a DataFrame { #step-4-get-a-dataframe}

```python
df = result.to_pandas()
print(df)
#     name  Amount  Status
# 0  Alice   150.0  Active
# 2  Carol   200.0  Active
```

`.to_pandas()` materializes the pipeline into a pandas DataFrame. You can
also use:

* `.to_arrow()` for a `pyarrow.Table`
* `.to_polars()` for a Polars DataFrame
* `.to_parquet(path)` to write to a Parquet file
* `rypipe.collect()` to get a list of dicts

See [Sinks](sinks.md#sinks) for the full reference.

## Recap { #recap }

* A **Source** is a handle over one input file. It parses lazily and caches.
* The `|` operator chains stages into a **Pipeline**.
* **Stages** (`RenameFields`, `CastTypes`, `FilterRows`) transform data.
* **Sinks** (`.to_pandas()`, `.to_arrow()`) materialize results.
* **rypipe** pushes fusable stages into the Rust parse loop automatically.

**Next:** [Building an Adapter](building-an-adapter.md#building-an-adapter),
the basic scaffolding.
