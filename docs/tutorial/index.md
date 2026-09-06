# Tutorial { #tutorial }

This tutorial teaches you how to use **rypipe** to read files into Arrow tables
and DataFrames, and how to build your own adapter. You do not need to write
Rust or build anything to get started.

## What is **rypipe**? { #what-is-rypipe }

**rypipe** is a format-agnostic columnar ingestion framework. It reads
row-oriented files (XML, CSV, JSONL, logs, etc.) and produces
<abbr title="Apache Arrow is a cross-language columnar memory format">Apache
Arrow</abbr> tables with near-zero Python overhead.

**rypipe** itself does not ship parsers. Instead, adapter packages provide
format-specific parsing. You install the adapter you need:

| Format | Adapter package | Extension |
|--------|----------------|-----------|
| Newline-delimited logs | [`rypipe_log`](building-an-adapter.md) | `.log` |

## Quick example { #quick-example }

Here is a complete example that reads a log file, renames columns, filters
rows, and produces a pandas DataFrame:

```python
from rypipe_log import LogSource, RenameFields, CastTypes, FilterRows

source = LogSource("report.log")

table = source.to_arrow()

df = (
    source
    | RenameFields({"Name": "name"})
    | CastTypes({"Amount": float})
    | FilterRows(field="Status", op="==", value="Active")
).to_pandas()

print(df)
#     name  Amount  Status
# 0  Alice   150.0  Active
# 2  Carol   200.0  Active
```

Five lines of code. **rypipe** handled parallel parsing, schema discovery,
type coercion, filtering, and Arrow export automatically.

**We will explain every line of this in the following pages.**

## Working example { #working-example }

Here is a complete workflow showing read, pipeline, streaming, and Parquet
output:

```python
from rypipe_log import LogSource, RenameFields, CastTypes, FilterRows, DropFields

# 1. Create a Source
src = LogSource("sales.log")

# 2. Simple read
table = src.to_arrow()

# 3. Pipeline with stages
df = (
    src
    | RenameFields({"CustName": "customer"})
    | DropFields(["debug_id"])
    | CastTypes({"amount": float, "quantity": int})
    | FilterRows(field="status", op="==", value="active")
).to_pandas()

# 4. Streaming (bounded memory)
df = src.to_pandas(memory="64MiB")

# 5. Streaming Parquet
src.to_parquet("output.parquet", memory="64MiB")

# 6. Parallel streaming
df = src.to_pandas(memory="64MiB", threads=16)
```

Each concept is explained in the following pages.

## Try it { #try-it }

Run the quick example:

```bash
uv add rypipe-log
python -c "
from rypipe_log import LogSource, RenameFields, CastTypes, FilterRows

source = LogSource('report.log')
table = source.to_arrow()

df = (
    source
    | RenameFields({'Name': 'name'})
    | CastTypes({'Amount': float})
    | FilterRows(field='Status', op='==', value='Active')
).to_pandas()

print(df)
"
```

Expected output:

```
    name  Amount  Status
0  Alice   150.0  Active
2  Carol   200.0  Active
```

## Recap { #recap }

* **rypipe** is a format-agnostic engine. Install an adapter for your format.
* Sources (`LogSource`, etc.) give you caching and the pipeline operator.
* Convert to pandas with `.to_pandas()` or to Polars with `.to_polars()`.
* Pass `memory=` for bounded-memory streaming.

**Next:** [First Steps](first-steps.md#first-steps), the Source abstraction
in depth.
