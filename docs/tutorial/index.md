# Tutorial { #tutorial }

This tutorial teaches you how to use **rypipe** to read files into Arrow
tables and DataFrames, step by step. Each page builds on the previous one,
but every page is self-contained: you can jump straight to the topic you
need and copy-paste the examples.

## What is **rypipe**? { #what-is-rypipe }

**rypipe** is a format-agnostic columnar ingestion framework. It reads
row-oriented files (XML, CSV, JSONL, logs, etc.) and produces
<abbr title="Apache Arrow is a cross-language columnar memory format">Apache
Arrow</abbr> tables with near-zero Python overhead.

**rypipe** itself ships no parsers: an *adapter* package teaches it your
format. The data flow looks like this:

```text
your file  ->  adapter  ->  rypipe engine  ->  Arrow / pandas / Polars
```

The **adapter** knows how to split and decode your specific format. The
**engine** handles everything else: parallel scheduling, memory-bounded
execution, schema discovery, type coercion, filtering, and Arrow export.

## Choosing an adapter { #choosing-an-adapter }

Every rypipe tutorial example needs a concrete adapter. This tutorial uses
[**crxml**](../crxml-adapter.md), the published adapter for Crystal Reports
XML exports.

Depending on your situation, pick one path:

* **Crystal Reports XML:** install **crxml** (the examples in this tutorial
  use it).
* **Another format with an existing adapter:** install that adapter instead
  and swap it for **crxml** in every example.
* **No adapter exists for your format:** follow the separate
  [Building an Adapter](../building-adapters/index.md) track to write one.

The pipeline API (`|`, stages, sinks) is the same regardless of which
adapter you use.

## Installation { #installation }

For this tutorial, install **crxml**:

```bash
pip install crxml
```

**crxml** pulls in the rypipe engine and `pyarrow` automatically. If you
want pandas or Polars output, install the extras:

```bash
pip install "crxml[pandas,polars]"
```

If you are using a different adapter, install it here instead and adjust
the import names in every example. The shapes of the API are the same; the
Source class name changes (e.g. `CrystalXMLSource` is specific to **crxml**).

## A tiny taste { #quick-example }

[Download the sample file report.xml](../examples/report.xml) and save it
next to your script. It is a small sales report with 15 rows.

```python
from crxml import CrystalXMLSource, RenameFields, CastTypes, FilterRows, to_pandas

source = CrystalXMLSource("report.xml", row_tag="Details")

df = to_pandas(
    source
    | RenameFields({"Name": "name"})
    | CastTypes({"Amount": float})
    | FilterRows(field="Status", op="==", value="Active")
)

print(df)
```

Output:

```console
$ python taste.py
             name   Department    Amount  Status        Date
0  Alice Johnson        Sales   15000.5  Active  2026-01-05
1      Bob Smith  Engineering    8500.0  Active  2026-01-06
2    Carol White    Marketing  12000.75  Active  2026-01-07
...
```

A few lines of code. The engine handled parsing, schema discovery, type
coercion, filtering, and DataFrame export automatically.

## Why do the imports come from **crxml**? { #why-imports-from-adapter }

The tutorial imports stages (`RenameFields`, `CastTypes`, `FilterRows`)
and sinks (`to_pandas`) from the adapter package rather than from
`rypipe` directly. This is normal: adapters re-export the pipeline API so
users only need one import. If you switch adapters, change the import
source and the Source class; the pipeline API stays the same.

## How the tutorial works { #how-the-tutorial-works }

Each page covers one topic:

1. [First Steps](first-steps.md#first-steps), read a file and get a DataFrame.
2. [Pipeline](pipeline.md#pipeline), chain transformations with `|`.
3. [Stages](stages.md#stages), the transformation building blocks.
4. [Sinks](sinks.md#sinks), get your data out.
5. [Streaming](streaming.md#streaming), process files larger than RAM.
6. [Configuration](configuration.md#configuration), all the options.

## Want to read your own format? { #building-your-own-adapter }

If your files are not Crystal Reports XML, you can teach **rypipe** your
format by writing a small adapter package. That is covered in the separate
[Building an Adapter](../building-adapters/index.md) track: you build a
working adapter from scratch, no prior Rust experience required.

!!! tip "Adapter developers"

    Even if you plan to write your own adapter, this tutorial is worth a
    read. **crxml** is the reference adapter, and its user-facing API
    (Sources, stages, sinks, streaming, configuration) is exactly what a
    good adapter API should look like. The pages here are a tour of the
    conventions your own package should offer; the
    [Building an Adapter](../building-adapters/index.md) track then shows
    you how to implement them.

## Recap { #recap }

* **rypipe** is the ingestion engine; adapters add format-specific parsing.
* Install the adapter you need (for this tutorial: `pip install crxml`).
* A Source reads a file; `|` chains stages; a sink materializes the result.

**Next:** [First Steps](first-steps.md#first-steps), read `report.xml` and
get your first DataFrame.
