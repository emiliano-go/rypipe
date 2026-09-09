# Tutorial { #tutorial }

This tutorial teaches you how to use **rypipe** to read files into Arrow
tables and DataFrames, step by step. Each page builds on the previous one,
but every page is self-contained: you can jump straight to the topic you
need and copy-paste the examples.

## What is **rypipe**? { #what-is-rypipe }

**rypipe** is a format-agnostic columnar ingestion framework. It reads
row-oriented files (XML, CSV, JSONL, logs, etc.) and produces
<abbr title="Apache Arrow is a cross-language columnar memory format">Apache
Arrow</abbr> tables with near-zero Python overhead. **rypipe** itself ships
no parsers: an *adapter* package teaches it your format. In this tutorial we
use [**crxml**](../crxml-adapter.md), the published adapter for Crystal
Reports XML exports.

## Installation { #installation }

```bash
pip install crxml
```

That is everything you need. **crxml** pulls in the engine and `pyarrow`
automatically.

If you want pandas or Polars output, install the extras:

```bash
pip install "crxml[pandas,polars]"
```

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
0   Alice Johnson        Sales   15000.5  Active  2026-01-05
1       Bob Smith  Engineering    8500.0  Active  2026-01-06
2     Carol White    Marketing  12000.75  Active  2026-01-07
...
```

A few lines of code. The engine handled parsing, schema discovery, type
coercion, filtering, and DataFrame export automatically.

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

* **rypipe** is a format-agnostic ingestion engine; adapters add formats.
* Install with `pip install crxml` (plus optional `"crxml[pandas,polars]"`).
* A Source reads a file; `|` chains stages; a sink materializes the result.

**Next:** [First Steps](first-steps.md#first-steps), read `report.xml` and
get your first DataFrame.
