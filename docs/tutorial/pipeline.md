# Pipeline { #pipeline }

The `|` operator chains transformation **stages** on a Source, like a Unix
pipe. Each stage transforms the data as it flows through. This page shows
how to build pipelines, reuse them, and get data out of them.

## Basic usage { #basic-usage }

```python
from crxml import CrystalXMLSource, RenameFields, CastTypes, FilterRows, to_dataframe

src = CrystalXMLSource("report.xml", row_tag="Details")

pipeline = (
    src
    | RenameFields({"Name": "name"})
    | CastTypes({"Amount": float})
    | FilterRows(field="Status", op="==", value="Active")
)

df = to_dataframe(pipeline)
```

Each `|` returns a new `Pipeline`. The original Source is never modified.

Stages are applied in order, left to right. Here the rename happens first,
then the cast, then the filter. See [Stages](stages.md#stages) for what each
stage does.

## Reusing pipelines { #reusing-pipelines }

Pipelines are immutable values. You can store them, extend them, and run
them more than once:

```python
from crxml import CrystalXMLSource, CastTypes, FilterRows, collect

src = CrystalXMLSource("report.xml", row_tag="Details")

typed = src | CastTypes({"Amount": float})

active = typed | FilterRows(field="Status", op="==", value="Active")
inactive = typed | FilterRows(field="Status", op="==", value="Inactive")

print(len(collect(active)))    # 12
print(len(collect(inactive)))  # 3
```

`collect(pipeline)` runs the pipeline and gathers the rows into a plain
Python `list` of `dict`s, one per row, so the result works with `len()`,
indexing, loops, and anything else that takes a list. Note that each
`collect()` call runs the pipeline, so the two calls above each read the
data once.

Both `active` and `inactive` share the `typed` prefix; nothing about `typed`
changes when you extend it.

You can also build a `Pipeline` explicitly, which is handy when you
construct stages dynamically:

```python
from crxml import CrystalXMLSource, Pipeline, RenameFields, CastTypes

src = CrystalXMLSource("report.xml", row_tag="Details")

pipeline = Pipeline(src) | RenameFields({"Name": "name"}) | CastTypes({"Amount": float})
```

## Iterating rows from a pipeline { #iterating-rows }

A pipeline is iterable: it yields one `dict` per row, after all stages:

```python
from crxml import CrystalXMLSource, FilterRows

src = CrystalXMLSource("report.xml", row_tag="Details")

for row in src | FilterRows(field="Department", op="==", value="Sales"):
    print(row["Name"], row["Amount"])
```

```console
Alice Johnson 15000.50
Dave Brown 9800.25
Frank Miller 13500.00
Ivy Anderson 6300.75
Kate Martinez 16800.25
Noah Clark 8900.00
```

## Getting results out { #getting-results-out }

Use the sink functions from `crxml`:

* `collect(pipeline)`, a list of row dicts.
* `to_dataframe(pipeline)`, a pandas DataFrame.
* `to_csv(pipeline, "out.csv")`, write a CSV file.

```python
from crxml import collect, to_dataframe, to_csv

rows = collect(pipeline)
df = to_dataframe(pipeline)
to_csv(pipeline, "active.csv")
```

See [Sinks](sinks.md#sinks) for the full guide, including the Source methods
(`.to_arrow()`, `.to_parquet()`, ...).

## Plans { #plans }

<details markdown="block">
<summary>Technical Details: how plans work</summary>

When you chain stages with `|`, **rypipe** collects them into a
**plan**. The plan tells the Rust engine what to do during parsing:
which columns to rename, which to drop, which to filter, and which
types to use.

### Fusable vs non-fusable stages { #fusable-vs-non-fusable }

Stages that can be expressed as plan options are **fusable**. The Rust
engine applies them while parsing, before any Python object is created:

| Stage | Fusable when | Plan key |
|-------|--------------|----------|
| `RenameFields` | Always | `field_mapping` |
| `DropFields` | Always | `drop_fields` |
| `CastTypes` | `int`, `float`, `bool` | `field_types` |
| `FilterRows` | Keyword form (constant, column comparison, `is_null`, `is_type`) or a lambda matching a known pattern | `filter` |

Stages that cannot be expressed as plan options are **non-fusable**. They run
in Python over the parsed batches. For `FilterRows`, that only happens when
the lambda uses a pattern the compiler does not recognize (calling your own
functions, for example); comparisons, string methods, membership tests, and
`and`/`or`/`not` logic all compile. See
[Stages: callable predicate](stages.md#filterrows-callable) for the full list.

### Why plans matter { #why-plans-matter }

Plans are the key to **rypipe**'s performance:

```text
Without fusion:  Parse → Python rename → Python filter → Python cast → Table
With fusion:     Parse (rename + filter + cast in Rust) → Table
```

Fused stages run at parsing speed and never touch Python. You do not
have to do anything: the pipeline splits fusable from non-fusable stages
automatically every time you materialize it. See
[Pushdown Fusion](../advanced/fusion.md) for the deep dive.

</details>

## Recap { #recap }

* `src | stage | stage` builds a `Pipeline`; the Source is not modified.
* Pipelines are reusable and composable: extend a stored pipeline freely.
* Iterate a pipeline to get row dicts; use `collect()`, `to_dataframe()`,
  or `to_csv()` to materialize it.
* Fusable stages run inside the Rust parse loop automatically.

**Next:** [Stages](stages.md#stages), the stage catalog.
