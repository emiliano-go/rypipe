# Plans { #plans }

When you chain stages with `|`, **rypipe** collects them into a **plan**.
The plan tells the Rust engine what to do during parsing: which columns to
rename, which to drop, which to filter, and which types to use.

## How plans work { #how-plans-work }

When you write:

```python
from crxml import CrystalXMLSource, RenameFields, DropFields, FilterRows

source = CrystalXMLSource("report.xml", row_tag="Details")

result = (
    source
    | RenameFields({"Name": "name"})
    | DropFields(["InternalId"])
    | FilterRows(field="Status", op="==", value="Active")
)
```

**rypipe** collects the stages into a plan before parsing:

```
Stages:
  RenameFields({"Name": "name"})    →  field_mapping: {"Name": "name"}
  DropFields(["InternalId"])        →  drop_fields: ["InternalId"]
  FilterRows(field="Status", ...)   →  filter: {"field": "Status", "op": "==", "value": "Active"}
```

When `.to_arrow()` is called, **rypipe** passes this plan to the Rust engine.
The engine applies all three operations during parsing, in a single pass,
before any Python object is created.

## Fusable vs non-fusable { #fusable-vs-non-fusable }

Stages that can be expressed as plan kwargs are **fusable**. The Rust engine
handles them at parse time:

| Stage | Fusable when | Plan key |
|-------|-------------|----------|
| `RenameFields` | Always | `field_mapping` |
| `DropFields` | Always | `drop_fields` |
| `CastTypes` | `int`, `float`, `bool` | `field_types` |
| `FilterRows` | Keyword form (`field`/`op`/`value`) | `filter` |
| `FilterRowsAny` | All filters are keyword form | `filter` |
| `FilterRowsAll` | All filters are keyword form | `filter` |
| `FilterRowsNot` | Inner filter is keyword form | `filter` |

Stages that cannot be expressed as plan kwargs (lambda predicates) are
**non-fusable**. They run in Python over the parsed data.

## Why plans matter { #why-plans-matter }

Plans are the key to **rypipe**'s performance. When all stages are fusable,
**rypipe** pushes the entire pipeline into the Rust parse loop:

```
Without fusion:   Parse → Python rename → Python filter → Python cast → Table
With fusion:      Parse (rename + filter + cast in Rust) → Table
```

Fusion eliminates the Python overhead for each row. On a 533 MB file, this
is the difference between ~500 MB/s (Python stages) and ~4.5 GB/s (fused).

## What you need to know { #what-you-need-to-know }

As a **user**, you don't need to think about plans. Just chain stages with
`|` and call `.to_arrow()`. **rypipe** handles the rest automatically.

As an **adapter author**, you need to forward plan kwargs to your Rust reader.
See [Writing Adapters](../writing-adapters/index.md) for details.

## Recap { #recap }

* Stages are collected into a **plan** before parsing.
* Fusable stages run in the Rust parse loop (fast).
* Non-fusable stages run in Python (slow).
* **rypipe** handles plan fusion automatically.

**Next:** [Sinks](sinks.md#sinks), materializing pipeline results.
