# Configuration { #configuration }

This page collects the options you will use day to day: the Source
constructor, streaming, and error handling. The constructor options below
are the ones `crxml` accepts; other adapters may differ, so check your
adapter's documentation.

## Source constructor options { #source-constructor-options }

```python
from crxml import CrystalXMLSource

src = CrystalXMLSource(
    "report.xml",
    row_tag="Details",          # str: which XML element is one row
    engine="auto",              # "auto", "stream", "columnar", "parallel"
    threads=0,                  # int: parser threads (0 = all cores)
    memory=None,                # str | int: bound memory, e.g. "512MB"
    field_mapping=None,         # dict[str, str]: rename columns
    drop_fields=None,           # list[str]: skip columns entirely
    filter=None,                # dict: pushdown filter spec
    field_types=None,           # dict[str, str]: type hints, e.g. {"Amount": "float64"}
    dictionary_columns=None,    # list[str]: dictionary-encode columns
    schema=None,                # list[str]: project exactly these columns, in this order
    auto_dict=False,            # bool: auto dictionary-encode low-cardinality strings
    use_mmap=True,              # bool: memory-mapped file I/O
    batch_size=1024,            # int: rows per internal batch
)
```

You only need `row_tag` to get started. The rest are optimizations and
conveniences:

```python
# Typed, filtered, renamed: applied during parsing, not after
src = CrystalXMLSource(
    "report.xml",
    row_tag="Details",
    field_types={"Amount": "float64"},
    filter={"field": "Status", "op": "==", "value": "Active"},
)
print(src.to_arrow().num_rows)  # 12
```

The `filter` dict uses the same spec as `FilterRows`. See the
[FilterRows reference](../reference/python-api.md#filterrows) for
all forms and operators.

## Schema projection { #schema-projection }

`schema=[...]` is a **projection + order declaration**: the output contains
exactly the listed columns, in the listed order. Fields in the data that are
not listed are skipped during parsing (never decoded or materialized; this
is the performance win), listed columns that are absent from some or all
rows come out null-filled, and extra/unknown fields in the data are ignored
rather than raising an error. `field_mapping` renames apply before schema
matching (list the renamed names), `field_types` still applies to schema
columns, and a column listed in both `schema` and `drop_fields` is dropped
(`drop_fields` wins).

```python
src = CrystalXMLSource("report.xml", row_tag="Details", schema=["Name", "Amount"])
print(src.schema())  # ['Name', 'Amount']: Department/Status/Date never parsed
```

!!! tip

    `field_mapping`, `drop_fields`, `filter`, and `field_types` do the same
    work as the pipeline stages, but at construction time. Stages are
    usually more readable; constructor options are handy when every read
    should apply them.

## Schema discovery { #schema-discovery }

`discover_schema()` scans a file once and returns its column names, which
is handy for exploring an unknown export:

```python
from crxml import discover_schema

print(discover_schema("report.xml", row_tag="Details"))
# ['Name', 'Department', 'Amount', 'Status', 'Date']
```

It accepts the same `field_mapping`, `drop_fields`, `filter`,
`field_types`, `dictionary_columns`, `schema`, and `auto_dict` options as
the Source constructor.

## Streaming options { #streaming-options }

`iter_record_batches()` (on Sources and pipelines) takes:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `memory` | `int \| str` | `"64MB"` | Memory budget per chunk (`B`/`KB`/`MB`/`GB`/`TB`, or bytes as int). |
| `threads` | `int \| None` | `None` | Threads for parallel streaming. `None` = single-threaded. |

```python
total = 0
for batch in src.iter_record_batches(memory="64MB", threads=16):
    total += batch.num_rows
```

See [Streaming](streaming.md#streaming) for the full patterns.

## How crxml plugs in { #register-adapter }

You never wire anything up yourself: `import crxml` registers the adapter
with the engine under the name `crxml` for `.xml` files, and every
`CrystalXMLSource` uses it from there. Registration only matters if you
are writing your own adapter package. See
[Building an Adapter](../building-adapters/python-wiring.md#registration).

## Exceptions { #exceptions }

| Exception | Raised when |
|-----------|-------------|
| `crxml.XmlError` | The file could not be parsed (malformed data, invalid UTF-8). |
| `crxml.PlanError` | The engine rejects the plan options. |
| `crxml.MergeError` | Schema mismatch between chunks. |
| `ValueError` | Invalid constructor options (bad filter spec, unknown engine). |
| `FileNotFoundError` | The input path does not exist. |

Catch `ValueError` for usage errors and `XmlError` for data problems:

```python
from crxml import CrystalXMLSource, XmlError

try:
    table = CrystalXMLSource("report.xml", row_tag="Details").to_arrow()
except XmlError as e:
    print("Could not parse:", e)
```

## Recap { #recap }

* The Source constructor is the full API; `row_tag` is the only required
  option.
* Constructor options apply renames, drops, filters, and types during
  parsing.
* `discover_schema()` inspects an unknown file without a full read.
* Streaming is configured with `memory=` and `threads=` on
  `iter_record_batches()`.
* `import crxml` registers the adapter; there is nothing to wire up.

**Next:** [Building an Adapter](../building-adapters/index.md), teach
the engine your own format.
