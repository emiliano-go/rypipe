# Pipeline { #pipeline }

The pipeline operator (`|`) lets you chain transformation stages on a Source.
Each stage transforms the data as it flows through, like a Unix pipe.

## Basic usage { #basic-usage }

```python
from crxml import CrystalXMLSource
from crxml import RenameFields, CastTypes, FilterRows

# Create a source
src = CrystalXMLSource("report.xml", row_tag="Details")

# Chain stages with |
result = (
    src
    | RenameFields({"Name": "name", "Amount": "amount"})
    | CastTypes({"amount": float})
    | FilterRows(field="status", op="==", value="active")
)

# Materialize to a table
table = result.to_arrow()
print(table)
# pyarrow.Table<name: string, amount: double, status: string>
# ----
# name: ["Alice", "Bob"]
# amount: [150.0, 75.0]
# status: ["active", "active"]
```

### What **rypipe** does automatically { #what-rypipe-does }

When you call `.to_arrow()` on a pipeline, **rypipe**:

1. Splits the stages into **fusable** and **non-fusable** groups.
2. Pushes fusable stages (RenameFields, DropFields, CastTypes, constant
   FilterRows) into the Rust parse loop via the plan.
3. Runs remaining stages (lambda predicates, complex combinators) over Arrow
   batches in Python.

This means fusable stages run at Rust speed during parsing: they never touch
Python.

## Available stages { #available-stages }

All stages are re-exported from adapter packages. Import them from the
adapter, not from **rypipe**:

```python
from crxml import RenameFields, DropFields, CastTypes, FilterRows
from crxml import FilterRowsAny, FilterRowsAll, FilterRowsNot
```

### RenameFields { #renamefields }

Renames columns. Fields not in the mapping pass through unchanged.

```python
from crxml import RenameFields

# Rename "Name" to "name" and "Amount" to "amount"
stage = RenameFields({"Name": "name", "Amount": "amount"})
```

**Parameters:**

* `mapping: dict[str, str]`: mapping from old names to new names.

### DropFields { #dropfields }

Removes columns entirely. Dropped columns are skipped during parsing: no
scanning, no decoding.

```python
from crxml import DropFields

# Drop the "InternalId" column
stage = DropFields(["InternalId"])

# Drop multiple columns
stage = DropFields(["InternalId", "TempCol", "DebugInfo"])
```

**Parameters:**

* `fields: list[str]`: list of column names to drop.

!!! warning

    Pass a list, not a bare string. `DropFields("name")` raises `TypeError`.
    Use `DropFields(["name"])` instead.


### CastTypes { #casttypes}

Casts column values to the specified Python types.

```python
from crxml import CastTypes

# Cast "amount" to float and "age" to int
stage = CastTypes({"amount": float, "age": int})
```

**Parameters:**

* `mapping: dict[str, Callable]`: mapping from column name to Python callable
  (`int`, `float`, `str`, `bool`).

!!! note

    If a row is missing the field, the cast is silently skipped. If the cast
    fails (e.g., `int("abc")`), a `ValueError` is raised with the row value.


### FilterRows { #filterrows}

Filters rows by a predicate. Supports three forms:

**Constant filter**: compare a field to a value:

```python
from crxml import FilterRows

# Keep rows where status == "active"
stage = FilterRows(field="status", op="==", value="active")

# Keep rows where age != "0"
stage = FilterRows(field="age", op="!=", value="0")
```

**Column comparison**: compare two fields:

```python
# Keep rows where price > cost
stage = FilterRows(field_a="price", op=">", field_b="cost")
```

**Callable predicate**: arbitrary Python logic:

```python
# Keep rows where name starts with "A"
stage = FilterRows(lambda r: r["name"].startswith("A"))
```

**Parameters:**

* `predicate`: a callable `(dict) -> bool` (mutually exclusive with the
  keyword forms below).
* `field: str`: column name for constant filter.
* `op: str`: operator: `"=="`, `"!="`, `"eq"`, `"ne"`.
* `value: str`: value to compare against (constant filter).
* `field_a: str`: left column for column comparison.
* `field_b: str`: right column for column comparison.

Column comparison operators: `">"`, `"<"`, `">="`, `"<="`, `"=="`, `"!="`,
`"gt"`, `"lt"`, `">="`, `"<="`, `"eq"`, `"ne"`.

### Combinators { #combinators }

Combine multiple `FilterRows` with boolean logic:

```python
from crxml import FilterRows, FilterRowsAny, FilterRowsAll, FilterRowsNot

# OR: keep rows matching ANY filter
stage = FilterRowsAny(
    FilterRows(field="status", op="==", value="active"),
    FilterRows(field="status", op="==", value="pending"),
)

# AND: keep rows matching ALL filters
stage = FilterRowsAll(
    FilterRows(field="status", op="==", value="active"),
    FilterRows(field="age", op="!=", value="0"),
)

# NOT: negate a filter
stage = FilterRowsNot(FilterRows(field="status", op="==", value="deleted"))
```

!!! warning

    Combinators only accept fusable `FilterRows` instances (the keyword form
    with `field`/`field_a`). Callable predicates cannot be combined.


## Chaining multiple stages { #chaining-multiple-stages }

Stages are applied in order. Each `|` returns a new `Pipeline`: the original
Source is not modified:

```python
from crxml import CrystalXMLSource
from crxml import RenameFields, DropFields, CastTypes, FilterRows

src = CrystalXMLSource("report.xml", row_tag="Details")

# All stages are fusable: runs entirely in Rust
table = (
    src
    | RenameFields({"Name": "name"})
    | DropFields(["InternalId"])
    | CastTypes({"amount": float})
    | FilterRows(field="status", op="==", value="active")
).to_arrow()
```

!!! tip

    When all stages are fusable, **rypipe** pushes the entire pipeline into the
    Rust parse loop. No Python row processing occurs.


## Iterating rows { #iterating-rows }

You can iterate over pipeline results as Python dicts:

```python
from crxml import CrystalXMLSource
from crxml import FilterRows

src = CrystalXMLSource("report.xml", row_tag="Details")
pipeline = src | FilterRows(field="status", op="==", value="active")

for row in pipeline:
    print(row["name"], row["amount"])
# Alice 150.0
# Bob 75.0
```

## Collecting to a list { #collecting-to-a-list }

```python
import rypipe

rows = rypipe.collect(src | FilterRows(field="status", op="==", value="active"))
print(rows)
# [{"name": "Alice", "amount": 150.0, "status": "active"}, ...]
```

## Recap { #recap }

* Use `|` to chain stages on a Source.
* **rypipe** pushes fusable stages into the Rust parse loop automatically.
* Import stages from the adapter package, not from **rypipe**.
* Call `.to_arrow()`, `.to_pandas()`, or `.to_polars()` to materialize.
* Iterate with `for row in pipeline` or collect with `rypipe.collect()`.

**Next:** [Stages](stages.md#stages): detailed reference for each stage class.
