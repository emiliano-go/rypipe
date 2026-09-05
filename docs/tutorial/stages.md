# Stages { #stages }

!!! note

    The stages shown here are available in most adapters but may have
    different options. Check your adapter's docs.

This page is a detailed reference for every pipeline stage. Each stage
transforms a stream of Python dicts and optionally pushes work into the
Rust parse loop.

## How stages work { #how-stages-work }

Stages are the building blocks of pipelines. Each stage transforms
the data as it flows through, like a Unix pipe:

```python
from crxml import CrystalXMLSource, RenameFields, FilterRows

source = CrystalXMLSource("report.xml", row_tag="Details")

# Each stage transforms the data in order
result = (
    source
    | RenameFields({"Name": "name"})    # renames columns
    | FilterRows(field="Status", op="==", value="Active")  # keeps matching rows
)

table = result.to_arrow()
```

Under the hood, stages are callables that transform dicts. **rypipe**
automatically pushes fusable stages (RenameFields, DropFields, CastTypes,
constant FilterRows) into the Rust parse loop for maximum performance.
Non-fusable stages (lambda predicates) run in Python.

## RenameFields { #renamefields }

Renames columns in each record.

```python
from crxml import RenameFields

# Rename "Name" to "name"
stage = RenameFields({"Name": "name"})
```

### What it does { #renamefields-what-it-does }

For each record, replaces keys according to the mapping. Keys not in the
mapping pass through unchanged:

```python
stage = RenameFields({"Name": "name", "Amount": "amount"})

# Input:  {"Name": "Alice", "Amount": 150, "Status": "active"}
# Output: {"name": "Alice", "amount": 150, "Status": "active"}
```

### Plan fusion { #renamefields-fusion }

`_plan_kwargs()` returns `{"field_mapping": {"Name": "name"}}`. The Rust
engine renames columns during parsing: no Python overhead.

## DropFields { #dropfields }

Removes columns entirely from each record.

```python
from crxml import DropFields

# Drop a single column
stage = DropFields(["InternalId"])

# Drop multiple columns
stage = DropFields(["InternalId", "TempCol"])
```

### What it does { #dropfields-what-it-does }

For each record, removes keys in the fields set:

```python
stage = DropFields(["InternalId"])

# Input:  {"Name": "Alice", "InternalId": 42, "Amount": 150}
# Output: {"Name": "Alice", "Amount": 150}
```

### Plan fusion { #dropfields-fusion }

`_plan_kwargs()` returns `{"drop_fields": ["InternalId"]}`. The Rust engine
skips the dropped column entirely: no scanning, no decoding, no memory
allocation for that column.

!!! tip

    Dropped columns are the cheapest optimization. The engine uses `wants()`
    to skip all work for the column during parsing.


## CastTypes { #casttypes }

Casts column values to the specified Python types.

```python
from crxml import CastTypes

# Cast "amount" to float
stage = CastTypes({"amount": float})

# Cast multiple columns
stage = CastTypes({"amount": float, "age": int, "active": bool})
```

### What it does { #casttypes-what-it-does }

For each record, applies the callable to the field value:

```python
stage = CastTypes({"age": int, "amount": float})

# Input:  {"name": "Alice", "age": "30", "amount": "150.5"}
# Output: {"name": "Alice", "age": 30, "amount": 150.5}
```

If the field is missing from the record, the cast is silently skipped. If
the cast fails (e.g., `int("abc")`), a `ValueError` is raised:

```python
# This raises ValueError: CastTypes: cannot cast field 'age' value 'abc':
# invalid literal for int()
CastTypes({"age": int})({"age": "abc"})
```

### Plan fusion { #casttypes-fusion }

`_plan_kwargs()` returns `{"field_types": {"age": "int64"}}`. The Rust engine
parses the column directly as the target type: no string-to-number
conversion in Python.

Supported type mappings:

| Python type | Rust type | Arrow type |
|------------|-----------|------------|
| `int` | `"int64"` | `int64` |
| `float` | `"float64"` | `float64` |
| `bool` | `"bool"` | `bool` |
| `str` | skipped | `string` (no-op) |

## FilterRows { #filterrows }

Filters rows by a predicate.

```python
from crxml import FilterRows

# Constant filter
stage = FilterRows(field="status", op="==", value="active")

# Column comparison
stage = FilterRows(field_a="price", op=">", field_b="cost")

# Callable predicate
stage = FilterRows(lambda r: r["amount"] > 100)
```

### Constant filter { #filterrows-constant }

Compares a field to a literal value:

```python
stage = FilterRows(field="status", op="==", value="active")

# Input:  {"name": "Alice", "status": "active"}   → kept
# Input:  {"name": "Bob",   "status": "inactive"} → dropped
```

Supported operators: `==`, `eq`, `!=`, `ne`.

### Column comparison { #filterrows-compare }

Compares two fields in the same record:

```python
stage = FilterRows(field_a="price", op=">", field_b="cost")

# Input:  {"price": 100, "cost": 50}  → kept (100 > 50)
# Input:  {"price": 30,  "cost": 50}  → dropped (30 is not > 50)
```

Supported operators: `>`, `<`, `>=`, `<=`, `==`, `!=`, `gt`, `lt`, `ge`,
`le`, `eq`, `ne`.

### Callable predicate { #filterrows-callable }

An arbitrary Python function that receives a dict and returns `True` to keep
or `False` to drop:

```python
stage = FilterRows(lambda r: r["name"].startswith("A") and r["amount"] > 100)
```

!!! warning

    Callable predicates **cannot be fused** into the Rust parse loop. They run
    in Python over the full table. For best performance, use the keyword form
    (`field`/`op`/`value`) whenever possible.


### Plan fusion { #filterrows-fusion }

Constant filters and column comparisons return a `_plan_kwargs()` dict that
the Rust engine applies during parsing. Callable predicates return `None`
(no fusion).

## FilterRowsAny { #filterrowsany }

Keeps rows that satisfy **any** of the given filters (logical OR).

```python
from crxml import FilterRows, FilterRowsAny

stage = FilterRowsAny(
    FilterRows(field="status", op="==", value="active"),
    FilterRows(field="status", op="==", value="pending"),
)

# Keeps rows where status is "active" OR "pending"
```

**Parameters:** At least two `FilterRows` instances (keyword form only).

### Plan fusion { #filterrowsany-fusion }

`_plan_kwargs()` returns `{"filter": {"or": [...]}}`. The Rust engine applies
the OR tree during parsing.

## FilterRowsAll { #filterrowsall }

Keeps rows that satisfy **all** of the given filters (logical AND).

```python
from crxml import FilterRows, FilterRowsAll

stage = FilterRowsAll(
    FilterRows(field="status", op="==", value="active"),
    FilterRows(field="age", op="!=", value="0"),
)

# Keeps rows where status == "active" AND age != "0"
```

**Parameters:** At least two `FilterRows` instances (keyword form only).

!!! note

    Chaining plain `FilterRows` with `|` already implies AND. `FilterRowsAll`
    is useful when combining inside another combinator or when the order matters.


## FilterRowsNot { #filterrowsnot }

Negates a single filter.

```python
from crxml import FilterRows, FilterRowsNot

stage = FilterRowsNot(FilterRows(field="status", op="==", value="deleted"))

# Keeps rows where status != "deleted"
```

**Parameters:** Exactly one `FilterRows` instance (keyword form only).

## Combining stages { #combining-stages }

Stages compose freely. The order matters: stages are applied left to right:

```python
from crxml import CrystalXMLSource
from crxml import RenameFields, DropFields, CastTypes, FilterRows
from crxml import FilterRowsAny, FilterRowsNot

src = CrystalXMLSource("report.xml", row_tag="Details")

# Complex pipeline
result = (
    src
    | RenameFields({"Name": "name", "Amount": "amount"})
    | DropFields(["InternalId", "DebugInfo"])
    | CastTypes({"amount": float})
    | FilterRowsAny(
        FilterRows(field="status", op="==", value="active"),
        FilterRows(field="status", op="==", value="pending"),
    )
    | FilterRowsNot(FilterRows(field="name", op="==", value="system"))
)

table = result.to_arrow()
```

## Recap { #recap }

* **RenameFields** renames columns. Always fusable.
* **DropFields** removes columns. Always fusable.
* **CastTypes** converts column types. Fusable for `int`, `float`, `bool`.
* **FilterRows** filters rows. Fusable when using the keyword form.
* **FilterRowsAny**, **FilterRowsAll**, **FilterRowsNot** combine filters.
* Import stages from the adapter package, not from **rypipe**.

**Next:** [Sinks](sinks.md#sinks): materializing pipeline results.
