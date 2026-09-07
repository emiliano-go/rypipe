# Stages { #stages }

Stages are the building blocks of pipelines. Each stage transforms the data
as it flows through. This page explains the stages and how to expose them
from your adapter.

## How stages work { #how-stages-work }

Stages are the building blocks of pipelines. Each stage transforms
the data as it flows through, like a Unix pipe:

```python
from rypipe_log import LogSource, RenameFields, FilterRows

source = LogSource("test.log")

# Each stage transforms the data in order
result = (
    source
    | RenameFields({"Name": "name"})    # renames columns
    | FilterRows(field="status", op="==", value="active")  # keeps matching rows
)

table = result.to_arrow()
```

**rypipe** stages have three methods:

* `apply(record)`: transform a single dict (used for fused iteration).
* `__call__(stream)`: transform an iterable of dicts (used for unfused
  iteration).
* `_plan_kwargs()`: return pushdown kwargs for the Rust engine, or `None`
  if the stage cannot be fused.

You never call these methods directly. The pipeline calls them automatically.
See [Stage Protocol](../advanced/stage-protocol.md) for how the engine
uses these methods.

## Re-exporting stages from your adapter { #re-exporting-stages }

Adapters re-export the stage classes from **rypipe**. Users import
everything from the adapter package, never from **rypipe** directly:

```python
from rypipe_log import LogSource, CastTypes, FilterRows, RenameFields, DropFields
```

### The re-export pattern { #re-export-pattern }

Add a `stages/` subpackage that re-exports from `rypipe.stages`:

```python
# rypipe_log/stages/__init__.py
from rypipe.stages import (
    CastTypes,
    FilterRows,
    FilterRowsAny,
    FilterRowsAll,
    FilterRowsNot,
    RenameFields,
    DropFields,
)

__all__ = [
    "CastTypes",
    "FilterRows",
    "FilterRowsAny",
    "FilterRowsAll",
    "FilterRowsNot",
    "RenameFields",
    "DropFields",
]
```

Then re-export from your package's top level:

```python
# rypipe_log/__init__.py
from .stages import (
    CastTypes,
    FilterRows,
    FilterRowsAny,
    FilterRowsAll,
    FilterRowsNot,
    RenameFields,
    DropFields,
)
```

!!! tip

    Re-exporting is zero-cost. The stage classes are the same objects; the
    engine fuses them identically whether they come from your package or from
    **rypipe**. Copying the implementations creates maintenance burden with no
    benefit.

## RenameFields { #renamefields }

Renames columns in each record.

```python
from rypipe_log import RenameFields

stage = RenameFields({"Name": "name", "Amount": "amount"})

# Input:  {"Name": "Alice", "Amount": 150, "Status": "active"}
# Output: {"name": "Alice", "amount": 150, "Status": "active"}
```

Keys not in the mapping pass through unchanged.

**Fusable.** The Rust engine renames columns during parsing, no Python overhead.
The `_plan_kwargs()` method returns `{"field_mapping": mapping}`.

## DropFields { #dropfields }

Removes columns entirely from each record.

```python
from rypipe_log import DropFields

stage = DropFields(["InternalId"])

# Input:  {"Name": "Alice", "InternalId": 42, "Amount": 150}
# Output: {"Name": "Alice", "Amount": 150}
```

**Fusable.** The Rust engine skips the dropped column entirely: no scanning,
no decoding, no memory allocation for that column.

!!! tip

    Dropped columns are the cheapest optimization. The engine skips all
    work for the column during parsing.

!!! warning

    `DropFields` expects a `list[str]`, not a bare string. Passing a string
    raises a `TypeError`:

    ```python
    # Wrong; raises TypeError
    DropFields("InternalId")

    # Correct
    DropFields(["InternalId"])
    ```

## CastTypes { #casttypes }

Casts column values to the specified Python types.

```python
from rypipe_log import CastTypes

stage = CastTypes({"age": int, "amount": float})

# Input:  {"name": "Alice", "age": "30", "amount": "150.5"}
# Output: {"name": "Alice", "age": 30, "amount": 150.5}
```

If the field is missing from the record, the cast is silently skipped. If
the cast fails (e.g., `int("abc")`), a `ValueError` is raised.

**Fusable** for `int`, `float`, `bool`, `date`, `datetime`, `Decimal`. The
Rust engine parses the column directly as the target type: no string-to-number
conversion in Python.

| Python type | Rust type | Fusable |
|-------------|-----------|---------|
| `int` | `int64` | Yes |
| `float` | `float64` | Yes |
| `bool` | `bool` | Yes |
| `date` | `date32` | Yes |
| `datetime` | `timestamp` | Yes |
| `Decimal` | `decimal128` | Yes |
| `str` | (no-op) | - |
| `UUID` | `string` | No |

!!! tip

    If your format is text-only and has no numeric fields, the `str` cast is a
    no-op. You can skip the `CastTypes` stage entirely.

## FilterRows { #filterrows }

Filters rows by a predicate.

### Constant filter { #filterrows-constant }

Compares a field to a literal value:

```python
from rypipe_log import FilterRows

stage = FilterRows(field="status", op="==", value="active")

# Input:  {"name": "Alice", "status": "active"}   kept
# Input:  {"name": "Bob",   "status": "inactive"} dropped
```

Supported operators: `==`, `!=`, `>`, `<`, `>=`, `<=`.

### Column comparison { #filterrows-compare }

Compares two fields in the same record:

```python
stage = FilterRows(field_a="price", op=">", field_b="cost")
```

### Callable predicate { #filterrows-callable }

An arbitrary Python function that receives a dict and returns `True` to keep
or `False` to drop:

```python
stage = FilterRows(lambda r: r["name"].startswith("A"))
```

Simple lambdas (field comparisons, `startswith`, `endswith`, `contains`,
`strip`, `lower`, `upper`, `replace`, `len()`, compound AND/OR, closures)
are automatically compiled into fusable predicates that run in the Rust parse
loop. Complex lambdas (method chains like `strip().lower()`) fall back to
Python execution.
See [Lambda Compiler](../architecture/lambda-compiler.md) for the full list
of supported patterns.

### Fusion { #filterrows-fusion }

FilterRows is fusable when using the keyword form or a compiled lambda.
The Rust engine applies the filter during parsing.

## FilterRowsAny { #filterrowsany }

Keeps rows that satisfy **any** of the given filters (logical OR).

```python
from rypipe_log import FilterRows, FilterRowsAny

stage = FilterRowsAny(
    FilterRows(field="status", op="==", value="active"),
    FilterRows(field="status", op="==", value="pending"),
)

# Keeps rows where status is "active" OR "pending"
```

**Parameters:** At least two `FilterRows` instances (keyword form only).

## FilterRowsAll { #filterrowsall }

Keeps rows that satisfy **all** of the given filters (logical AND).

```python
from rypipe_log import FilterRows, FilterRowsAll

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
from rypipe_log import FilterRows, FilterRowsNot

stage = FilterRowsNot(FilterRows(field="status", op="==", value="deleted"))

# Keeps rows where status != "deleted"
```

**Parameters:** Exactly one `FilterRows` instance (keyword form only).

## Combining stages { #combining-stages }

Stages compose freely. The order matters: stages are applied left to right:

```python
from rypipe_log import LogSource
from rypipe_log import RenameFields, DropFields, CastTypes, FilterRows
from rypipe_log import FilterRowsAny, FilterRowsNot

src = LogSource("test.log")

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

## When to re-implement { #when-to-re-implement }

Re-export the standard stages. Only re-implement when you need
format-specific behavior that the standard stages cannot express.

### Example: validated filter with logging { #example-validated-filter }

Suppose your format has a `status` field that is always uppercase, but
downstream consumers expect lowercase. You want to normalize before filtering
and log warnings for unexpected values:

```python
from rypipe.stages import FilterRows

class ValidatingFilterRows(FilterRows):
    """FilterRows that normalizes status values and logs warnings."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._warnings = []

    def apply(self, record: dict) -> dict | None:
        # Normalize before filtering
        if "status" in record:
            record["status"] = record["status"].lower()
            if record["status"] not in ("active", "inactive", "pending"):
                self._warnings.append(f"unexpected status: {record['status']}")
        return super().apply(record)
```

Use it like the standard `FilterRows`; it fuses identically because
`_plan_kwargs()` is inherited:

```python
from rypipe_log import LogSource
from my_adapter.stages import ValidatingFilterRows

src = LogSource("test.log")
result = src | ValidatingFilterRows(field="status", op="==", value="active")
table = result.to_arrow()
```

!!! note

    If you override `_plan_kwargs()` and return `None`, the stage falls back
    to Python execution. Only do this when fusion is impossible (e.g., the
    stage depends on external state).

See [Stage Protocol](../advanced/stage-protocol.md) for the full protocol
reference and [Pushdown Fusion](../advanced/fusion.md) for how the engine
compiles stages into an execution plan.

## Recap { #recap }

* **Re-export** stages from `rypipe.stages`; don't copy implementations.
* **RenameFields** renames columns. Always fusable.
* **DropFields** removes columns. Always fusable.
* **CastTypes** converts column types. Fusable for `int`, `float`, `bool`,
  `date`, `datetime`, `Decimal`.
* **FilterRows** filters rows. Fusable when using the keyword form or a
  compiled lambda. See [Lambda Compiler](../architecture/lambda-compiler.md).
* **FilterRowsAny**, **FilterRowsAll**, **FilterRowsNot** combine filters.
* Import stages from the adapter package, not from **rypipe**.
* Only re-implement when you need format-specific behavior.

**Next:** [Plans](plans.md#plans), how plan fusion works.
