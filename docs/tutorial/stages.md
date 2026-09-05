# Stages { #stages }

Stages are the building blocks of pipelines. Each stage transforms the data
as it flows through. This page explains the stages and how to repack them
for your adapter.

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

## Repacking stages for your adapter { #repacking-stages-for-your-adapter }

Adapters include their own copies of the pipeline stage classes. This
makes the adapter self-contained: users never import from **rypipe**.

### `rypipe_log/stages/__init__.py` { #stages-init }

```python
import importlib

__all__ = ["CastTypes", "FilterRows", "RenameFields", "DropFields"]

_modules = {
    "CastTypes": ".cast",
    "FilterRows": ".filter",
    "RenameFields": ".rename",
    "DropFields": ".drop",
}


def __getattr__(name):
    if name in _modules:
        mod = importlib.import_module(_modules[name], __package__)
        return getattr(mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return __all__
```

The `__init__.py` uses lazy loading: modules are only imported when accessed.
This avoids loading stage implementations until they are actually needed.

## RenameFields { #renamefields }

Renames columns in each record.

### `rypipe_log/stages/rename.py` { #rename-py }

```python
class RenameFields:
    __slots__ = ("_mapping",)

    def __init__(self, mapping: dict[str, str]):
        self._mapping = mapping

    def apply(self, record: dict) -> dict:
        mapping = self._mapping
        return {mapping.get(k, k): v for k, v in record.items()}

    def __call__(self, stream):
        return map(self.apply, stream)

    def _plan_kwargs(self) -> dict | None:
        return {"field_mapping": self._mapping}
```

### What it does { #renamefields-what-it-does }

For each record, replaces keys according to the mapping. Keys not in the
mapping pass through unchanged:

```python
stage = RenameFields({"Name": "name", "Amount": "amount"})

# Input:  {"Name": "Alice", "Amount": 150, "Status": "active"}
# Output: {"name": "Alice", "amount": 150, "Status": "active"}
```

### Fusion { #renamefields-fusion }

**Fusable.** The Rust engine renames columns during parsing, no Python overhead.

## DropFields { #dropfields }

Removes columns entirely from each record.

### `rypipe_log/stages/drop.py` { #drop-py }

```python
class DropFields:
    __slots__ = ("_fields_set",)

    def __init__(self, fields: list[str]):
        if isinstance(fields, str):
            raise TypeError(
                f"DropFields expects a list, got a string; use DropFields([{fields!r}])"
            )
        self._fields_set = frozenset(fields)

    def apply(self, record: dict) -> dict:
        return {k: v for k, v in record.items() if k not in self._fields_set}

    def __call__(self, stream):
        return map(self.apply, stream)

    def _plan_kwargs(self) -> dict | None:
        return {"drop_fields": sorted(self._fields_set)}
```

### What it does { #dropfields-what-it-does }

For each record, removes keys in the fields set:

```python
stage = DropFields(["InternalId"])

# Input:  {"Name": "Alice", "InternalId": 42, "Amount": 150}
# Output: {"Name": "Alice", "Amount": 150}
```

### Fusion { #dropfields-fusion }

**Fusable.** The Rust engine skips the dropped column entirely: no scanning,
no decoding, no memory allocation for that column.

!!! tip

    Dropped columns are the cheapest optimization. The engine skips all
    work for the column during parsing.

## CastTypes { #casttypes }

Casts column values to the specified Python types.

### `rypipe_log/stages/cast.py` { #cast-py }

```python
from datetime import date, datetime
from decimal import Decimal
from typing import Callable
from uuid import UUID

_PY_TO_RUST_TYPE = {
    int: "int64",
    float: "float64",
    str: None,
    bool: "bool",
    date: "date32",
    datetime: "timestamp",
    Decimal: "decimal128",
    UUID: "string",
}


class CastTypes:
    __slots__ = ("_mapping",)

    def __init__(self, mapping: dict[str, Callable]):
        self._mapping = mapping

    def apply(self, record: dict) -> dict:
        for field, cast_fn in self._mapping.items():
            try:
                record[field] = cast_fn(record[field])
            except KeyError:
                pass
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"CastTypes: cannot cast field '{field}' "
                    f"value {record[field]!r}: {e}"
                ) from e
        return record

    def __call__(self, stream):
        return map(self.apply, stream)

    def _plan_kwargs(self) -> dict | None:
        ft = {}
        for field, fn in self._mapping.items():
            rust_type = _PY_TO_RUST_TYPE.get(fn)
            if rust_type is None:
                if fn is str:
                    continue
                return None
            ft[field] = rust_type
        if not ft:
            return None
        return {"field_types": ft}
```

### What it does { #casttypes-what-it-does }

For each record, applies the callable to the field value:

```python
stage = CastTypes({"age": int, "amount": float})

# Input:  {"name": "Alice", "age": "30", "amount": "150.5"}
# Output: {"name": "Alice", "age": 30, "amount": 150.5}
```

If the field is missing from the record, the cast is silently skipped. If
the cast fails (e.g., `int("abc")`), a `ValueError` is raised.

### Fusion { #casttypes-fusion }

**Fusable** for `int`, `float`, `bool`, `date`, `datetime`, `Decimal`. The
Rust engine parses the column directly as the target type: no string-to-number
conversion in Python.

## FilterRows { #filterrows }

Filters rows by a predicate.

### `rypipe_log/stages/filter.py` { #filter-py }

```python
class FilterRows:
    __slots__ = ("_predicate", "_filter_spec")

    def __init__(self, predicate=None, *, field=None, op=None, value=None,
                 field_a=None, field_b=None):
        if predicate is not None:
            self._predicate = predicate
            self._filter_spec = None
        elif field is not None and op is not None and value is not None:
            self._filter_spec = {"field": field, "op": op, "value": value}
            self._predicate = lambda r: (
                r.get(field) == value if op in ("==", "eq")
                else r.get(field) != value
            )
        elif field_a is not None and op is not None and field_b is not None:
            self._filter_spec = {"field_a": field_a, "op": op, "field_b": field_b}
            ops = {
                ">": lambda a, b: a > b, "<": lambda a, b: a < b,
                ">=": lambda a, b: a >= b, "<=": lambda a, b: a <= b,
                "==": lambda a, b: a == b, "!=": lambda a, b: a != b,
            }
            fn = ops[op]
            self._predicate = lambda r: bool(fn(r.get(field_a), r.get(field_b)))
        else:
            raise ValueError(
                "FilterRows requires a callable predicate or "
                "keyword arguments (field+op+value or field_a+op+field_b)"
            )

    def apply(self, record: dict) -> dict | None:
        return record if self._predicate(record) else None

    def __call__(self, stream):
        return (r for r in map(self.apply, stream) if r is not None)

    def _plan_kwargs(self) -> dict | None:
        return {"filter": self._filter_spec} if self._filter_spec else None
```

### Constant filter { #filterrows-constant }

Compares a field to a literal value:

```python
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

Simple lambdas (field comparisons, `startswith`, `endswith`, compound AND/OR)
are automatically compiled into fusable predicates that run in the Rust parse
loop. Complex lambdas (closures, nested calls) fall back to Python execution.
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

## Recap { #recap }

* **RenameFields** renames columns. Always fusable.
* **DropFields** removes columns. Always fusable.
* **CastTypes** converts column types. Fusable for `int`, `float`, `bool`.
* **FilterRows** filters rows. Fusable when using the keyword form or a
  compiled lambda. See [Lambda Compiler](../architecture/lambda-compiler.md).
* **FilterRowsAny**, **FilterRowsAll**, **FilterRowsNot** combine filters.
* Import stages from the adapter package, not from **rypipe**.

**Next:** [Plans](plans.md#plans), how plan fusion works.
