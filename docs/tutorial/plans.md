# Plans { #plans }

When you chain stages with `|`, **rypipe** collects them into a **plan**.
The plan tells the Rust engine what to do during parsing: which columns to
rename, which to drop, which to filter, and which types to use.

## How plans work { #how-plans-work }

When you write:

```python
from rypipe_log import LogSource, RenameFields, FilterRows

source = LogSource("test.log")

result = (
    source
    | RenameFields({"Name": "name"})
    | FilterRows(field="status", op="==", value="active")
)
```

**rypipe** collects the stages into a plan before parsing:

```
Stages:
  RenameFields({"Name": "name"})    field_mapping: {"Name": "name"}
  FilterRows(field="status", ...)   filter: {"field": "status", "op": "==", "value": "active"}
```

When `.to_arrow()` is called, **rypipe** passes this plan to the Rust engine.
The engine applies all three operations during parsing, in a single pass,
before any Python object is created.

## Fusable vs non-fusable { #fusable-vs-non-fusable }

Stages that can be expressed as plan kwargs are **fusable**. The Rust engine
handles them at parse time. Fusable stages include keyword-form filters
and lambdas that the [lambda compiler](../architecture/lambda-compiler.md)
can analyze:

| Stage | Fusable when | Plan key |
|-------|-------------|----------|
| `RenameFields` | Always | `field_mapping` |
| `DropFields` | Always | `drop_fields` |
| `CastTypes` | `int`, `float`, `bool` | `field_types` |
| `FilterRows` | Keyword form, or compiled lambda (comparisons, `startswith`, `endswith`, `in`, arithmetic, compound AND/OR) | `filter` |
| `FilterRowsAny` | All inner filters are fusable | `filter` |
| `FilterRowsAll` | All inner filters are fusable | `filter` |
| `FilterRowsNot` | Inner filter is fusable | `filter` |

Stages that cannot be expressed as plan kwargs (non-resolvable lambda
predicates) are **non-fusable**. They run in Python over the parsed data.

## How plan forwarding works { #how-plan-forwarding-works }

When a user writes `source | RenameFields(...) | FilterRows(...)`, the pipeline
collects stages into a plan. When `.to_arrow()` is called, the pipeline calls
`_read_arrow(plan_overrides=...)` on your source.

Your adapter's `_read_arrow` must merge these overrides with the
construction-time kwargs:

```python
def _read_arrow(self, plan_overrides=None):
    # Start with construction-time kwargs
    plan = self._build_plan_kwargs()
    # Fused pipeline stages override construction-time kwargs
    if plan_overrides:
        plan.update(plan_overrides)
    # Pass the merged plan to the Rust reader
    return _rypipe_log.read_log(str(self._path), **plan)
```

## Why plans matter { #why-plans-matter }

Plans are the key to **rypipe**'s performance. When all stages are fusable,
**rypipe** pushes the entire pipeline into the Rust parse loop:

```
Without fusion:   Parse → Python rename → Python filter → Python cast → Table
With fusion:      Parse (rename + filter + cast in Rust) → Table
```

Fusion eliminates the Python overhead for each row. On a 533 MB file, this
is the difference between ~2 seconds (all Python) and ~200 ms (fused into Rust).

## Recap { #recap }

* Stages are collected into a **plan** before parsing.
* Fusable stages run in the Rust parse loop (fast).
* Non-fusable stages run in Python (slow).
* **rypipe** handles plan fusion automatically.

**Next:** [Sinks](sinks.md#sinks), materializing pipeline results.
