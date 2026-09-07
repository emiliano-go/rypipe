# Stage Protocol { #stage-protocol }

This page explains the stage protocol: how stages integrate with the
**rypipe** engine, why re-exporting works, and when to re-implement.

## The stage protocol { #the-stage-protocol }

Any Python class with these three methods is a valid stage:

```python
class MyStage:
    def apply(self, record: dict) -> dict | None:
        """Transform a single record. Return None to drop the row."""
        ...

    def __call__(self, stream):
        """Transform an iterable of records (unfused path)."""
        return map(self.apply, stream)

    def _plan_kwargs(self) -> dict | None:
        """Return pushdown kwargs for the Rust engine, or None if not fusable."""
        ...
```

The pipeline calls these methods automatically. You never call them directly.

| Method | When called | Purpose |
|--------|-------------|---------|
| `_plan_kwargs()` | Once, when the pipeline is materialized | Collects pushdown kwargs for the Rust engine |
| `apply(record)` | Per-row, in the Rust parse loop (if fused) | Transform a single dict during parsing |
| `__call__(stream)` | Per-row, in Python (if not fused) | Transform an iterable of dicts outside the engine |

### The protocol is the contract { #the-protocol-is-the-contract }

The engine does not check class names, inheritance, or module origins. It
calls `_plan_kwargs()` on every stage in the pipeline. If the return value
is a dict, the engine merges it into the `ExecutionPlan`. If it is `None`,
the stage runs in Python.

This is why re-exporting works: the engine sees the same `_plan_kwargs()`
method regardless of whether the class was imported from `rypipe.stages`
or from `my_adapter.stages`. The protocol is the contract, not the import
path.

## How fusion works { #how-fusion-works }

When a pipeline reaches `.to_arrow()` (or any sink), the engine:

1. Walks the stage chain left to right.
2. Calls `_plan_kwargs()` on each stage.
3. Merges the results into a single `ExecutionPlan`.
4. Passes the plan to the Rust parser.

```python
result = (
    source
    | RenameFields({"old_name": "new_name"})  # _plan_kwargs → {"field_mapping": ...}
    | DropFields(["internal_id"])              # _plan_kwargs → {"drop_fields": ...}
    | FilterRows(field="status", op="==", value="active")  # _plan_kwargs → {"filter": ...}
    | CastTypes({"amount": float})             # _plan_kwargs → {"field_types": ...}
).to_arrow()
```

The Rust parser then applies all four operations during the parse loop:
renames arrive, dropped fields are skipped, rows are filtered, and types
are cast, all without materializing a full table in Python.

See [Pushdown Fusion](./fusion.md) for the full flow diagram and
optimization details.

### What happens when fusion is not possible { #when-fusion-fails }

If any stage returns `None` from `_plan_kwargs()`, the pipeline splits:

1. The stages before the non-fusable stage are fused into a plan.
2. The Rust parser runs with that plan, producing a table.
3. The non-fusable stage runs in Python over the materialized table.
4. The remaining stages run in Python (or fuse a second plan if possible).

This fallback is correct but 10–50× slower. Only return `None` from
`_plan_kwargs()` when fusion is genuinely impossible.

## Why re-exporting works { #why-re-exporting-works }

Stage classes are stateless Python objects. The engine interacts with them
through the protocol (`_plan_kwargs()`), not through their identity or
location in memory.

```python
# This works
from rypipe.stages import FilterRows

# This also works
from my_adapter.stages import FilterRows  # re-exported from rypipe

# Same object, same protocol, same fusion
assert FilterRows is FilterRows
```

Re-exporting gives adapter authors:

- **Zero maintenance burden**: stage improvements in **rypipe** propagate
  automatically.
- **Identical fusion**: the engine sees the same `_plan_kwargs()` method.
- **User-facing isolation**: users import from the adapter, never from
  **rypipe** directly.

## When to re-implement { #when-to-re-implement }

Re-export the standard stages. Only re-implement when you need behavior
that the standard stages cannot express.

### Legitimate reasons to re-implement { #legitimate-reasons }

| Reason | Example |
|--------|---------|
| Format-specific validation | Normalize field values before filtering |
| Custom logging | Log warnings for unexpected values |
| Additional state | Track statistics during pipeline execution |
| Non-fusable logic | Apply a side effect that cannot run in Rust |

### Example: logging filter { #example-logging-filter }

A filter that logs every rejected row for debugging:

```python
from rypipe.stages import FilterRows
import logging

logger = logging.getLogger(__name__)

class LoggingFilterRows(FilterRows):
    """FilterRows that logs rejected rows."""

    def apply(self, record: dict) -> dict | None:
        result = super().apply(record)
        if result is None:
            logger.debug("dropped row: %s", record)
        return result
```

This works because `apply()` is called per-row. The `_plan_kwargs()` method
is inherited, so fusion still applies for the keyword form.

### Example: non-fusable stage { #example-non-fusable-stage }

A stage that writes to an external service cannot be fused:

```python
class SendToWebhook:
    """Send each record to a webhook. Never fusable."""

    def __init__(self, url: str):
        self._url = url

    def apply(self, record: dict) -> dict | None:
        requests.post(self._url, json=record)
        return record  # pass through

    def __call__(self, stream):
        return map(self.apply, stream)

    def _plan_kwargs(self) -> dict | None:
        return None  # cannot fuse: side effect
```

Returning `None` forces the pipeline to materialize a table first, then
run this stage in Python. This is correct but slower.

## Stage reference { #stage-reference }

| Stage | `_plan_kwargs()` key | Fusable | Description |
|-------|----------------------|---------|-------------|
| `RenameFields` | `field_mapping` | Yes | Rename columns |
| `DropFields` | `drop_fields` | Yes | Remove columns |
| `CastTypes` | `field_types` | Yes (for `int`, `float`, `bool`, `date`, `datetime`, `Decimal`) | Cast column types |
| `FilterRows` | `filter` | Yes (keyword form or compiled lambda) | Filter rows by predicate |
| `FilterRowsAny` | (composed) | Yes | Logical OR of filters |
| `FilterRowsAll` | (composed) | Yes | Logical AND of filters |
| `FilterRowsNot` | (composed) | Yes | Negate a filter |

### _plan_kwargs keys { #plan-kwargs-keys }

These keys are merged into the `ExecutionPlan` passed to the Rust parser:

| Key | Type | Purpose |
|-----|------|---------|
| `field_mapping` | `dict[str, str]` | Rename mapping: raw name → output name |
| `drop_fields` | `list[str]` | Fields to skip entirely |
| `field_types` | `dict[str, str]` | Type overrides (e.g., `"int64"`, `"float64"`) |
| `filter` | `dict` | Predicate spec: `{"field", "op", "value"}` or `{"field_a", "op", "field_b"}` |

See [Execution Plan](../architecture/plan.md) for the full plan structure.
