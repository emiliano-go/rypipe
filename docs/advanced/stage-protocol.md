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

This fallback is correct but 10-50× slower. Only return `None` from
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

A filter that logs every rejected row for debugging. Attach the side effect
as an observer hook so it keeps working when the stage fuses:

```python
from rypipe.stages import FilterRows
import logging

logger = logging.getLogger(__name__)

class LoggingFilterRows(FilterRows):
    """FilterRows that logs rejected rows (fusion-safe)."""

    def _plan_kwargs(self) -> dict | None:
        kwargs = super()._plan_kwargs() or {}
        kwargs["observer"] = {"on_row_rejected": self._log_rejected}
        return kwargs

    def _log_rejected(self, row_index: int) -> None:
        logger.debug("dropped row %d", row_index)
```

How the call reaches your method: the engine runs in Rust, so `_log_rejected`
is not called directly by Python code. The dict you pass is stored in the plan
and wrapped by a Rust `RowObserver` (`PyObserver`) that holds a reference to
the Python callable. When the Rust filter drops a row, it fires
`on_row_rejected`, the wrapper acquires the GIL and invokes your bound method:

```text
Rust engine (fused parse+filter loop)
    row fails predicate ──► PyObserver::on_row_rejected(row_index)
                               │  (acquires GIL)
                               ▼
Python: LoggingFilterRows._log_rejected(row_index)  →  logger.debug(...)
```

Yes: `_log_rejected` is effectively called from Rust, once per rejected row,
re-entrant into Python.

The `_plan_kwargs()` method is inherited and extended, so the filter still
fuses into the parse loop, and the `on_row_rejected` hook fires from inside
the engine for every rejected row.

The naive alternative (overriding `apply()` to log and delegating to
`super().apply()`) breaks silently: when fusion succeeds the stage never
runs in Python, so `apply()` is never called and nothing is logged. Rule of
thumb: overriding `apply()` on a fusable stage drops your override whenever
fusion succeeds; put side effects in observer hooks instead.

## Custom spec producers { #custom-spec-producers }

`FilterRows(predicate=obj)` fuses any object that carries a `_to_spec()`
method: the dict it returns becomes the filter spec, so a custom predicate
runs in the Rust parse loop exactly like the built-in forms. Minimal custom
producer:

```python
from rypipe.stages import FilterRows

class Weekday:
    """Keep Mon-Fri rows; fusable like any other spec."""

    def __init__(self, field: str):
        self._field = field

    def _to_spec(self) -> dict:
        return {"field": self._field, "op": "not_in", "values": ["Sat", "Sun"]}

src | FilterRows(Weekday("day"))
```

Here `_to_spec()` is the whole implementation: `FilterRows` duck-types on
it (no arguments, returns a spec dict in the shapes from
[Supported operations](../architecture/expressions.md#supported-operations)
or nested `and`/`or`/`not`), so a plain class works with no `rypipe.expr`
import. What a plain class does not get is composition (no `&`/`|`/`~`).
For that, use the built-in `Predicate`, which provides `_to_spec()` for
you.

With `Predicate`, `_to_spec()` is an accessor, not a hook you implement:
`Predicate.__init__(spec)` stores the dict as `self._spec` (that is the
whole constructor), and the inherited `_to_spec()` returns it to the plan
builder. So the subclass pattern below, calling `super().__init__(spec)`,
sets everything up; there is no `_to_spec()` to write. Only override
`_to_spec()` for lazily computed specs, and even then keep `self._spec`
populated, because the `&`/`|`/`~` operators read the attribute directly
rather than calling the method.

If your producer subclasses `Predicate` (or returns an instance of it),
composition comes for free. `Predicate` overloads the bitwise operators `&`
(and), `|` (or), and `~` (not) via `__and__` / `__or__` / `__invert__`, and
subclasses inherit `_to_spec()`. These are predicate composition, not
pipeline syntax:

```python
from rypipe.expr import Predicate   # framework base class (adapter code)
from crxml import col               # users get col from the adapter

class Rating(Predicate):
    def __init__(self, field: str):
        super().__init__({"field": field, "op": ">=", "value": "4"})

# & / | / ~ here combine predicates; | on the source builds the pipeline
src | FilterRows(Rating("stars") & ~col("hidden").is_null())
```

(Composing two `Predicate`s returns a base `Predicate`, so combinations lose
the subclass type; only the spec matters downstream, so this is cosmetic.)

Two caveats:

- The spec must be expressible in the spec language. There is no fallback:
  an unknown operator fails when the plan is built, not at row time.
- Adding a genuinely new operator is a Rust change: a new `FilterPredicate`
  variant in `rypipe-core` plus a parsing arm in
  `rypipe-python/src/plan_kwargs.rs` (`parse_filter_spec`). See the
  [Rust API](../reference/rust-api.md#filterpredicate).

Libraries and adapters can ship their own expression helpers on top of this
protocol without touching rypipe.

## Observer hooks { #observer-hooks }

A stage (or a source, via its `observer=` kwarg) can attach a dict of
callables under the `observer` plan key. The engine fires them per row from
parse threads, on every engine (serial, parallel, bounded, streaming):

| Hook | Arguments | Fires when |
|------|-----------|------------|
| `on_begin_row` | `(row_index)` | A new row starts |
| `on_put_field` | `(row_index, name, slot, value)` | A field lands in a column |
| `on_row_accepted` | `(row_index)` | The row passes the filter and is committed |
| `on_row_rejected` | `(row_index)` | The filter rejects the row |
| `on_chunk_finished` | `(total, accepted, rejected)` | A batch finishes (per-batch counts in streaming) |

Notes:

- `row_index` counts accepted rows so far, so rejected rows reuse the next
  free index.
- When a filter buffers values before the predicate resolves, `on_put_field`
  fires only for accepted rows (hooks fire on drain, not on buffer).
- Hooks run on parse threads (several at once on parallel engines): they
  must be thread-safe. Exceptions raised by a hook are printed and
  swallowed; a hook can never abort a parse.
- `on_put_field` takes the GIL per call from Python (roughly 50ns, about
  0.5s for 1M rows with 10 fields). Prefer row-level hooks in Python; keep
  field-level hooks in Rust.
- Adapter authors can attach a pure-Rust `RowObserver` via
  `ExecutionPlan::with_observer` instead: no Python bindings, no GIL, a few
  nanoseconds per event, fully parallel across parse threads. See the
  [Rust API](../reference/rust-api.md#rowobserver).
- When several stages provide the same hook, the pipeline chains the
  callables in stage order instead of overwriting.

### Side effects and fusion { #side-effects-and-fusion }

| Approach | Fuses? | Side effect when fused |
|----------|--------|------------------------|
| Override `apply()` on a fusable stage | Yes | Silently lost (never called) |
| Return `None` from `_plan_kwargs()` | No | Runs in Python over the materialized table |
| Observer hooks via `observer=` / `_plan_kwargs()` | Yes | Fires from the parse loop |

`ObservedStage` (in `rypipe.stages`) packages the third row: override
`observer_hooks()` to return the hook dict, and override `apply()` as the
fallback for pipelines that cannot fuse.

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
| `FilterRows` | `filter` | Yes (keyword form, `is_null`/`is_type`, `regex`, or expression predicate) | Filter rows by predicate |
| `FilterRowsAny` | (composed) | Yes | Logical OR of filters |
| `FilterRowsAll` | (composed) | Yes | Logical AND of filters |
| `FilterRowsNot` | (composed) | Yes | Negate a filter |
| `ObservedStage` | `observer` | Yes | Base class for observer-hook side effects |

### _plan_kwargs keys { #plan-kwargs-keys }

These keys are merged into the `ExecutionPlan` passed to the Rust parser:

| Key | Type | Purpose |
|-----|------|---------|
| `field_mapping` | `dict[str, str]` | Rename mapping: raw name → output name |
| `drop_fields` | `list[str]` | Fields to skip entirely |
| `field_types` | `dict[str, str]` | Type overrides (e.g., `"int64"`, `"float64"`) |
| `filter` | `dict` | Predicate spec: `{"field", "op", "value"}` (with `op="regex"` for regex search), `{"field_a", "op", "field_b"}`, `{"is_null": true}`, or `{"is_type": "int64"}` |
| `observer` | `dict[str, callable]` | Row observer hooks (`on_row_rejected`, ...); merged per-hook, chained across stages |

See [Execution Plan](../architecture/plan.md) for the full plan structure.
