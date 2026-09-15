"""Plan fusion: push fusable stages into the source's Rust parse loop."""

from __future__ import annotations

from typing import Callable, Iterable, Iterator, Optional


def _chain_hooks(fns):
    """Compose several callables for the same observer hook into one."""

    def chained(*args):
        for fn in fns:
            fn(*args)

    return chained


def _merge_observer_hooks(plan_overrides: dict, hooks: dict) -> None:
    """Merge a stage's ``observer`` hook dict, chaining per-hook callables
    when several stages (or the source) provide the same hook."""
    existing = plan_overrides.get("observer")
    if not existing:
        plan_overrides["observer"] = dict(hooks)
        return
    for hook, fn in hooks.items():
        if hook in existing:
            existing[hook] = _chain_hooks([existing[hook], fn])
        else:
            existing[hook] = fn


def plan_split(stages, source_plan=None):
    """Push a prefix matching the engine's rename/drop/cast/filter order."""
    order = {"field_mapping": 0, "drop_fields": 1, "field_types": 2,
             "filter": 3, "observer": 4}
    existing = dict(source_plan or {})
    rank = max((order[k] for k, v in existing.items() if k in order and v), default=-1)
    plan_overrides: dict = {}
    filter_specs = [existing["filter"]] if existing.get("filter") is not None else []
    for index, stage in enumerate(stages):
        kwargs = stage._plan_kwargs() if hasattr(stage, "_plan_kwargs") else None
        if kwargs is None:
            return plan_overrides, list(stages[index:])
        ranks = [order[k] for k in kwargs if k in order]
        if (ranks and min(ranks) < rank
                or kwargs.get("field_mapping") and existing.get("field_mapping")
                or set(kwargs.get("field_types", ())) & set(existing.get("field_types", ()))):
            return plan_overrides, list(stages[index:])
        for key, value in kwargs.items():
            if key == "filter":
                filter_specs.append(value)
                value = filter_specs[0] if len(filter_specs) == 1 else {"and": list(filter_specs)}
            elif key == "drop_fields":
                value = sorted(set(existing.get(key, ())) | set(value))
            elif key == "field_types":
                value = {**existing.get(key, {}), **value}
            elif key == "observer":
                _merge_observer_hooks(plan_overrides, value)
                continue
            plan_overrides[key] = value
            existing[key] = value
        rank = max([rank, *ranks])
    return plan_overrides, []


def _try_columnar_fusion(source, stages):
    """Run fusable stages inside the source and return a dict iterator."""
    if not hasattr(source, "_read_arrow") or not hasattr(source, "_build_plan_kwargs"):
        return None
    cached = getattr(source, "_cached_arrow", None)
    if cached is not None:
        plan_overrides, remaining = {}, stages
    else:
        plan_overrides, remaining = plan_split(stages, source._build_plan_kwargs())

    if cached is None and not plan_overrides and len(remaining) == len(stages):
        return None

    table = cached if cached is not None else source._read_arrow(plan_overrides=plan_overrides or None)
    from .source import _require_table

    table = _require_table(table)
    from .batchpipe import build_chain, iter_dicts

    op, trailing = build_chain(
        table,
        remaining,
        batch_size=getattr(source, "_batch_size", 1024),
    )
    stream = iter_dicts(op)
    for stage in trailing:
        stream = stage(stream)
    return stream


def is_fusable(stage) -> bool:
    """A stage is fusable in dict mode if it exposes ``.apply``."""
    try:
        return callable(stage.apply)
    except AttributeError:
        return False


def fused_iter(source: Iterable[dict], stages: list[Callable]) -> Iterator[dict]:
    """Best-effort fused iteration over ``source`` with ``stages``."""
    result = _try_columnar_fusion(source, stages)
    if result is not None:
        return result

    fusables: list = []
    rem = list(stages)
    while rem and is_fusable(rem[0]):
        fusables.append(rem.pop(0))

    bound = [s.apply for s in fusables]

    source_iter = (
        source._iter_batches()
        if hasattr(source, "_iter_batches")
        else source
    )

    if not bound:
        stream = (
            (r for batch in source_iter for r in batch)
            if hasattr(source, "_iter_batches")
            else iter(source_iter)
        )
        for stage in rem:
            stream = stage(stream)
        return stream

    def fused():
        iterator = (
            (r for batch in source_iter for r in batch)
            if hasattr(source, "_iter_batches")
            else source_iter
        )
        for record in iterator:
            r = record
            for fn in bound:
                r = fn(r)
                if r is None:
                    break
            else:
                yield r

    stream = fused()
    for stage in rem:
        stream = stage(stream)
    return stream
