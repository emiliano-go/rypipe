"""Plan fusion: push fusable stages into the source's Rust parse loop."""

from __future__ import annotations

from typing import Callable, Iterable, Iterator, Optional


def _arrow_iter(table) -> Iterator[dict]:
    """Row iterator over a pyarrow Table (fallback)."""
    for i in range(table.num_rows):
        yield {col: table.column(col)[i].as_py() for col in table.column_names}


def plan_split(stages):
    """Split stages into (pushdown plan kwargs, remaining stages).

    Multiple fusable ``FilterRows`` stages that each push a ``filter`` spec are
    combined with an implicit ``and`` (``{"and": [...]}``) so chaining
    ``FilterRows`` stages no longer silently drops all but the last filter.
    Other plan keys (``field_mapping``, ``drop_fields``, etc.) still merge
    with last-write-wins via ``dict.update``.
    """
    plan_overrides: dict = {}
    remaining: list = []
    filter_specs: list = []
    for stage in stages:
        if hasattr(stage, "_plan_kwargs"):
            kwargs = stage._plan_kwargs()
            if kwargs is not None:
                if "filter" in kwargs:
                    filter_specs.append(kwargs["filter"])
                    rest = {k: v for k, v in kwargs.items() if k != "filter"}
                    if rest:
                        plan_overrides.update(rest)
                else:
                    plan_overrides.update(kwargs)
                continue
        remaining.append(stage)
    if filter_specs:
        plan_overrides["filter"] = filter_specs[0] if len(filter_specs) == 1 else {"and": filter_specs}
    return plan_overrides, remaining


def _try_columnar_fusion(source, stages):
    """Run fusable stages inside the source and return a dict iterator."""
    if not hasattr(source, "_read_arrow") or not hasattr(source, "_build_plan_kwargs"):
        return None

    plan_overrides, remaining = plan_split(stages)

    if not plan_overrides and len(remaining) == len(stages):
        return None

    table = source._read_arrow(plan_overrides=plan_overrides or None)
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
