"""Vectorized batch pipeline over Arrow RecordBatches.

A pull-based chain of operators. Filters AND into a boolean selection mask;
rows are physically dropped at a pipeline breaker or sink.
"""

from __future__ import annotations

from typing import Callable, Iterator, Optional


class Batch:
    """A dense RecordBatch plus an optional boolean selection mask."""

    __slots__ = ("data", "selection")

    def __init__(self, data, selection=None):
        self.data = data
        self.selection = selection

    def compact(self):
        """Apply the selection mask and return a dense RecordBatch."""
        if self.selection is None:
            return self.data
        return self.data.filter(self.selection)


class Operator:
    """Pull-based operator."""

    __slots__ = ()

    def open(self) -> None:
        pass

    def next_batch(self) -> Optional[Batch]:
        raise NotImplementedError

    def close(self) -> None:
        pass


class ArrowSource(Operator):
    """Source operator over an already-parsed pyarrow Table."""

    __slots__ = ("_batches", "_i")

    def __init__(self, table, batch_size: int = 1024):
        self._batches = table.to_batches(max_chunksize=batch_size)
        self._i = 0

    def next_batch(self) -> Optional[Batch]:
        if self._i >= len(self._batches):
            return None
        b = self._batches[self._i]
        self._i += 1
        return Batch(b)


def _fuse_rename(mapping: dict):
    import pyarrow as pa

    def fn(batch: Batch) -> Batch:
        rb = batch.data
        names = [mapping.get(n, n) for n in rb.schema.names]
        batch.data = pa.RecordBatch.from_arrays(rb.columns, names=names)
        return batch

    return fn


def _fuse_drop(fields: frozenset):
    import pyarrow as pa

    def fn(batch: Batch) -> Batch:
        rb = batch.data
        keep = [i for i, n in enumerate(rb.schema.names) if n not in fields]
        batch.data = pa.RecordBatch.from_arrays(
            [rb.column(i) for i in keep],
            names=[rb.schema.names[i] for i in keep],
        )
        return batch

    return fn


def _fuse_filter_spec(spec: dict):
    """Compile a FilterRows spec into a boolean mask ANDed into the selection."""
    import pyarrow.compute as pc

    if "field" in spec:
        field, op, value = spec["field"], spec["op"], spec["value"]
        eq = op in ("==", "eq")

        def mask_of(rb):
            m = pc.equal(rb.column(field), value)
            if not eq:
                m = pc.invert(m)
            return pc.fill_null(m, not eq)

    else:
        field_a, op, field_b = spec["field_a"], spec["op"], spec["field_b"]
        fn_name = {
            ">": "greater",
            "gt": "greater",
            "<": "less",
            "lt": "less",
            ">=": "greater_equal",
            "ge": "greater_equal",
            "<=": "less_equal",
            "le": "less_equal",
            "==": "equal",
            "eq": "equal",
            "!=": "not_equal",
            "ne": "not_equal",
        }[op]

        def mask_of(rb):
            m = getattr(pc, fn_name)(rb.column(field_a), rb.column(field_b))
            return pc.fill_null(m, False)

    def fn(batch: Batch) -> Batch:
        m = mask_of(batch.data)
        if batch.selection is None:
            batch.selection = m
        else:
            batch.selection = pc.and_(batch.selection, m)
        return batch

    return fn


def _arrow_fusable(stage) -> Optional[Callable[[Batch], Batch]]:
    """Compile a stage to a single-pass batch function, or None."""
    from .stages.drop import DropFields
    from .stages.filter import FilterRows
    from .stages.rename import RenameFields

    if isinstance(stage, RenameFields):
        return _fuse_rename(stage._mapping)
    if isinstance(stage, DropFields):
        return _fuse_drop(stage._fields_set)
    if isinstance(stage, FilterRows) and stage._filter_spec is not None:
        return _fuse_filter_spec(stage._filter_spec)
    return None


class FusedTransforms(Operator):
    """Fusion segment: one pass per batch for a run of fusable transforms."""

    __slots__ = ("_upstream", "_fns")

    def __init__(self, upstream: Operator, fns):
        self._upstream = upstream
        self._fns = fns

    def next_batch(self) -> Optional[Batch]:
        b = self._upstream.next_batch()
        if b is None:
            return None
        for fn in self._fns:
            b = fn(b)
        return b


class LambdaOp(Operator):
    """Fallback for row-local ``.apply(record) -> record | None`` stages.

    Compacts the batch, applies each function row by row, and rebuilds a
    RecordBatch. The output schema is pinned from the first emitted batch.
    """

    __slots__ = ("_upstream", "_applies", "_schema", "_names")

    def __init__(self, upstream: Operator, applies):
        self._upstream = upstream
        self._applies = applies
        self._schema = None
        self._names = None

    def _build(self, rows):
        import pyarrow as pa

        if self._schema is None:
            rb = pa.RecordBatch.from_pylist(rows)
            self._schema = rb.schema
            self._names = frozenset(rb.schema.names)
            return rb
        unknown = sorted({k for r in rows for k in r} - self._names)
        if unknown:
            raise ValueError(
                f".apply stage produced field(s) {unknown} not present in "
                f"earlier batches; outputs must keep a stable key set "
                f"(expected {sorted(self._names)})"
            )
        try:
            columns = [
                pa.array([r.get(name) for r in rows], type=field.type)
                for name, field in zip(self._schema.names, self._schema)
            ]
        except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
            raise ValueError(
                f".apply stage output no longer matches its earlier schema "
                f"{self._schema.names}: {e}"
            ) from e
        return pa.RecordBatch.from_arrays(columns, schema=self._schema)

    def next_batch(self) -> Optional[Batch]:
        while True:
            b = self._upstream.next_batch()
            if b is None:
                return None
            rows = b.compact().to_pylist()
            out = []
            for r in rows:
                for fn in self._applies:
                    r = fn(r)
                    if r is None:
                        break
                else:
                    out.append(r)
            if out:
                return Batch(self._build(out))


def build_chain(table, stages, batch_size: int = 1024):
    """Plan the operator chain for ``stages`` over ``table``.

    Returns ``(operator, trailing_stages)``. Trailing stages are generic
    stream transformers that cannot run per-batch, so the caller applies them
    to the dict stream.
    """
    op: Operator = ArrowSource(table, batch_size)
    i = 0
    n = len(stages)
    while i < n:
        stage = stages[i]
        fn = _arrow_fusable(stage)
        if fn is not None:
            fns = [fn]
            i += 1
            while i < n:
                nxt = _arrow_fusable(stages[i])
                if nxt is None:
                    break
                fns.append(nxt)
                i += 1
            op = FusedTransforms(op, fns)
        elif hasattr(stage, "apply") and callable(stage.apply):
            applies = [stage.apply]
            i += 1
            while (
                i < n
                and _arrow_fusable(stages[i]) is None
                and hasattr(stages[i], "apply")
            ):
                applies.append(stages[i].apply)
                i += 1
            op = LambdaOp(op, applies)
        else:
            break
    return op, list(stages[i:])


def iter_dicts(op: Operator) -> Iterator[dict]:
    """Terminal dict sink: compact each batch and yield rows."""
    op.open()
    try:
        while True:
            b = op.next_batch()
            if b is None:
                return
            yield from b.compact().to_pylist()
    finally:
        op.close()


def collect_table(op: Operator):
    """Terminal table sink: compact all batches into one pyarrow Table."""
    import pyarrow as pa

    op.open()
    try:
        batches = []
        while True:
            b = op.next_batch()
            if b is None:
                break
            dense = b.compact()
            if dense.num_rows:
                batches.append(dense)
        if not batches:
            return pa.table({})
        return pa.Table.from_batches(batches)
    finally:
        op.close()
