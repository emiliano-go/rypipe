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
        import pyarrow as pa

        self._batches = table.to_batches(max_chunksize=batch_size) or [
            pa.RecordBatch.from_arrays([c.combine_chunks() for c in table.columns], schema=table.schema)
        ]
        self._i = 0

    def next_batch(self) -> Optional[Batch]:
        if self._i >= len(self._batches):
            return None
        b = self._batches[self._i]
        self._i += 1
        return Batch(b)


def _fuse_rename(mapping: dict):
    def fn(batch: Batch) -> Batch:
        rb = batch.data
        names = [mapping.get(n, n) for n in rb.schema.names]
        batch.data = rb.rename_columns(names)
        return batch

    return fn


def _fuse_drop(fields: frozenset):
    def fn(batch: Batch) -> Batch:
        rb = batch.data
        keep = [i for i, n in enumerate(rb.schema.names) if n not in fields]
        batch.data = rb.select(keep)
        return batch

    return fn


def _fuse_filter_spec(spec: dict):
    """Compile a FilterRows spec into a boolean mask ANDed into the selection."""
    import pyarrow as pa
    import pyarrow.compute as pc

    if "field" in spec:
        field = spec["field"]
        op = spec["op"]

        if op == "is_null":
            def mask_of(rb):
                m = pc.is_null(rb.column(field))
                return pc.fill_null(m, False)
        else:
            value = spec["value"]
            fn_name = {
                ">": "greater", "gt": "greater",
                "<": "less", "lt": "less",
                ">=": "greater_equal", "ge": "greater_equal",
                "<=": "less_equal", "le": "less_equal",
                "==": "equal", "eq": "equal",
                "!=": "not_equal", "ne": "not_equal",
            }[op]

            def mask_of(rb):
                from .stages.filter import _literal_scalar

                column = rb.column(field)
                try:
                    literal = _literal_scalar(value, column.type)
                except (ValueError, TypeError, pa.ArrowException):
                    return pa.array([False] * rb.num_rows, type=pa.bool_())
                m = getattr(pc, fn_name)(column, literal)
                if pa.types.is_floating(column.type):
                    m = pc.and_(m, pc.invert(pc.is_nan(column)))
                    if literal.as_py() != literal.as_py():
                        return pa.array([False] * rb.num_rows, type=pa.bool_())
                return pc.fill_null(m, False)

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
            a, b = rb.column(field_a), rb.column(field_b)
            if a.type != b.type:
                numeric = lambda t: pa.types.is_integer(t) or pa.types.is_floating(t)
                if numeric(a.type) and numeric(b.type):
                    a, b = pc.cast(a, pa.float64(), safe=False), pc.cast(b, pa.float64(), safe=False)
                elif pa.types.is_decimal(a.type) and pa.types.is_decimal(b.type):
                    common = pa.decimal256(76, max(a.type.scale, b.type.scale))
                    a, b = pc.cast(a, common), pc.cast(b, common)
                else:
                    return pa.array([False] * rb.num_rows, type=pa.bool_())
            m = getattr(pc, fn_name)(a, b)
            if pa.types.is_floating(a.type):
                m = pc.and_(m, pc.invert(pc.or_(pc.is_nan(a), pc.is_nan(b))))
            return pc.fill_null(m, False)

    def fn(batch: Batch) -> Batch:
        fields = (spec["field"],) if "field" in spec else (spec["field_a"], spec["field_b"])
        if any(field not in batch.data.schema.names for field in fields):
            m = pa.array([spec["op"] == "is_null"] * batch.data.num_rows, type=pa.bool_())
        else:
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
    from .stages.filter import FilterRows, FilterRowsAll, FilterRowsAny, FilterRowsNot
    from .stages.rename import RenameFields
    from .stages.cast import CastTypes, _cast_column

    if isinstance(stage, CastTypes):
        plan = stage._plan_kwargs()
        if plan is not None:
            def cast(batch):
                batch.data = batch.compact()
                batch.selection = None
                for name, kind in plan["field_types"].items():
                    index = batch.data.schema.get_field_index(name)
                    if index >= 0:
                        column = _cast_column(batch.data.column(index), kind)
                        batch.data = batch.data.set_column(index, name, column)
                return batch

            return cast

    if isinstance(stage, RenameFields):
        return _fuse_rename(stage._mapping)
    if isinstance(stage, DropFields):
        return _fuse_drop(stage._fields_set)
    if isinstance(stage, (FilterRows, FilterRowsAll, FilterRowsAny, FilterRowsNot)):
        plan = stage._plan_kwargs()
        if plan is None:
            return None
        spec = plan["filter"]
        if (set(spec) <= {"field", "field_a", "field_b", "op", "value"}
                and spec.get("op") in {"==", "eq", "!=", "ne", ">", "gt", "<", "lt",
                                       ">=", "ge", "<=", "le", "is_null"}):
            return _fuse_filter_spec(spec)

        def filter_rows(batch):
            import pyarrow as pa

            batch.data = batch.compact()
            batch.selection = pa.array(
                [stage.apply(row) is not None for row in batch.data.to_pylist()],
                type=pa.bool_(),
            )
            return batch

        return filter_rows
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
            return Batch(b.data.slice(0, 0))


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
        empty = None
        while True:
            b = op.next_batch()
            if b is None:
                break
            dense = b.compact()
            if dense.num_rows:
                batches.append(dense)
            else:
                empty = dense
        if not batches:
            return pa.Table.from_batches([empty]) if empty is not None else pa.table({})
        return pa.Table.from_batches(batches)
    finally:
        op.close()
