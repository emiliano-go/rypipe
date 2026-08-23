"""Generic pipeline of stages over a rypipe Source."""

from __future__ import annotations

from typing import Callable, Iterable, Iterator, Optional


Stage = Callable[[Iterable[dict]], Iterable[dict]]


class Pipeline:
    """A chain of stages applied to a ``Source``.

    Stages are added with the ``|`` operator. Fusable stages (``RenameFields``,
    ``DropFields``, ``CastTypes``, ``FilterRows`` with a constant predicate)
    are pushed into the Rust parse loop when the source supports plan kwargs;
    remaining stages run over Arrow batches or dict rows.
    """

    __slots__ = ("_source", "_stages", "_batch_size")

    def __init__(
        self,
        source,
        stages: Optional[list[Stage]] = None,
        *,
        batch_size: int = 1024,
    ):
        self._source = source
        self._stages = stages or []
        self._batch_size = batch_size

    def __or__(self, stage: Stage) -> "Pipeline":
        return Pipeline(
            self._source,
            [*self._stages, stage],
            batch_size=self._batch_size,
        )

    def __iter__(self) -> Iterator[dict]:
        from .fusion import fused_iter

        return fused_iter(self._source, self._stages)

    def _to_arrow(self):
        """Try to run the whole pipeline as a batch chain to one table.

        Returns ``None`` when the pipeline cannot short-circuit (e.g. the
        source is not plan-aware or there are trailing generic stages).
        """
        src = self._source
        if not (hasattr(src, "_read_arrow") and hasattr(src, "_build_plan_kwargs")):
            return None

        from .batchpipe import build_chain, collect_table
        from .fusion import plan_split

        plan_overrides, remaining = plan_split(self._stages)
        table = src._read_arrow(plan_overrides=plan_overrides or None)
        op, trailing = build_chain(
            table,
            remaining,
            batch_size=getattr(src, "_batch_size", 1024),
        )
        if trailing:
            return None
        return collect_table(op)

    def _iter_batches(self, batch_size: Optional[int] = None):
        if batch_size is None:
            batch_size = self._batch_size
        table = self._to_arrow()
        if table is not None:
            for batch in table.to_batches(max_chunksize=batch_size):
                yield batch.to_pylist()
            return

        batch: list[dict] = []
        for row in self:
            batch.append(row)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
