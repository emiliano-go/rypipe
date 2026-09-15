"""Generic pipeline of stages over a rypipe Source."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Optional, Union

from .source import _positive_int, _require_table

Stage = Callable[[Iterable[dict]], Iterable[dict]]


class Pipeline:
    """A chain of stages applied to a ``Source``.

    Stages are added with the ``|`` operator. Fusable stages (``RenameFields``,
    ``DropFields``, ``CastTypes``, ``FilterRows`` with a constant predicate)
    are pushed into the Rust parse loop when the source supports plan kwargs;
    remaining stages run over Arrow batches or dict rows.
    """

    __slots__ = ("_source", "_stages", "_batch_size", "_cached_arrow")

    def __init__(
        self,
        source,
        stages: Optional[list[Stage]] = None,
        *,
        batch_size: int = 1024,
    ):
        self._source = source
        self._stages = list(stages or ())
        for stage in self._stages:
            if not callable(stage) and not callable(getattr(stage, "apply", None)):
                raise TypeError("pipeline stage must be callable or expose apply(record)")
        self._batch_size = _positive_int(batch_size)
        self._cached_arrow = None

    def __or__(self, stage: Stage) -> "Pipeline":
        return Pipeline(
            self._source,
            [*self._stages, stage],
            batch_size=self._batch_size,
        )

    def __iter__(self) -> Iterator[dict]:
        from .fusion import fused_iter

        if self._cached_arrow is not None:
            return (row for batch in self._cached_arrow.to_batches(max_chunksize=self._batch_size)
                    for row in batch.to_pylist())
        return fused_iter(self._source, self._stages)

    def _read_arrow(self):
        """Materialize a plan-aware source and execute its stages once."""
        src = self._source
        if not (hasattr(src, "_read_arrow") and hasattr(src, "_build_plan_kwargs")):
            return None

        from .batchpipe import build_chain, collect_table
        from .fusion import plan_split

        if getattr(src, "_cached_arrow", None) is not None:
            plan_overrides, remaining = {}, self._stages
        else:
            plan_overrides, remaining = plan_split(self._stages, src._build_plan_kwargs())
        table = src._read_arrow(plan_overrides=plan_overrides) if plan_overrides else src.to_arrow()
        table = _require_table(table)
        if not remaining:
            return table
        op, trailing = build_chain(
            table,
            remaining,
            batch_size=getattr(src, "_batch_size", 1024),
        )
        if trailing:
            import pyarrow as pa

            table = collect_table(op)
            stream = (row for batch in table.to_batches(max_chunksize=self._batch_size)
                      for row in batch.to_pylist())
            for stage in trailing:
                stream = stage(stream)
            rows = list(stream)
            return pa.Table.from_pylist(rows) if rows else table.slice(0, 0)
        return collect_table(op)

    def to_arrow(self):
        """Execute once and cache the resulting ``pyarrow.Table``."""
        import pyarrow as pa

        if self._cached_arrow is None:
            table = self._read_arrow()
            self._cached_arrow = table if table is not None else pa.Table.from_pylist(list(self))
        return self._cached_arrow

    def _to_arrow(self):
        return self.to_arrow()

    def clear_cache(self) -> None:
        """Drop the cached pipeline result, leaving the source cache intact."""
        self._cached_arrow = None

    def schema(self) -> list[str]:
        """Return output column names, materializing and caching if needed."""
        return self.to_arrow().column_names

    def _iter_batches(self, batch_size: Optional[int] = None):
        if batch_size is None:
            batch_size = self._batch_size
        batch_size = _positive_int(batch_size)
        for batch in self.to_arrow().to_batches(max_chunksize=batch_size):
            yield batch.to_pylist()

    def iter_arrow_batches(self, batch_size: Optional[int] = None):
        """Yield batches from the pipeline's cached Arrow table."""
        if batch_size is None:
            batch_size = self._batch_size
        batch_size = _positive_int(batch_size)
        yield from self.to_arrow().to_batches(max_chunksize=batch_size)

    def iter_record_batches(
        self, memory: int | str = "64MiB", batch_size: Optional[int] = None, **kwargs: Any
    ):
        """Stream fusable stages when supported; otherwise materialize the pipeline."""
        if batch_size is not None:
            batch_size = _positive_int(batch_size)
        src = self._source
        if self._cached_arrow is not None or getattr(src, "_cached_arrow", None) is not None:
            yield from self.to_arrow().to_batches(
                max_chunksize=self._batch_size if batch_size is None else batch_size
            )
            return
        if hasattr(src, "iter_record_batches") and hasattr(src, "_build_plan_kwargs"):
            from .fusion import plan_split

            plan_overrides, remaining = plan_split(self._stages, src._build_plan_kwargs())
            if not remaining:
                yield from src.iter_record_batches(
                    memory=memory, batch_size=batch_size, **{**kwargs, **plan_overrides}
                )
                return
        if kwargs:
            raise TypeError("streaming options require a fully fusable pipeline")
        yield from self.iter_arrow_batches(batch_size=batch_size)

    def to_pandas(
        self,
        memory: int | str | None = None,
        dtype_backend: str = "pyarrow",
        **kwargs: Any,
    ):
        """Return a pandas DataFrame.

        Parameters
        ----------
        memory:
            Memory budget per parsing chunk (e.g. ``"64MiB"``).  When
            provided, batches are produced via ``iter_record_batches`` and
            each batch is converted to a DataFrame incrementally.  Pass
            ``threads`` in ``**kwargs`` for parallel streaming.
        dtype_backend:
            ``"pyarrow"`` (default) for Arrow-backed dtypes.
        **kwargs:
            Forwarded to ``iter_record_batches`` (e.g. ``threads=16``).
        """
        from .sinks import to_pandas

        return to_pandas(self, memory=memory, dtype_backend=dtype_backend, **kwargs)

    def to_polars(self, memory: int | str | None = None, **kwargs: Any):
        """Return a Polars DataFrame.

        Parameters
        ----------
        memory:
            Memory budget per parsing chunk (e.g. ``"64MiB"``).  When
            provided, batches are produced via ``iter_record_batches`` and
            each batch is converted to a DataFrame incrementally.  Pass
            ``threads`` in ``**kwargs`` for parallel streaming.
        **kwargs:
            Forwarded to ``iter_record_batches`` (e.g. ``threads=16``).
        """
        from .sinks import to_polars

        return to_polars(self, memory=memory, **kwargs)

    def to_parquet(
        self,
        path: Union[str, Path],
        memory: int | str | None = None,
        **kwargs: Any,
    ) -> None:
        """Write the pipeline to a Parquet file.

        Parameters
        ----------
        path:
            Output file path.
        memory:
            Memory budget per parsing chunk (e.g. ``"64MiB"``).  When
            provided, batches are produced via ``iter_record_batches`` and
            written incrementally via ``ParquetWriter``.  Pass ``threads``
            in ``**kwargs`` for parallel streaming.
        **kwargs:
            Forwarded to ``ParquetWriter`` or ``iter_record_batches``.
        """
        from .sinks import to_parquet

        to_parquet(self, path, memory=memory, **kwargs)
