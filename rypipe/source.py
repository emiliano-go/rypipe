"""Abstract row source and table sinks for rypipe adapters.

A ``Source`` is the user-facing handle over one input file. Adapter packages
subclass it and implement ``_read_arrow``. Once they do, users get the same
pipeline syntax crxml had for free::

    source = MyAdapterSource("data.csv")
    df = (
        source
        | RenameFields({"old_name": "new_name"})
        | DropFields(["internal_id"])
        | FilterRows(field="status", op="==", value="active")
        | CastTypes({"amount": float})
    ).to_pandas()
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterator, Optional, Union

import pyarrow as pa


class Source(ABC):
    """Base class for row-oriented file sources.

    Subclasses implement ``_read_arrow`` and return a ``pyarrow.Table``. The
    base class provides row iteration, caching, and the ``|`` pipeline
    operator. Fusable stages (rename, drop, constant filter, typed cast) are
    pushed into the Rust parse loop automatically.
    """

    __slots__ = (
        "_path",
        "_field_mapping",
        "_drop_fields",
        "_filter",
        "_field_types",
        "_dictionary_columns",
        "_schema",
        "_auto_dict",
        "_strict_types",
        "_observer",
        "_use_mmap",
        "_batch_size",
        "_cached_arrow",
    )

    def __init__(
        self,
        path: Union[str, Path],
        *,
        field_mapping: Optional[dict[str, str]] = None,
        drop_fields: Optional[list[str]] = None,
        filter: Optional[dict[str, Any]] = None,
        field_types: Optional[dict[str, str]] = None,
        dictionary_columns: Optional[list[str]] = None,
        schema: Optional[list[str]] = None,
        auto_dict: bool = False,
        strict_types: bool = False,
        observer: Optional[dict[str, Any]] = None,
        use_mmap: bool = True,
        batch_size: int = 1024,
    ):
        self._path = Path(path)
        if not self._path.exists():
            raise FileNotFoundError(f"File not found: {self._path}")

        self._field_mapping = field_mapping or {}
        self._drop_fields = drop_fields or []
        self._filter = filter
        self._field_types = field_types or {}
        self._dictionary_columns = dictionary_columns or []
        self._schema = schema or []
        self._auto_dict = auto_dict
        self._strict_types = strict_types
        self._observer = observer
        self._use_mmap = use_mmap
        self._batch_size = batch_size
        self._cached_arrow = None

    @abstractmethod
    def _read_arrow(self, plan_overrides: Optional[dict[str, Any]] = None) -> pa.Table:
        """Read the file into a ``pyarrow.Table``.

        ``plan_overrides`` contains pushdown kwargs assembled by the pipeline
        fusion layer (field_mapping, drop_fields, filter, field_types, ...).
        Implementations should merge them with the source's own plan fields,
        with pipeline stages taking precedence over construction-time options.
        """
        ...

    def _build_plan_kwargs(self) -> dict[str, Any]:
        """Return the construction-time pushdown kwargs."""
        kwargs: dict[str, Any] = {"use_mmap": self._use_mmap}
        if self._field_mapping:
            kwargs["field_mapping"] = self._field_mapping
        if self._drop_fields:
            kwargs["drop_fields"] = self._drop_fields
        if self._filter is not None:
            kwargs["filter"] = self._filter
        if self._field_types:
            kwargs["field_types"] = self._field_types
        if self._dictionary_columns:
            kwargs["dictionary_columns"] = self._dictionary_columns
        if self._schema:
            kwargs["schema"] = self._schema
        kwargs["auto_dict"] = self._auto_dict
        if self._strict_types:
            kwargs["strict_types"] = True
        if self._observer:
            kwargs["observer"] = dict(self._observer)
        return kwargs

    def schema(self) -> list[str]:
        """Return the output column names from the first row."""
        if self._schema:
            return list(self._schema)
        first = next(iter(self), None)
        return [*first] if first else []

    def _iter_batches(self, batch_size: Optional[int] = None) -> Iterator[list[dict]]:
        """Yield batches of dicts from the parsed table."""
        if batch_size is None:
            batch_size = self._batch_size
        for batch in self.to_arrow().to_batches(max_chunksize=batch_size):
            yield batch.to_pylist()

    def iter_arrow_batches(
        self, batch_size: Optional[int] = None
    ) -> Iterator["pa.RecordBatch"]:
        """Yield ``pyarrow.RecordBatch`` objects incrementally.

        Note: batches are currently produced from the materialized table, so
        memory is bounded during *parsing* (when the adapter streams) but a
        full table is still held. Use :func:`rypipe.read_batches` with an
        adapter that supports bounded-memory reads for lower peak memory.
        """
        if batch_size is None:
            batch_size = self._batch_size
        yield from self.to_arrow().to_batches(max_chunksize=batch_size)

    def iter_record_batches(
        self, memory: int | str = "64MiB", batch_size: Optional[int] = None
    ) -> Iterator["pa.RecordBatch"]:
        """Yield ``RecordBatch`` objects with constant memory (streaming).

        Unlike ``to_arrow()`` (which materializes a full table), this streams
        via ``BatchConsumer`` and drops each batch after the consumer returns.
        Peak is ``memory`` + one batch. Pass ``memory="64KB"`` and
        ``batch_size=1`` for one-row batches (Rust-only 64 KB).

        Subclasses that wrap a Rust `StreamingBatchIterator` (e.g. ``crxml``)
        should override ``_iter_record_batches_stream`` to get true streaming;
        otherwise this falls back to ``to_arrow().to_batches()``.
        """
        if hasattr(self, "_iter_record_batches_stream"):
            yield from self._iter_record_batches_stream(memory, batch_size)  # type: ignore
            return
        # Fallback: materialize then split (still bounded during parse if adapter streams)
        yield from self.to_arrow().to_batches(max_chunksize=batch_size or self._batch_size)

    def __iter__(self) -> Iterator[dict]:
        """Iterate rows as dicts."""
        for batch in self._iter_batches():
            yield from batch

    def to_arrow(self) -> pa.Table:
        """Return a ``pyarrow.Table``, parsing once and caching the result."""
        if self._cached_arrow is None:
            self._cached_arrow = self._read_arrow()
        return self._cached_arrow

    def clear_cache(self) -> None:
        """Drop the cached Arrow table to free memory."""
        self._cached_arrow = None

    def to_pandas(
        self,
        memory: int | str | None = None,
        dtype_backend: str = "pyarrow",
        **kwargs: Any,
    ) -> "pd.DataFrame":
        """Return a pandas DataFrame.

        Parameters
        ----------
        memory:
            Memory budget per parsing chunk (e.g. ``"64MiB"``).  When
            provided, batches are produced via ``iter_record_batches`` and
            each batch is converted to a DataFrame incrementally.  Peak
            memory is bounded by ``memory`` + one batch.  Pass ``threads``
            in ``**kwargs`` for parallel streaming.
        dtype_backend:
            ``"pyarrow"`` (default) for Arrow-backed dtypes, ``"numpy"``
            for NumPy-backed dtypes.
        **kwargs:
            Forwarded to ``iter_record_batches`` (e.g. ``threads=16``).
        """
        import pandas as pd

        if memory is not None:
            types_mapper = pd.ArrowDtype if dtype_backend == "pyarrow" else None
            chunks = []
            for batch in self.iter_record_batches(memory=memory, **kwargs):
                chunks.append(batch.to_pandas(types_mapper=types_mapper))
            return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        table = self.to_arrow()
        if dtype_backend == "pyarrow":
            return table.to_pandas(types_mapper=pd.ArrowDtype)
        return table.to_pandas()

    def to_polars(self, memory: int | str | None = None, **kwargs: Any):
        """Return a Polars DataFrame.

        Parameters
        ----------
        memory:
            Memory budget per parsing chunk (e.g. ``"64MiB"``).  When
            provided, batches are produced via ``iter_record_batches`` and
            each batch is converted to a DataFrame incrementally.  Peak
            memory is bounded.  Pass ``threads`` in ``**kwargs`` for
            parallel streaming.
        **kwargs:
            Forwarded to ``iter_record_batches`` (e.g. ``threads=16``).
        """
        import polars as pl

        if memory is not None:
            chunks = []
            for batch in self.iter_record_batches(memory=memory, **kwargs):
                chunks.append(pl.from_arrow(batch))
            return pl.concat(chunks) if chunks else pl.DataFrame()
        return pl.from_arrow(self.to_arrow())

    def to_parquet(
        self,
        path: Union[str, Path],
        memory: int | str | None = None,
        **kwargs: Any,
    ) -> None:
        """Write the table to Parquet.

        Parameters
        ----------
        path:
            Output file path.
        memory:
            Memory budget per parsing chunk (e.g. ``"64MiB"``).  When
            provided, batches are produced via ``iter_record_batches`` and
            written incrementally via ``ParquetWriter``.  Peak memory is
            bounded.  Pass ``threads`` in ``**kwargs`` for parallel
            streaming.
        **kwargs:
            Forwarded to ``ParquetWriter`` (e.g. ``compression="zstd"``)
            or to ``iter_record_batches`` (e.g. ``threads=16``).
        """
        import pyarrow.parquet as pq

        if memory is not None:
            parquet_keys = {
                "compression", "compression_level", "row_group_size",
                "use_dictionary", "write_statistics", "version",
            }
            parquet_kwargs = {k: v for k, v in kwargs.items() if k in parquet_keys}
            iter_kwargs = {k: v for k, v in kwargs.items() if k not in parquet_keys}
            writer = None
            for batch in self.iter_record_batches(memory=memory, **iter_kwargs):
                if writer is None:
                    writer = pq.ParquetWriter(
                        str(path), batch.schema, **parquet_kwargs
                    )
                writer.write_batch(batch)
            if writer is not None:
                writer.close()
            return
        pq.write_table(self.to_arrow(), str(path), **kwargs)

    def __or__(self, stage) -> "Pipeline":
        from .pipeline import Pipeline

        return Pipeline(self) | stage


class Adapter(Source):
    """Convenience base class for building adapter sources.

    Subclasses only need to implement ``read(self, path, **kwargs)`` and return
    a ``pyarrow.Table``. Plan kwargs (rename, drop, cast, filter, ...) are
    collected automatically and passed through to ``read``. The rest of the
    Source API (caching, iteration, sinks, and the ``|`` pipeline operator)
    comes for free.

    Example::

        class CsvAdapter(rypipe.Adapter):
            def read(self, path, **kwargs):
                return _rypipe_csv.read_csv(path, **kwargs)

        source = CsvAdapter("data.csv")
        df = (
            source
            | RenameFields({"old_name": "new_name"})
            | DropFields(["internal_id"])
        ).to_pandas()
    """

    __slots__ = ()

    def read(self, path: str, **kwargs: Any) -> pa.Table:
        """Read ``path`` into a ``pyarrow.Table``.

        ``kwargs`` contains the merged pushdown plan. Implementations should
        pass unknown kwargs down to the underlying parser.
        """
        raise NotImplementedError("subclasses must implement read()")

    def _read_arrow(self, plan_overrides: Optional[dict[str, Any]] = None) -> pa.Table:
        plan = self._build_plan_kwargs()
        if plan_overrides:
            if plan.get("observer") and plan_overrides.get("observer"):
                from .fusion import _merge_observer_hooks

                _merge_observer_hooks(plan, plan_overrides["observer"])
                plan_overrides = {
                    k: v for k, v in plan_overrides.items() if k != "observer"
                }
            plan.update(plan_overrides)
        return self.read(str(self._path), **plan)
