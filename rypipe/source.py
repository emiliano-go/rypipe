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
from copy import deepcopy
from operator import index
from pathlib import Path
from typing import Any, Iterator, Optional, Union

import pyarrow as pa


def _positive_int(value, name="batch_size") -> int:
    try:
        result = index(value)
    except TypeError:
        raise ValueError(f"{name} must be a positive integer") from None
    if isinstance(value, bool) or result <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return result


def _require_table(table) -> pa.Table:
    if not isinstance(table, pa.Table):
        raise TypeError(f"reader must return a pyarrow.Table; got {type(table).__name__}")
    return table


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
        "_reader_options",
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
        **reader_options: Any,
    ):
        self._path = Path(path)
        if not self._path.exists():
            raise FileNotFoundError(f"File not found: {self._path}")

        if self._path.is_dir():
            raise IsADirectoryError(f"Expected an input file: {self._path}")
        self._field_mapping = dict(field_mapping or {})
        self._drop_fields = list(drop_fields or ())
        self._filter = deepcopy(filter)
        self._field_types = dict(field_types or {})
        self._dictionary_columns = list(dictionary_columns or ())
        self._schema = list(schema or ())
        self._auto_dict = auto_dict
        self._strict_types = strict_types
        self._observer = dict(observer) if observer is not None else None
        self._use_mmap = use_mmap
        self._batch_size = _positive_int(batch_size)
        self._cached_arrow = None
        self._reader_options = reader_options

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
        kwargs: dict[str, Any] = {**getattr(self, "_reader_options", {}), "use_mmap": self._use_mmap}
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
        """Return output column names, materializing and caching if needed."""
        return self.to_arrow().column_names

    def _iter_batches(self, batch_size: Optional[int] = None) -> Iterator[list[dict]]:
        """Yield batches of dicts from the parsed table."""
        if batch_size is None:
            batch_size = self._batch_size
        batch_size = _positive_int(batch_size)
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
        batch_size = _positive_int(batch_size)
        yield from self.to_arrow().to_batches(max_chunksize=batch_size)

    def iter_record_batches(
        self, memory: int | str = "64MiB", batch_size: Optional[int] = None,
        **plan_overrides: Any,
    ) -> Iterator["pa.RecordBatch"]:
        """Yield batches from the cache or the adapter's streaming reader.

        Adapters implement ``_iter_record_batches_stream`` to stream. Without
        that hook, this warns and materializes the full table. ``memory`` limits
        executor buffers, not input storage, Python overhead, or retained output.
        """
        if batch_size is not None:
            batch_size = _positive_int(batch_size)
        if self._cached_arrow is not None and not plan_overrides:
            yield from self._cached_arrow.to_batches(
                max_chunksize=self._batch_size if batch_size is None else batch_size
            )
            return
        if hasattr(self, "_iter_record_batches_stream"):
            import pyarrow as pa

            for batch in self._iter_record_batches_stream(memory, batch_size, **plan_overrides):
                if not isinstance(batch, pa.RecordBatch):
                    raise TypeError(
                        "streaming reader must yield pyarrow.RecordBatch; "
                        f"got {type(batch).__name__}"
                    )
                yield batch
            return
        import warnings

        warnings.warn(
            f"{type(self).__name__} has no streaming reader; materializing the full table",
            RuntimeWarning, stacklevel=2,
        )
        table = self._read_arrow(plan_overrides=plan_overrides) if plan_overrides else self.to_arrow()
        yield from table.to_batches(max_chunksize=self._batch_size if batch_size is None else batch_size)

    def __iter__(self) -> Iterator[dict]:
        """Iterate rows as dicts."""
        for batch in self._iter_batches():
            yield from batch

    def to_arrow(self) -> pa.Table:
        """Return a ``pyarrow.Table``, parsing once and caching the result."""
        if self._cached_arrow is None:
            self._cached_arrow = _require_table(self._read_arrow())
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
            each batch is converted to a DataFrame incrementally. The full
            output remains in memory. Pass ``threads``
            in ``**kwargs`` for parallel streaming.
        dtype_backend:
            ``"pyarrow"`` (default) for Arrow-backed dtypes, ``"numpy"``
            for NumPy-backed dtypes.
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
            each batch is converted to a DataFrame incrementally. The full
            output remains in memory. Pass ``threads`` in ``**kwargs`` for
            parallel streaming.
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
        """Write the table to Parquet.

        Parameters
        ----------
        path:
            Output file path.
        memory:
            Memory budget per parsing chunk (e.g. ``"64MiB"``).  When
            provided, batches are produced via ``iter_record_batches`` and
            written incrementally via ``ParquetWriter``. Adapters without a
            streaming reader materialize first. Pass ``threads`` for parallel
            streaming.
        **kwargs:
            Forwarded to ``ParquetWriter`` (e.g. ``compression="zstd"``)
            or to ``iter_record_batches`` (e.g. ``threads=16``).
        """
        from .sinks import to_parquet

        to_parquet(self, path, memory=memory, **kwargs)

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
        return _require_table(self.read(str(self._path), **plan))
