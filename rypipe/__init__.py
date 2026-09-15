"""rypipe: pure format-agnostic columnar engine for Python.

`rypipe` itself does not ship parsers for XML, CSV, JSON, HTML, or any other
format. It provides the ingestion-to-Arrow engine, an adapter registry, and a
pipeline API that lets adapters expose chainable sources::

    import rypipe
    from rypipe import RenameFields, DropFields, FilterRows, CastTypes
    import my_adapter

    source = my_adapter.MySource("data.myfmt")
    df = (
        source
        | RenameFields({"old": "new"})
        | DropFields(["temp"])
        | FilterRows(field="status", op="==", value="active")
        | CastTypes({"amount": float})
    ).to_pandas()

Adapters register themselves so the high-level ``read`` API also works::

    table = rypipe.read("data.myfmt", fields={"amount": "float64"})
"""

from __future__ import annotations

import os
import re
import warnings
from pathlib import Path
from typing import Any, Iterable, Iterator

import _rypipe

from .source import Adapter, Source, _positive_int, _require_table
from .expr import col
from .pipeline import Pipeline
from .stages import (
    CastTypes,
    DropFields,
    FilterRows,
    FilterRowsAll,
    FilterRowsAny,
    FilterRowsNot,
    RenameFields,
)
from .sinks import (
    collect,
    to_arrow,
    to_csv,
    to_pandas,
    to_parquet,
    to_polars,
)

__all__ = [
    "Adapter",
    "Source",
    "Pipeline",
    "read",
    "read_par",
    "read_stream",
    "read_batches",
    "iter_record_batches",
    "register_adapter",
    "resolve_engine",
    "RenameFields",
    "DropFields",
    "CastTypes",
    "col",
    "FilterRows",
    "FilterRowsAny",
    "FilterRowsAll",
    "FilterRowsNot",
    "collect",
    "to_arrow",
    "to_csv",
    "to_pandas",
    "to_parquet",
    "to_polars",
    "ParseError",
    "XmlError",
    "PlanError",
    "MergeError",
    "ParserError",
    "RypipeError",
]

ParseError = _rypipe.ParseError
XmlError = _rypipe.XmlError
PlanError = _rypipe.PlanError
MergeError = _rypipe.MergeError
ParserError = _rypipe.ParserError
resolve_engine = _rypipe.resolve_engine

# Map common extensions to adapter names. Adapters must register themselves
# under these names for auto-detection to work.
_EXTENSION_MAP: dict[str, str] = {}

# Registered adapters: name -> module/object with a compatible read() method.
_ADAPTERS: dict[str, Any] = {}


class RypipeError(RuntimeError):
    """Base exception for invalid rypipe API usage."""


_FORMAT_RE = re.compile(
    r"^\s*(\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB|KiB|MiB|GiB|TiB)?\s*$", re.I
)


def _parse_memory(value: int | str) -> int:
    """Convert a human-readable memory string to bytes."""
    if isinstance(value, int):
        return max(value, 1)

    match = _FORMAT_RE.match(value)
    if not match:
        raise RypipeError(
            f"invalid memory value {value!r}; use e.g. '128MiB' or 64000000"
        )

    amount = float(match.group(1))
    unit = (match.group(2) or "B").upper()
    multiplier = {
        "B": 1,
        "KB": 1_000,
        "MB": 1_000_000,
        "GB": 1_000_000_000,
        "TB": 1_000_000_000_000,
        "KIB": 1_024,
        "MIB": 1_024**2,
        "GIB": 1_024**3,
        "TIB": 1_024**4,
    }[unit]
    return max(int(amount * multiplier), 1)


def _guess_format(path: str | os.PathLike[str]) -> str:
    """Guess the adapter name from the file extension."""
    suffix = Path(path).suffix.lower()
    fmt = _EXTENSION_MAP.get(suffix)
    if fmt is None:
        raise RypipeError(
            f"cannot infer adapter from extension {suffix!r}; "
            "pass `format=` or `adapter=` explicitly, or install an adapter package"
        )
    return fmt


def register_adapter(
    name: str,
    adapter: Any,
    extensions: Iterable[str] | str | None = None,
) -> None:
    """Register a format adapter with rypipe.

    Adapter packages should call this on import. `adapter` must expose a
    `read(path, **kwargs)` method that returns a `pyarrow.Table`.

    Parameters
    ----------
    name:
        Adapter name used for `format=` lookups.
    adapter:
        Object (typically a module) with a `read(path, **kwargs)` method.
    extensions:
        Optional file extensions that map to this adapter (e.g. [".xml"]).
    """
    if not isinstance(name, str) or not name.strip():
        raise ValueError("adapter name must be a non-empty string")
    _validate_adapter(adapter)
    exts = [extensions] if isinstance(extensions, str) else list(extensions or ())
    if any(not isinstance(ext, str) or not ext.startswith(".") or len(ext) < 2 for ext in exts):
        raise ValueError("extensions must be non-empty file suffixes such as '.csv'")
    _ADAPTERS[name] = adapter
    _EXTENSION_MAP.update((ext.lower(), name) for ext in exts)


def _validate_adapter(adapter: Any) -> None:
    if not callable(getattr(adapter, "read", None)):
        raise TypeError("adapter must expose a callable read(path, **kwargs) method")


def _get_adapter(path, format, adapter):
    if adapter is None:
        fmt = format if format is not None else _guess_format(path)
        adapter = _ADAPTERS.get(fmt)
        if adapter is None:
            raise RypipeError(
                f"no adapter registered for {fmt!r}; import its adapter package before reading"
            )
    _validate_adapter(adapter)
    return adapter


def read(
    path: str | os.PathLike[str],
    *,
    format: str | None = None,
    adapter: Any | None = None,
    **kwargs: Any,
) -> Any:
    """Read a row-oriented file into a PyArrow table using a registered adapter.

    Parameters
    ----------
    path:
        Path to the input file.
    format:
        Registered adapter name. When omitted, inferred from the file extension.
    adapter:
        An adapter object with a `read(path, **kwargs)` method. Overrides
        `format` when provided.
    **kwargs:
        Options passed through to the adapter (e.g. `row_tag="Row"` for XML).

    Returns
    -------
    pyarrow.Table
        The parsed table.

    Raises
    ------
    RypipeError
        If no adapter is registered for the requested format.
    """
    adapter = _get_adapter(path, format, adapter)
    return _require_table(adapter.read(str(path), **kwargs))


def read_par(
    path: str | os.PathLike[str],
    *,
    chunks: int = 4,
    **kwargs: Any,
) -> Any:
    """Read a file in parallel using a registered adapter.

    This is a convenience wrapper that passes `chunks` through to the adapter's
    `read` method. The adapter decides how to interpret it.
    """
    return read(path, chunks=chunks, **kwargs)


def read_stream(
    path: str | os.PathLike[str],
    *,
    memory: int | str = "64MiB",
    **kwargs: Any,
) -> Any:
    """Pass a parsing memory budget to the registered adapter's reader.

    This is a convenience wrapper that passes `memory` through to the adapter's
    `read` method. The adapter decides how to interpret it.
    """
    return read(path, memory=memory, **kwargs)


def read_batches(
    path: str | os.PathLike[str],
    *,
    memory: int | str = "64MiB",
    batch_size: int | None = None,
    **kwargs: Any,
) -> Iterator[Any]:
    """Read a file and yield ``pyarrow.RecordBatch`` objects incrementally.

    Uses ``iter_record_batches``. Adapters with streaming support produce
    batches incrementally; other adapters warn and materialize first.

    Yields
    ------
    pyarrow.RecordBatch
        One batch at a time, sized by the adapter (or ``batch_size``).
    """
    yield from iter_record_batches(path, memory=memory, batch_size=batch_size, **kwargs)


def iter_record_batches(
    path: str | os.PathLike[str],
    *,
    format: str | None = None,
    adapter: Any | None = None,
    memory: int | str = "64MiB",
    batch_size: int | None = None,
    **kwargs: Any,
) -> Iterator[Any]:
    """Yield Arrow batches using the adapter's streaming reader when available.

    Without a streaming reader, this warns and materializes the full table.
    ``memory`` limits executor buffers, not input storage, Python overhead, or
    output retained by the consumer. Batch sizing depends on the adapter.

    Yields
    ------
    pyarrow.RecordBatch
        One batch at a time.

    Examples
    --------
    >>> import pyarrow.parquet as pq
    >>> writer = pq.ParquetWriter("out.parquet", schema)
    >>> for batch in rypipe.iter_record_batches("data.xml", format="crxml", memory="64MB", row_tag="Details"):
    ...     writer.write_batch(batch)
    >>> writer.close()
    """
    if batch_size is not None:
        batch_size = _positive_int(batch_size)
    adapter = _get_adapter(path, format, adapter)
    stream = getattr(adapter, "iter_record_batches", None)
    if callable(stream):
        import pyarrow as pa

        for batch in stream(str(path), memory=memory, batch_size=batch_size, **kwargs):
            if not isinstance(batch, pa.RecordBatch):
                raise TypeError(f"streaming reader must yield pyarrow.RecordBatch; got {type(batch).__name__}")
            yield batch
        return
    warnings.warn("adapter has no streaming reader; materializing the full table", RuntimeWarning, stacklevel=2)
    table = _require_table(adapter.read(str(path), **kwargs))
    yield from table.to_batches(max_chunksize=batch_size)
