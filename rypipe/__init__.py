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
    ).to_dataframe()

Adapters register themselves so the high-level ``read`` API also works::

    table = rypipe.read("data.myfmt", fields={"amount": "float64"})
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Iterable

import _rypipe

from .source import Adapter, Source
from .pipeline import Pipeline
from .stages import RenameFields, CastTypes, FilterRows, DropFields
from .sinks import (
    collect,
    to_arrow,
    to_csv,
    to_dataframe,
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
    "register_adapter",
    "RenameFields",
    "DropFields",
    "CastTypes",
    "FilterRows",
    "collect",
    "to_arrow",
    "to_csv",
    "to_dataframe",
    "to_pandas",
    "to_parquet",
    "to_polars",
    "ParseError",
    "XmlError",
    "PlanError",
    "MergeError",
    "RypipeError",
]

ParseError = _rypipe.ParseError
XmlError = _rypipe.XmlError
PlanError = _rypipe.PlanError
MergeError = _rypipe.MergeError

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
    extensions: Iterable[str] | None = None,
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
    _ADAPTERS[name] = adapter
    if extensions is not None:
        for ext in extensions:
            _EXTENSION_MAP[ext.lower()] = name


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
    if adapter is not None:
        return adapter.read(str(path), **kwargs)

    fmt = format if format is not None else _guess_format(path)
    adapter = _ADAPTERS.get(fmt)
    if adapter is None:
        raise RypipeError(
            f"no adapter registered for {fmt!r}; "
            f"install the corresponding adapter package (e.g. pip install rypipe-{fmt})"
        )
    return adapter.read(str(path), **kwargs)


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
    """Read a file with bounded memory using a registered adapter.

    This is a convenience wrapper that passes `memory` through to the adapter's
    `read` method. The adapter decides how to interpret it.
    """
    return read(path, memory=memory, **kwargs)
