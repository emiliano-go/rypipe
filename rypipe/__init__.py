"""rypipe: format-agnostic columnar engine for Python.

This package wraps the Rust extension `_rypipe` with a developer-friendly API
for reading row-oriented files into Apache Arrow tables.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Iterable

import _rypipe

__all__ = [
    "read",
    "read_par",
    "read_stream",
    "XmlError",
    "PlanError",
    "MergeError",
]

XmlError = _rypipe.XmlError
PlanError = _rypipe.PlanError
MergeError = _rypipe.MergeError

# Map common extensions to the format strings known by `_rypipe.read`.
_EXTENSION_MAP: dict[str, str] = {
    ".xml": "xml",
}


class RypipeError(RuntimeError):
    """Base exception for invalid rypipe API usage."""


_FORMAT_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB|KiB|MiB|GiB|TiB)?\s*$", re.I)


def _parse_memory(value: int | str) -> int:
    """Convert a human-readable memory string to bytes."""
    if isinstance(value, int):
        return max(value, 1)

    match = _FORMAT_RE.match(value)
    if not match:
        raise RypipeError(f"invalid memory value {value!r}; use e.g. '128MiB' or 64000000")

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
    """Guess the file format from its extension."""
    suffix = Path(path).suffix.lower()
    fmt = _EXTENSION_MAP.get(suffix)
    if fmt is None:
        raise RypipeError(
            f"cannot infer format from extension {suffix!r}; "
            "pass `format=` explicitly"
        )
    return fmt


def _format_options(
    fmt: str,
    kwargs: dict[str, Any],
) -> dict[str, Any]:
    """Extract parser-specific options from the remaining kwargs."""
    options: dict[str, Any] = {}
    if fmt == "xml":
        if "row_tag" in kwargs:
            options["row_tag"] = kwargs.pop("row_tag")
    return options


def _build_plan_kwargs(
    rename: dict[str, str] | None,
    drop: Iterable[str] | None,
    fields: dict[str, str] | None,
    dictionary: Iterable[str] | None,
    schema: Iterable[str] | None,
    auto_dict: bool,
) -> dict[str, Any]:
    """Map public kwargs to the `_rypipe.read` plan kwargs."""
    return {
        "field_mapping": rename,
        "drop_fields": list(drop) if drop is not None else None,
        "field_types": fields,
        "dictionary_columns": list(dictionary) if dictionary is not None else None,
        "schema": list(schema) if schema is not None else None,
        "auto_dict": auto_dict,
    }


def read(
    path: str | os.PathLike[str],
    *,
    format: str | None = None,
    mode: str = "par",
    chunks: int = 4,
    memory: int | str = "64MiB",
    rename: dict[str, str] | None = None,
    drop: Iterable[str] | None = None,
    fields: dict[str, str] | None = None,
    dictionary: Iterable[str] | None = None,
    schema: Iterable[str] | None = None,
    filter: dict[str, Any] | None = None,
    auto_dict: bool = False,
    use_mmap: bool = False,
    prefault: bool = False,
    **format_options: Any,
) -> Any:
    """Read a row-oriented file into a PyArrow table.

    Parameters
    ----------
    path:
        Path to the input file.
    format:
        Parser format (e.g. "xml"). When omitted, inferred from the file
        extension.
    mode:
        Execution mode: "sync", "multi", "par" (default), or "stream".
    chunks:
        Number of chunks for "multi" and "par" modes.
    memory:
        Memory budget for "stream" mode; accepts bytes (int) or a human string
        like "128MiB".
    rename:
        Map raw field names to output column names.
    drop:
        Field/column names to drop.
    fields:
        Type overrides per output column name; values are "string", "int64",
        "float64", "bool", or "dictionary".
    dictionary:
        Column names to dictionary-encode explicitly.
    schema:
        Desired output column order.
    filter:
        Row filter dict. Per-row: ``{"field": "x", "op": "==", "value": "v"}``.
        Compare: ``{"field_a": "a", "op": ">", "field_b": "b"}``.
    auto_dict:
        Automatically upgrade low-cardinality string columns to dictionary
        encoding.
    use_mmap:
        Memory-map the input when possible.
    prefault:
        Pre-fault mapped pages.
    **format_options:
        Parser-specific options such as ``row_tag="Row"`` for XML.

    Returns
    -------
    pyarrow.Table
        The parsed table.
    """
    fmt = format if format is not None else _guess_format(path)
    options = _format_options(fmt, dict(format_options))
    plan_kwargs = _build_plan_kwargs(rename, drop, fields, dictionary, schema, auto_dict)

    return _rypipe.read(
        str(path),
        fmt,
        format_options=options or None,
        mode=mode,
        num_chunks=chunks,
        memory=_parse_memory(memory),
        filter=filter,
        use_mmap=use_mmap,
        prefault=prefault,
        **plan_kwargs,
    )


def read_par(
    path: str | os.PathLike[str],
    *,
    chunks: int = 4,
    **kwargs: Any,
) -> Any:
    """Read a file in parallel; convenience wrapper around ``read(..., mode="par")``."""
    return read(path, mode="par", chunks=chunks, **kwargs)


def read_stream(
    path: str | os.PathLike[str],
    *,
    memory: int | str = "64MiB",
    **kwargs: Any,
) -> Any:
    """Read a file with bounded memory; wrapper around ``read(..., mode="stream")``."""
    return read(path, mode="stream", memory=memory, **kwargs)
