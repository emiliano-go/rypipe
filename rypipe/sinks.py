"""Terminal sinks for rypipe pipelines."""

from __future__ import annotations

import csv
import warnings
from pathlib import Path
from typing import Any, Iterable, Union


def to_pandas(
    pipeline: Iterable[dict],
    chunksize: int | None = None,
    memory: int | str | None = None,
    dtype_backend: str = "pyarrow",
    **kwargs: Any,
) -> "pd.DataFrame":
    """Convert a pipeline of records into a pandas DataFrame.

    Parameters
    ----------
    pipeline:
        A Source, Pipeline, or iterable of dicts.
    chunksize:
        Number of rows per intermediate DataFrame (chunks construction only,
        not parsing).  Use ``memory`` for true bounded-memory streaming.
    memory:
        Memory budget per parsing chunk (e.g. ``"64MiB"``).  When provided,
        batches are produced via ``iter_record_batches`` and each batch is
        converted to a DataFrame incrementally.  Peak memory is bounded by
        ``memory`` + one batch.
    dtype_backend:
        ``"pyarrow"`` (default) for Arrow-backed dtypes, ``"numpy"`` for
        NumPy-backed dtypes.
    **kwargs:
        Forwarded to ``iter_record_batches`` (e.g. ``threads=16``).
    """
    import pandas as pd

    types_mapper = pd.ArrowDtype if dtype_backend == "pyarrow" else None

    # True streaming path: use iter_record_batches for bounded memory
    if memory is not None and hasattr(pipeline, "iter_record_batches"):
        chunks = []
        for batch in pipeline.iter_record_batches(memory=memory, **kwargs):
            chunks.append(batch.to_pandas(types_mapper=types_mapper))
        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

    # Existing chunksize path (chunks DataFrame construction only)
    if chunksize is None:
        if hasattr(pipeline, "_to_arrow"):
            table = pipeline._to_arrow()
            if table is not None:
                return table.to_pandas(types_mapper=types_mapper)
        if hasattr(pipeline, "_iter_batches"):
            chunks = [
                pd.DataFrame.from_records(batch) for batch in pipeline._iter_batches()
            ]
            return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        return pd.DataFrame.from_records(iter(pipeline))
    chunks = []
    batch = []
    for rec in pipeline:
        batch.append(rec)
        if len(batch) >= chunksize:
            chunks.append(pd.DataFrame.from_records(batch))
            batch = []
    if batch:
        chunks.append(pd.DataFrame.from_records(batch))
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def to_arrow(pipeline: Iterable[dict]):
    """Return a ``pyarrow.Table`` from a pipeline or source."""
    if hasattr(pipeline, "_to_arrow"):
        table = pipeline._to_arrow()
        if table is not None:
            return table
    if hasattr(pipeline, "to_arrow"):
        return pipeline.to_arrow()
    import pyarrow as pa

    return pa.Table.from_pylist(list(pipeline))


def to_polars(
    pipeline: Iterable[dict],
    memory: int | str | None = None,
    **kwargs: Any,
):
    """Return a Polars DataFrame from a pipeline or source.

    Parameters
    ----------
    pipeline:
        A Source, Pipeline, or iterable of dicts.
    memory:
        Memory budget per parsing chunk (e.g. ``"64MiB"``).  When provided,
        batches are produced via ``iter_record_batches`` and each batch is
        converted to a DataFrame incrementally.  Peak memory is bounded.
    **kwargs:
        Forwarded to ``iter_record_batches`` (e.g. ``threads=16``).
    """
    import polars as pl

    if memory is not None and hasattr(pipeline, "iter_record_batches"):
        chunks = []
        for batch in pipeline.iter_record_batches(memory=memory, **kwargs):
            chunks.append(pl.from_arrow(batch))
        return pl.concat(chunks) if chunks else pl.DataFrame()
    return pl.from_arrow(to_arrow(pipeline))


def to_parquet(
    pipeline: Iterable[dict],
    path: Union[str, Path],
    memory: int | str | None = None,
    **kwargs: Any,
) -> None:
    """Write a pipeline or source to Parquet.

    Parameters
    ----------
    pipeline:
        A Source, Pipeline, or iterable of dicts.
    path:
        Output file path.
    memory:
        Memory budget per parsing chunk (e.g. ``"64MiB"``).  When provided,
        batches are produced via ``iter_record_batches`` and written
        incrementally via ``ParquetWriter``.  Peak memory is bounded.
    **kwargs:
        Forwarded to ``pyarrow.parquet.ParquetWriter`` (e.g.
        ``compression="zstd"``) or to ``iter_record_batches`` (e.g.
        ``threads=16``).
    """
    import pyarrow.parquet as pq

    # Separate ParquetWriter kwargs from iter_record_batches kwargs
    parquet_kwargs: dict[str, Any] = {}
    iter_kwargs: dict[str, Any] = {}
    parquet_keys = {
        "compression", "compression_level", "row_group_size",
        "use_dictionary", "write_statistics", "version",
    }
    for k, v in kwargs.items():
        if k in parquet_keys:
            parquet_kwargs[k] = v
        else:
            iter_kwargs[k] = v

    if memory is not None and hasattr(pipeline, "iter_record_batches"):
        writer = None
        try:
            for batch in pipeline.iter_record_batches(memory=memory, **iter_kwargs):
                if writer is None:
                    writer = pq.ParquetWriter(str(path), batch.schema, **parquet_kwargs)
                writer.write_batch(batch)
        finally:
            if writer is not None:
                writer.close()
        return
    pq.write_table(to_arrow(pipeline), str(path), **parquet_kwargs)


def to_csv(
    pipeline: Iterable[dict],
    path: Union[str, Path],
    encoding: str = "utf-8",
    delimiter: str = ",",
    fieldnames: list[str] | None = None,
) -> None:
    """Stream records to CSV."""
    path = Path(path)
    stream = iter(pipeline)
    try:
        first = next(stream)
    except StopIteration:
        path.write_text("", encoding=encoding)
        return
    if fieldnames is None:
        fieldnames = [*first]
    known = set(fieldnames)
    warned: set[str] = set()
    with open(path, "w", encoding=encoding, newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            delimiter=delimiter,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerow(first)
        for record in stream:
            fresh = {k for k in record if k not in known} - warned
            if fresh:
                warned |= fresh
                warnings.warn(
                    f"to_csv: field(s) {sorted(fresh)!r} are not in the CSV "
                    f"header and will be omitted; pass fieldnames= to "
                    f"include them",
                    UserWarning,
                    stacklevel=2,
                )
            writer.writerow(record)


def collect(
    pipeline: Iterable[dict],
    memory: int | str | None = None,
    **kwargs: Any,
) -> list[dict]:
    """Collect a pipeline or source into a list of dicts.

    Parameters
    ----------
    pipeline:
        A Source, Pipeline, or iterable of dicts.
    memory:
        Memory budget per parsing chunk (e.g. ``"64MiB"``).  When provided,
        batches are produced via ``iter_record_batches`` and collected
        incrementally.  Peak memory is bounded.
    **kwargs:
        Forwarded to ``iter_record_batches`` (e.g. ``threads=16``).
    """
    if memory is not None and hasattr(pipeline, "iter_record_batches"):
        rows = []
        for batch in pipeline.iter_record_batches(memory=memory, **kwargs):
            rows.extend(batch.to_pylist())
        return rows
    if hasattr(pipeline, "_to_arrow"):
        table = pipeline._to_arrow()
        if table is not None:
            return table.to_pylist()
    if hasattr(pipeline, "_iter_batches"):
        rows = []
        for batch in pipeline._iter_batches():
            rows.extend(batch)
        return rows
    return list(pipeline)
