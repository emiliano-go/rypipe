"""Terminal sinks for rypipe pipelines."""

from __future__ import annotations

import csv
import warnings
from pathlib import Path
from typing import Iterable, Union


def to_dataframe(
    pipeline: Iterable[dict],
    chunksize: int | None = None,
    dtype_backend: str = "pyarrow",
) -> "pd.DataFrame":
    return to_pandas(pipeline, chunksize=chunksize, dtype_backend=dtype_backend)


def to_pandas(
    pipeline: Iterable[dict],
    chunksize: int | None = None,
    dtype_backend: str = "pyarrow",
) -> "pd.DataFrame":
    import pandas as pd

    types_mapper = pd.ArrowDtype if dtype_backend == "pyarrow" else None
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


def to_polars(pipeline: Iterable[dict]):
    """Return a Polars DataFrame from a pipeline or source."""
    import polars as pl

    return pl.from_arrow(to_arrow(pipeline))


def to_parquet(pipeline: Iterable[dict], path: Union[str, Path], **kwargs):
    """Write a pipeline or source to Parquet."""
    import pyarrow.parquet as pq

    pq.write_table(to_arrow(pipeline), str(path), **kwargs)


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


def collect(pipeline: Iterable[dict]) -> list[dict]:
    """Collect a pipeline or source into a list of dicts."""
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
