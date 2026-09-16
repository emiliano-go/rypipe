---
title: pandas
description: Load rypipe output into pandas DataFrames
---

# pandas { #pandas }

`to_pandas` is rypipe's primary Python sink. It converts a pipeline into
a pandas DataFrame, Arrow-backed by default.

## Setup { #setup }

```bash
pip install "crxml[all]"
```

The `all` extra includes pandas and pyarrow.

## Basic usage { #basic-usage }

```python
from crxml import CrystalXMLSource, CastTypes, to_pandas

df = to_pandas(
    CrystalXMLSource("report.xml", row_tag="Details")
    | CastTypes({"Amount": float})
)

df.groupby("Department")["Amount"].sum()
```

## Dtype backends { #dtype-backends }

By default, columns use Arrow-backed dtypes (`pd.ArrowDtype`), which
preserve nulls and avoid the object-dtype fallback of NumPy conversion.
Pass `dtype_backend="numpy"` for classic NumPy-backed columns:

```python
df = to_pandas(pipeline, dtype_backend="numpy")
```

## Chunked construction { #chunked-construction }

Two knobs control how the DataFrame is built:

- `chunksize=` splits the finished Arrow table into batches and
  concatenates DataFrames incrementally; useful when
  `table.to_pandas()` on the full table spikes memory.
- `memory=` (e.g. `"64MiB"`) uses the adapter's streaming reader via
  `iter_record_batches`, so parsing itself stays within the budget.

```python
df = to_pandas(
    CrystalXMLSource("big-report.xml", row_tag="Details"),
    memory="64MiB",
)
```

All output remains in memory; the budget bounds the parsing side only.

## Why this works { #why-this-works }

pandas ≥ 2.0 converts Arrow tables efficiently and can back columns with
Arrow memory directly. rypipe's parser already produces Arrow, so the
conversion is a columnar transfer rather than row-by-row Python object
materialization.
