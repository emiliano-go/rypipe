---
title: Excel
description: Export rypipe output to Excel workbooks
---

# Excel { #excel }

Excel export goes through pandas: convert the pipeline with `to_pandas`,
then write with `DataFrame.to_excel`.

## Setup { #setup }

```bash
pip install "crxml[all]" openpyxl
```

`openpyxl` handles `.xlsx` files. Use `xlsxwriter` instead if you need
charts or heavy formatting.

## Basic usage { #basic-usage }

```python
from crxml import CrystalXMLSource, CastTypes, to_pandas

df = to_pandas(
    CrystalXMLSource("report.xml", row_tag="Details")
    | CastTypes({"Amount": float})
)

df.to_excel("sales.xlsx", sheet_name="Details", index=False)
```

## Multiple sheets { #multiple-sheets }

```python
import pandas as pd

summary = df.groupby("Department", as_index=False)["Amount"].sum()

with pd.ExcelWriter("sales.xlsx") as writer:
    df.to_excel(writer, sheet_name="Details", index=False)
    summary.to_excel(writer, sheet_name="Summary", index=False)
```

## Notes { #notes }

- Excel caps sheets at 1,048,576 rows. For larger outputs, write Parquet
  or CSV instead (see the Parquet integration).
- Arrow-backed dtypes are converted by pandas on export; `CastTypes` in
  the pipeline ensures numbers arrive as numbers, not text.

## Why this works { #why-this-works }

`to_excel` is a pandas feature, and `to_pandas` is rypipe's native sink.
The pipeline handles parsing and typing; pandas handles the workbook
serialization.
