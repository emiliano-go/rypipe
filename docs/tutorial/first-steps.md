# First Steps { #first-steps }

By the end of this page you will read `report.xml` into a DataFrame,
iterate over rows, and chain your first pipeline stage. If you have not
installed yet:

```bash
pip install "crxml[pandas]"
```

[Download report.xml](../examples/report.xml) and save it next to your
script. It looks like this:

```xml
<Report Title="Sales Report">
  <Group Name="East">
    <Details>
      <Field Name="Name"><Value>Alice Johnson</Value></Field>
      <Field Name="Department"><Value>Sales</Value></Field>
      <Field Name="Amount"><Value>15000.50</Value></Field>
      ...
    </Details>
    ...
```

Each `<Details>` element is one row, with fields `Name`, `Department`,
`Amount`, `Status`, and `Date`.

## Step 1: Create a Source { #step-1-create-a-source }

```python
from crxml import CrystalXMLSource

source = CrystalXMLSource("report.xml", row_tag="Details")
```

A **Source** is a handle over one input file. `row_tag="Details"` tells
**crxml** which XML element is a row. Nothing is parsed yet; the Source
waits for you to ask for data.

You can peek at the column names with `.schema()`:

```python
print(source.schema())
# ['Name', 'Department', 'Amount', 'Status', 'Date']
```

## Step 2: Get a DataFrame { #step-2-get-a-dataframe }

```python
df = source.to_pandas()
print(df.head(3))
```

`to_pandas()` parses the file and returns a pandas DataFrame. The first call
parses the file; the result is cached, so later calls are instant.

You can also use:

* `.to_arrow()` for a `pyarrow.Table`
* `.to_pandas()` for a pandas DataFrame
* `.to_polars()` for a Polars DataFrame
* `.to_parquet("out.parquet")` to write a Parquet file

See [Sinks](sinks.md#sinks) for the details.

## Step 3: Iterate over rows { #step-3-iterate-over-rows }

Iterating a Source yields one Python `dict` per row:

```python
for row in source:
    print(row["Name"], row["Amount"])
```

```console
Alice Johnson 15000.50
Bob Smith 8500.00
Carol White 12000.75
...
```

All values come out of the file as strings. You will fix that in the next
step.

## Step 4: Chain a stage with | { #step-4-chain-stages-with }

Stages transform the data as it flows through a pipeline. Chain them on the
Source with the `|` operator, like a Unix pipe:

```python
from crxml import CastTypes, FilterRows, to_dataframe

df = to_dataframe(
    source
    | CastTypes({"Amount": float})
    | FilterRows(field="Status", op="==", value="Active")
)
print(df.head(3))
```

* `CastTypes` converts the `Amount` column from strings to floats.
* `FilterRows` keeps only rows where `Status` is `"Active"`.
* `to_dataframe()` runs the pipeline and collects the result.

Each `|` returns a new `Pipeline`. The original Source is not modified, so
you can reuse it as often as you like.

## Run it and check the output { #run-it }

Save this as `first_steps.py` next to `report.xml`:

```python
from crxml import CrystalXMLSource, CastTypes, FilterRows, to_dataframe

source = CrystalXMLSource("report.xml", row_tag="Details")
print("Columns:", source.schema())

df = to_dataframe(
    source
    | CastTypes({"Amount": float})
    | FilterRows(field="Status", op="==", value="Active")
)
print(df)
print("Active rows:", len(df))
```

Run it:

```console
$ python first_steps.py
Columns: ['Name', 'Department', 'Amount', 'Status', 'Date']
             Name   Department    Amount  Status        Date
0   Alice Johnson        Sales   15000.5  Active  2026-01-05
1       Bob Smith  Engineering    8500.0  Active  2026-01-06
2     Carol White    Marketing  12000.75  Active  2026-01-07
3       Eve Davis  Engineering   11200.0  Active  2026-01-09
4    Frank Miller        Sales   13500.0  Active  2026-01-10
5   Grace Wilson    Marketing    7600.5  Active  2026-01-11
6   Henry Taylor  Engineering   14200.0  Active  2026-01-12
7    Jack Thomas    Marketing   10100.0  Active  2026-01-14
8  Kate Martinez        Sales  16800.25  Active  2026-01-15
9     Leo Garcia  Engineering    9400.0  Active  2026-01-15
10  Mia Robinson    Marketing  11700.5  Active  2026-01-15
11  Olivia Lewis  Engineering  12600.75  Active  2026-01-15
Active rows: 12
```

## Recap, step by step { #recap }

1. `CrystalXMLSource("report.xml", row_tag="Details")` creates a Source over
   one file. Nothing is parsed yet.
2. `.to_pandas()` parses the file into a cached pandas DataFrame.
3. Iterating the Source yields rows as dicts, all values as strings.
4. `source | CastTypes(...) | FilterRows(...)` builds a Pipeline; the Source
   itself is untouched.
5. `to_dataframe(pipeline)` runs the pipeline and collects the result.

**Next:** [Pipeline](pipeline.md#pipeline), the `|` operator in depth.
