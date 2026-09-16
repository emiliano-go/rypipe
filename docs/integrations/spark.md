---
title: Apache Spark
description: Load rypipe output into PySpark DataFrames
---

# Apache Spark { #spark }

PySpark creates DataFrames from pandas DataFrames, and with Arrow
optimization enabled the transfer is columnar rather than row-by-row.

## Setup { #setup }

```bash
pip install "crxml[all]" pyspark
```

## Basic usage { #basic-usage }

```python
from crxml import CrystalXMLSource, CastTypes, to_pandas
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("rypipe")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .getOrCreate()
)

pdf = to_pandas(
    CrystalXMLSource("report.xml", row_tag="Details")
    | CastTypes({"Amount": float})
)

df = spark.createDataFrame(pdf)
df.groupBy("Department").sum("Amount").show()
```

With `arrow.pyspark.enabled`, `createDataFrame` serializes the pandas
DataFrame through Arrow. Since `to_pandas` defaults to Arrow-backed
dtypes, the data stays columnar from parser to Spark.

## SQL queries { #sql }

```python
df.createOrReplaceTempView("sales")
spark.sql("""
    SELECT Department, SUM(Amount) AS total
    FROM sales
    GROUP BY Department
""").show()
```

## When to reach for Spark { #when-to-reach-for-spark }

Spark adds distributed scheduling overhead; for single-machine workloads
rypipe plus DuckDB or Polars is usually faster. Use this integration when
the data must land in an existing Spark cluster, a Hive metastore, or a
Spark-managed table format.

## Why this works { #why-this-works }

PySpark's Arrow optimization uses the same Arrow columnar format rypipe
produces. The pandas DataFrame acts as the handoff point, and Arrow
memory moves into the Spark JVM without Python object serialization.
