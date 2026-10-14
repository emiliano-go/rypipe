---
title: Ecosystem overview
description: Where rypipe fits alongside DuckDB, Polars, pandas, and the rest of the Arrow ecosystem
---

# Where rypipe fits { #where-rypipe-fits }

rypipe does not replace DuckDB, Polars, pandas, Spark, or any other engine
listed in this section. Those are query and compute engines: they are built
for joins, aggregations, window functions, and SQL. What they are not is a
parser for arbitrary record-oriented formats. Before any of them can query
your data, the data has to exist as typed, columnar tables, and something
has to get it there.

rypipe is that something. It is a high-throughput ingestion engine: it takes
raw bytes in any format an adapter exists for (XML, JSON, CSV, HTML, custom
formats) and produces Arrow tables, in parallel, under an explicit memory
budget, at multi-GB/s rates.

The division of labor:

- **rypipe** turns bytes into typed columnar batches. Projection, casting,
  filtering, and dictionary encoding happen here, at parse time, pushed down
  into the decoder so unneeded data is never materialized.
- **Your engine of choice** takes it from there. DuckDB runs SQL over the
  table, Polars builds lazy plans on it, pandas does ad-hoc analysis, Spark
  distributes it across a cluster, Parquet and Delta Lake store it.

Because rypipe speaks Arrow natively, the handoff is not an export step with
serialization overhead. Every tool in this section consumes Arrow through
the Arrow C Data Interface or PyArrow, so passing a rypipe table onward is
passing a pointer, not copying data.

In short: rypipe gets the data in; the ecosystem takes it from there. The
pages below show the specifics for each tool.

## Roadmap { #roadmap }

| Page | What you will learn |
|------|---------------------|
| [Arrow ecosystem](./arrow.md) | The zero-copy handoff protocol; using rypipe output anywhere Arrow is accepted. |
| [DuckDB](./duckdb.md) | Registering and querying rypipe tables with SQL, no import step. |
| [Polars](./polars.md) | Loading rypipe output into eager and lazy Polars DataFrames. |
| [pandas](./pandas.md) | Loading rypipe output into pandas DataFrames. |
| [Parquet files](./parquet.md) | Writing rypipe output to Parquet files and partitioned datasets. |
| [Delta Lake](./delta-lake.md) | Appending rypipe output to Delta Lake tables. |
| [Apache Iceberg](./iceberg.md) | Appending rypipe output to Apache Iceberg tables. |
| [Apache Spark](./spark.md) | Loading rypipe output into PySpark DataFrames. |
| [Direct SQL databases](./sql.md) | Bulk-loading rypipe output into PostgreSQL, SQLite, and other SQL databases. |
| [Excel](./excel.md) | Exporting rypipe output to Excel workbooks. |
