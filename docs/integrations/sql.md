---
title: Direct SQL databases
description: Load rypipe output into PostgreSQL, SQLite, and other SQL databases
---

# Direct SQL databases { #direct-sql }

rypipe has no database connector of its own. It produces Arrow tables,
which you load into a SQL database with either **ADBC** (Arrow-native,
fastest) or **pandas + SQLAlchemy** (broadest database support).

## Setup { #setup }

```bash
# ADBC route (PostgreSQL example; also: adbc-driver-sqlite, adbc-driver-flightsql)
pip install "crxml[all]" adbc-driver-postgresql

# SQLAlchemy route
pip install "crxml[all]" sqlalchemy psycopg2-binary
```

## ADBC (recommended) { #adbc }

ADBC drivers ingest Arrow data in bulk without row-by-row inserts. Pass
the rypipe Arrow table to `adbc_ingest`:

```python
from crxml import CrystalXMLSource, CastTypes
import adbc_driver_postgresql.dbapi as dbapi

table = (
    CrystalXMLSource("report.xml", row_tag="Details")
    | CastTypes({"Amount": float})
).to_arrow()

with dbapi.connect("postgresql://localhost/mydb") as con, con.cursor() as cur:
    cur.adbc_ingest("sales", table, mode="create_append")
```

Available ADBC drivers include PostgreSQL, SQLite, Snowflake, BigQuery,
and Flight SQL. The `table` argument accepts any Arrow-compatible object,
so no pandas conversion happens at any point.

!!! tip

    All values come out of the parser as strings. Use `CastTypes` in the
    pipeline so database columns get real numeric types instead of
    `VARCHAR`.

## SQLAlchemy + pandas { #sqlalchemy }

For databases without an ADBC driver, convert to pandas and use
`DataFrame.to_sql`:

```python
from crxml import CrystalXMLSource, CastTypes, to_pandas
from sqlalchemy import create_engine

df = to_pandas(
    CrystalXMLSource("report.xml", row_tag="Details")
    | CastTypes({"Amount": float})
)

engine = create_engine("postgresql://localhost/mydb")
df.to_sql("sales", engine, if_exists="append", index=False)
```

## SQLite { #sqlite }

SQLite works through either route, ADBC:

```python
import adbc_driver_sqlite.dbapi as dbapi

with dbapi.connect("sales.db") as con, con.cursor() as cur:
    cur.adbc_ingest("sales", table, mode="create_append")
```

or the standard library via pandas:

```python
import sqlite3

with sqlite3.connect("sales.db") as con:
    df.to_sql("sales", con, if_exists="replace", index=False)
```

## Large inputs { #large-inputs }

For inputs that don't fit comfortably in memory, ingest batch by batch
using `iter_record_batches` with an ADBC connection:

```python
from crxml import CrystalXMLSource
import adbc_driver_postgresql.dbapi as dbapi

source = CrystalXMLSource("big-report.xml", row_tag="Details")

with dbapi.connect("postgresql://localhost/mydb") as con, con.cursor() as cur:
    for i, batch in enumerate(source.iter_record_batches(memory="64MiB")):
        cur.adbc_ingest(
            "sales",
            batch,
            mode="create" if i == 0 else "append",
        )
```

Each batch is bounded by the `memory` budget; the database accumulates
the full result.

## Why this works { #why-this-works }

rypipe produces Arrow tables. ADBC is an Arrow-native database API:
drivers consume columnar batches directly and translate them into the
database's bulk-load protocol (e.g. PostgreSQL `COPY`), avoiding the
per-row overhead of traditional drivers.
