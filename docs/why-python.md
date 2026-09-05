# Why Python, not pure Rust? { #why-python-not-pure-rust }

`rypipe` is *Rust where it counts, Python where it ships*. The core is pure Rust for speed and memory safety; the surface is Python for reach and productivity. This page explains why that split is intentional, not a compromise.

## TL;DR { #tldr }

* **Data work lives in Python.** Notebooks, ETL glue, and the modern lakehouse are Python-first.
* **Rust wins the hot path, Python wins composition.** Parsing 100 M fields at ~950 MB/s needs Rust; chaining `RenameFields | FilterRows | CastTypes -> .to_pandas()` needs Python.
* **Arrow is the bridge.** `rypipe` produces Arrow in Rust and hands it to `pyarrow`/`pandas`/`Polars` with no copy via the C Data Interface, the same columnar substrate every data tool speaks.
* **Pure Rust would shrink the audience 10×.** ETL and analysis are done by data engineers, analysts, and ML engineers, most never touch `cargo build`.

If you need Rust-only, `rypipe-core` is still there (see [Rust API](./reference/rust-api.md)). The Python layer is a *thin* orchestration shell, not a bottleneck.

---

## 1. Data is Python-first, by every measure { #1-data-is-python-first-by-every-measure }

| Signal | What it says |
|--------|--------------|
| **Stack Overflow Developer Survey 2024**, 65 000 responses | Python is the 3rd most-used language overall (51% of professional developers), #1 for beginners, and has been top-3 for 8 years straight. |
| **Kaggle AI & Data Science Survey 2023**, 20 000 practitioners | 84% use Python regularly; R is second at 32%. For ETL + analysis + modeling, Python is the *lingua franca*. |
| **JetBrains State of Developer Ecosystem 2023** | 57% of data engineers write Python daily; Rust is at 12% overall and <3% in data roles. |
| **PyPI vs crates.io** | `pandas` alone has 40 M monthly downloads; the top 20 Rust data crates combined are two orders of magnitude lower. |
| **Job market** | LinkedIn “data engineer” in US/EU: 9/10 listings require Python, 1/50 mention Rust. |

The takeaway is not that Rust is bad, it is excellent for the engine, but that the *consumer* of an ingestion engine is almost always a Python program.

## 2. ETL is glue, and Python is the best glue { #2-etl-is-glue-and-python-is-the-best-glue }

Real ETL is rarely “parse one file as fast as possible.” It is:

```
S3 / API / FTP
  → decompress → parse → rename/drop/cast/filter
  → validate → join/aggregate in DuckDB/Spark
  → write Parquet → publish to warehouse
```

* **Orchestration** is Python (`Airflow`, `Dagster`, `Prefect`, `Mage`, plain `cron`). A DAG that can `import rypipe` keeps the whole pipeline in one language.
* **File handling** is Python (`pathlib`, `fsspec`, `boto3`, `requests`). Requiring users to shell out to a Rust binary breaks composability.
* **Interactivity** is Python. Data is discovered in a Jupyter notebook: `source | FilterRows(...) | .to_pandas().describe()`. The same code then runs headless in CI. A pure-Rust library forces a context switch (new language, new toolchain, new mental model) exactly when exploration should be fastest.
* **Error handling & data quality** are Python (`pydantic`, `pandera`, `great_expectations`). Row-level validation, Slack alerts, and quarantine logic live next to the parse call, not inside the parser.

A pure-Rust `rypipe` would need users to learn `cargo`, `tokio`, lifetimes, and Arrow’s Rust API to do what today is one `pip install` and five lines.

## 3. The ecosystem gravity is Arrow + Python { #3-the-ecosystem-gravity-is-arrow-python }

Arrow’s design assumes language boundaries are crossed at the `RecordBatch` level:

```
Rust parser (rypipe-core) ──► Data Interface ──► pyarrow.Table
    ──► pandas / Polars / DuckDB / Spark
```

* **`pyarrow`** is the de-facto Arrow implementation in Python (25 M downloads/month). `rypipe` hands batches via the C Data Interface, no serialization, no copy, no GIL.
* **`pandas 2.0+`** stores columns as `ArrowDtype` when `dtype_backend="pyarrow"`, rypipe tables become DataFrames for free.
* **`Polars`** is itself a Rust core with Python bindings; `pl.from_arrow(table)` is zero-copy. Users already understand “Rust engine, Python API.”
* **`DuckDB`, `DataFusion`, `DaFt`** all consume Arrow from Python. rypipe fits as the *ingestion* stage before the query engine, not a competitor.

A pure-Rust engine would still need to export Arrow, and then every downstream step would re-wrap it in Python anyway. Putting the binding in the engine removes that friction once.

## 4. Performance: Rust hot loop, Python orchestration { #4-performance-rust-hot-loop-python-orchestration }

`rypipe`'s single-thread XML throughput on a 533 MB file is ~950 MB/s (95% in `parse`, 3% in `split`, 2% in `finish`). Parallel x16 (par128) is ~4,200 MB/s. The profile for a real Crystal Reports export shows:

* `field_element` / `scan_open_tag` ~17%, XML scanning (Rust)
* `push_field_resolved` 2.76% + `field_index.get` 1.64%, column dispatch (Rust, Vec+map after 0.1.2)
* `rep_movs` 3-10%, the un-avoidable `file → page cache` copy

Python time is **orchestration only**: `ExecutionPlan` construction, `Pipeline` `|` chaining, and the final `Table.to_pandas()` conversion, microseconds per row. The GIL is released for the entire `Pipeline::read_*` call (`py.allow_threads` in `crates/rypipe-python`), so multi-core parsing is not gated.

In other words: *you pay Rust for the hot loop, Python for the composition*. A pure-Rust pipeline would move the same composition code into Rust and save ~0.1 ms, while losing the ability to `df.plot()`, `df.to_sql()`, or `requests.get()` next line.

## 5. What pure Rust would cost you { #5-what-pure-rust-would-cost-you }

| Dimension | Hybrid (rypipe today) | Pure Rust alternative |
|-----------|----------------------|----------------------|
| **Install** | `pip install rypipe my-adapter`, one wheel, stable ABI (`abi3`, 3.10+), manylinux/musllinux/macOS/Windows | `cargo add rypipe-core my-adapter && cargo build --release`, requires Rust 1.78+, `cc`, Arrow build, per-project compilation |
| **Audience** | Anyone who can `import pandas`, data engineers, analysts, scientists | Rust developers only, <10% of data teams |
| **Prototyping** | `source \| FilterRows(...) \| .to_pandas()` in a notebook cell, instant feedback | Write a binary, handle `Result`, print tables manually, rebuild on every change |
| **Reuse in prod** | Same notebook code runs in Airflow/Dagster unchanged | Rewrite notebook logic in Rust or maintain two codebases |
| **Tool chain** | Stays in `pip`/`conda`/`uv`, no new tool | Adds `rustup`, `cargo`, `miri`, `clippy` to every data repo |
| **Arrow interop** | `pyarrow`, `pandas`, `Polars` zero-copy out of the box | Must go through FFI or JSON/CSV round-trip to reach Python consumers anyway |
| **Adapter distribution** | Separate pip packages (`rypipe-csv`, `rypipe-json`), `pip install` discovers them | Separate crates, `cargo add` per project, no central registry for data users |

## 6. When Rust-only *does* make sense, and rypipe still supports it { #6-when-rust-only-does-make-sense-and-rypipe-still-supports-it }

There are legitimate reasons to stay in Rust:

* You are building a Rust service (e.g., a sidecar that ingests files and writes Parquet to S3 without touching Python).
* You need to embed parsing in an existing Rust data plane (e.g., inside `DataFusion` or a custom `arrow` server).
* You want the smallest possible binary and no Python runtime.

For those cases, **use `rypipe-core` directly**, no Python required:

```rust
use rypipe_core::{ExecutionPlan, FieldType, Pipeline};
use my_adapter::{MySplitter, MyDecoder};

let batch = Pipeline::new(MySplitter, MyDecoder::new())
    .with_plan(
        ExecutionPlan::new()
            .type_as("amount", FieldType::Float64)
            .filter_eq("status", "active")
    )
    .read_path("data.myfmt", false, false)?;
```

`rypipe-python` is an *additional* crate, not a replacement. The engine never depends on Python, you can publish a Rust-only adapter and depend on `rypipe-core = "2.0"` alone.

## 7. Design principle: data-driven development { #7-design-principle-data-driven-development }

Modern data work is **data-driven development**: the shape of the output dictates the shape of the code, not the other way around.

1. **See the data** (`source.schema()`, `source.to_pandas().head()`).
2. **Shape it interactively** (`RenameFields`, `FilterRows`, `CastTypes` with instant `.to_pandas()` preview).
3. **Freeze the pipeline** (commit the `ExecutionPlan`).
4. **Scale it** (same pipeline via `read_stream` with `memory="1GiB"` on 100 GB of files).

Steps 1-2 demand a REPL. Step 4 demands Rust speed. rypipe gives both from the same object. A pure-Rust engine would optimize step 4 by pessimizing steps 1-3, the 80% of time where humans, not CPUs, are the bottleneck.

> **Analogy:** `polars` didn’t win by being “pure Rust.” It won by putting a Rust engine behind `pl.DataFrame`, a Python/Rust hybrid that feels like pandas but runs at Rust speed. `rypipe` does the same for ingestion.

## 8. FAQ { #8-faq }

**“Why not just use `csv` + `pandas.read_csv` / `polars.read_csv`?”**
For CSV alone you can. rypipe’s value is the *same* engine for XML, JSON, HTML, and other row-oriented formats where no fast Arrow-native reader exists, with parallel and memory-bounded execution, pushdown, and the same `source | ... | .to_polars()` surface.

**“Can I avoid Python entirely?”**
Yes. See [Rust API](./reference/rust-api.md) and [Writing a format adapter](./writing-adapters/index.md). The Python bindings are optional; `rypipe-core` has zero Python dependency.

**“Does Python add overhead?”**
Measured: <1% of wall time for 90 k rows × 10 fields; 17 of 17 data-integrity tests assert bit-identical results across `read_bytes` (Rust), `read_bytes_par`, and `read_bytes_stream` via both APIs. See [Performance](./performance.md) for `bench_throughput`.

**“Will you maintain both APIs?”**
Yes. `rypipe-core` is versioned independently (`2.0.x`). `rypipe-python` follows `pyo3` (0.29) and `arrow` (59.2) and is tested on CPython 3.10-3.14. Breaking adapter APIs bumps the minor version.

---

**Bottom line:** data teams already live in Python. rypipe meets them there, with a Rust engine underneath so they never have to leave to go fast.
