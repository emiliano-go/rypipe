//! `rypipe-python`: PyO3 bindings and helper functions over the `rypipe-core`
//! columnar engine.
//!
//! This crate is intentionally format-agnostic. It exposes reusable building
//! blocks (plan construction, Arrow export, typed exceptions) so separate
//! adapter crates can build their own Python APIs on top of `rypipe-core`.
//!
//! Adapter crates (for XML, CSV, JSON, HTML, etc.) live in their own packages
//! and depend on `rypipe-core` plus `rypipe-python` for the Python boundary
//! helpers.

use pyo3::prelude::*;

mod export;
mod plan_kwargs;

pub use export::{
    record_batch_to_pyarrow, record_batches_to_pyarrow, record_batches_to_pyarrow_batches,
    record_batches_to_pyarrow_table,
};
pub use plan_kwargs::execution_plan_from_kwargs;

// Typed exceptions so callers can distinguish failure classes:
// `ParseError`: malformed/unparseable input (including invalid UTF-8).
// `PlanError`: invalid pushdown plan kwargs (bad ops, unknown types).
// `MergeError`: chunk-merge conflict (e.g. type mismatch across chunks).
pyo3::create_exception!(_rypipe, ParseError, pyo3::exceptions::PyException);
// Backward-compatible alias kept for consumers that previously caught
// `XmlError` from crxml-style adapters.
pyo3::create_exception!(_rypipe, XmlError, ParseError);
pyo3::create_exception!(_rypipe, PlanError, pyo3::exceptions::PyException);
pyo3::create_exception!(_rypipe, MergeError, pyo3::exceptions::PyException);
pyo3::create_exception!(_rypipe, ParserError, pyo3::exceptions::PyException);

/// Convert a `rypipe_core::Error` into an appropriate Python exception.
pub fn py_err_from_rypipe(err: rypipe_core::Error) -> PyErr {
    match err {
        rypipe_core::Error::Utf8(e) => ParseError::new_err(format!("invalid UTF-8: {e}")),
        rypipe_core::Error::Plan(msg) => PlanError::new_err(msg),
        rypipe_core::Error::Merge(msg) => MergeError::new_err(msg),
        rypipe_core::Error::Parser(msg) => ParserError::new_err(msg),
        rypipe_core::Error::Lifetime(msg) => ParserError::new_err(format!("lifetime error: {msg}")),
        rypipe_core::Error::Io(e) => pyo3::exceptions::PyIOError::new_err(e.to_string()),
        rypipe_core::Error::Arrow(e) => {
            pyo3::exceptions::PyException::new_err(format!("Arrow error: {e}"))
        }
    }
}

/// Resolve the best engine mode based on file characteristics and user options.
///
/// This is the Python-callable version of `rypipe_core::resolve_engine`.
/// Adapters can use this to implement `engine="auto"` selection.
///
/// Parameters
/// ----------
/// file_size : int
///     Size of the input file in bytes.
/// memory : str or int or None
///     Memory budget (e.g. "64MiB"). None = no budget (full table).
/// threads : int or None
///     Number of threads for parallel processing. None = 1 (single-threaded).
/// schema : list[str] or None
///     Explicit column names. Some = schema provided, None = discover.
/// has_parallel : bool
///     Whether the adapter has parallel support.
/// has_columnar : bool
///     Whether the adapter has columnar support.
///
/// Returns
/// -------
/// str
///     One of "columnar", "parallel", "stream", "parallel_streaming".
#[pyfunction]
#[pyo3(signature = (file_size, memory=None, threads=None, schema=None, has_parallel=true, has_columnar=true))]
fn resolve_engine<'py>(
    py: Python<'py>,
    file_size: u64,
    memory: Option<Bound<'py, pyo3::types::PyAny>>,
    threads: Option<usize>,
    schema: Option<Vec<String>>,
    has_parallel: bool,
    has_columnar: bool,
) -> PyResult<String> {
    // Parse memory parameter (string like "64MiB" or int bytes)
    let memory_bytes = if let Some(m) = memory {
        if let Ok(val) = m.extract::<u64>() {
            Some(val)
        } else if let Ok(s) = m.extract::<String>() {
            Some(parse_memory_string(&s)?)
        } else {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "memory must be a string (e.g. '64MiB') or int (bytes)",
            ));
        }
    } else {
        None
    };

    let config = rypipe_core::AutoConfig {
        file_size,
        memory: memory_bytes,
        threads,
        schema,
        has_parallel,
        has_columnar,
    };

    let mode = rypipe_core::resolve_engine(&config);
    Ok(mode.as_str().to_string())
}

/// Parse a memory string like "64MiB" or "1GB" into bytes.
fn parse_memory_string(s: &str) -> PyResult<u64> {
    let s = s.trim();

    // Find where the numeric part ends.
    // Walk from the start, past digits and at most one decimal point.
    let mut num_end = 0;
    let mut seen_dot = false;
    for (i, c) in s.char_indices() {
        if c == '.' && !seen_dot {
            seen_dot = true;
            num_end = i + 1;
        } else if c.is_ascii_digit() {
            num_end = i + 1;
        } else {
            break;
        }
    }

    let (num_str, unit_str) = if num_end > 0 {
        (&s[..num_end], s[num_end..].trim())
    } else {
        // No number found; treat as bare unit (e.g. "MB" means "1MB")
        ("1", s.trim())
    };

    let num: f64 = num_str.parse().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid memory value: {s:?}"))
    })?;

    let unit_upper = unit_str.to_uppercase();
    let multiplier: u64 = match unit_upper.as_str() {
        "" | "B" => 1,
        "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "TB" => 1_000_000_000_000,
        "KIB" => 1_024,
        "MIB" => 1_024 * 1_024,
        "GIB" => 1_024 * 1_024 * 1_024,
        "TIB" => 1_024 * 1_024 * 1_024 * 1_024,
        _ => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unknown memory unit: {unit_str:?}"
            )))
        }
    };

    Ok((num * multiplier as f64) as u64)
}

#[pymodule]
fn _rypipe(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("ParseError", m.py().get_type::<ParseError>())?;
    m.add("XmlError", m.py().get_type::<XmlError>())?;
    m.add("PlanError", m.py().get_type::<PlanError>())?;
    m.add("MergeError", m.py().get_type::<MergeError>())?;
    m.add_function(wrap_pyfunction!(resolve_engine, m)?)?;
    // Build provenance: the git SHA this .so was compiled from.
    m.add("__build_sha__", env!("RYPIPE_BUILD_SHA"))?;
    Ok(())
}
