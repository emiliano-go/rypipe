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

/// Convert a `rypipe_core::Error` into an appropriate Python exception.
pub fn py_err_from_rypipe(err: rypipe_core::Error) -> PyErr {
    match err {
        rypipe_core::Error::Utf8(e) => ParseError::new_err(format!("invalid UTF-8: {e}")),
        rypipe_core::Error::Plan(msg) => PlanError::new_err(msg),
        rypipe_core::Error::Merge(msg) => MergeError::new_err(msg),
        rypipe_core::Error::Io(e) => pyo3::exceptions::PyIOError::new_err(e.to_string()),
        rypipe_core::Error::Arrow(e) => {
            pyo3::exceptions::PyException::new_err(format!("Arrow error: {e}"))
        }
    }
}

#[pymodule]
fn _rypipe(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("ParseError", m.py().get_type::<ParseError>())?;
    m.add("XmlError", m.py().get_type::<XmlError>())?;
    m.add("PlanError", m.py().get_type::<PlanError>())?;
    m.add("MergeError", m.py().get_type::<MergeError>())?;
    // Build provenance: the git SHA this .so was compiled from.
    m.add("__build_sha__", env!("RYPIPE_BUILD_SHA"))?;
    Ok(())
}
