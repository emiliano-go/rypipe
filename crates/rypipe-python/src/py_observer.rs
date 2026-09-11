//! Python-callable row observer: wraps a dict of Python callables as a
//! `rypipe_core::RowObserver`.
//!
//! Hook errors are printed (unraisable) and swallowed: a broken observer must
//! never abort a parse. Field-level hooks take the GIL per call, so prefer
//! row-level hooks from Python.
//!
//! **Performance warning**: when used with parallel or parallel_streaming
//! executors, per-field hooks (`on_put_field`) acquire the GIL from every
//! worker thread on every field of every row. This serializes all Python
//! execution and can turn a parallel parse into a GIL-thrashed one. Always
//! prefer `on_row_accepted`/`on_row_rejected` for parallel workloads.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use rypipe_core::{RowObserver, Value};

/// The five recognized hook keys in an observer dict.
pub const HOOK_KEYS: [&str; 5] = [
    "on_begin_row",
    "on_put_field",
    "on_row_accepted",
    "on_row_rejected",
    "on_chunk_finished",
];

/// Convert a core `Value` to a Python object: Str→str, Int64→int,
/// Float64→float, Bool→bool, Date32/Timestamp→int, Null→None.
fn value_to_pyobject(py: Python<'_>, value: &Value<'_>) -> Py<PyAny> {
    match value {
        Value::Str(s) => s.as_ref().into_pyobject(py).unwrap().unbind().into(),
        Value::Int64(i) => i.into_pyobject(py).unwrap().unbind().into(),
        Value::Float64(f) => f.into_pyobject(py).unwrap().unbind().into(),
        Value::Bool(b) => b.into_pyobject(py).unwrap().to_owned().unbind().into(),
        Value::Date32(d) => d.into_pyobject(py).unwrap().unbind().into(),
        Value::Timestamp(t) => t.into_pyobject(py).unwrap().unbind().into(),
        Value::Null => py.None(),
    }
}

/// Row observer backed by Python callables. Slots not present in the dict
/// stay `None` and cost one branch per event.
pub struct PyObserver {
    on_begin_row: Option<Py<PyAny>>,
    on_put_field: Option<Py<PyAny>>,
    on_row_accepted: Option<Py<PyAny>>,
    on_row_rejected: Option<Py<PyAny>>,
    on_chunk_finished: Option<Py<PyAny>>,
}

impl PyObserver {
    /// Build from a dict `{"on_row_rejected": fn, ...}`. Unknown keys are a
    /// `PlanError`; non-callable values are a `PlanError`.
    pub fn from_dict(dict: &Bound<'_, PyDict>) -> PyResult<std::sync::Arc<Self>> {
        let mut obs = Self {
            on_begin_row: None,
            on_put_field: None,
            on_row_accepted: None,
            on_row_rejected: None,
            on_chunk_finished: None,
        };
        for (key, val) in dict.iter() {
            let key: String = key
                .extract()
                .map_err(|_| crate::PlanError::new_err("observer keys must be strings"))?;
            if !val.is_callable() {
                return Err(crate::PlanError::new_err(format!(
                    "observer hook {key:?} must be callable"
                )));
            }
            let slot = match key.as_str() {
                "on_begin_row" => &mut obs.on_begin_row,
                "on_put_field" => &mut obs.on_put_field,
                "on_row_accepted" => &mut obs.on_row_accepted,
                "on_row_rejected" => &mut obs.on_row_rejected,
                "on_chunk_finished" => &mut obs.on_chunk_finished,
                other => {
                    return Err(crate::PlanError::new_err(format!(
                        "unknown observer hook {other:?}; valid hooks: {HOOK_KEYS:?}"
                    )))
                }
            };
            *slot = Some(val.unbind());
        }
        Ok(std::sync::Arc::new(obs))
    }
}

/// Invoke `hook` with `args`, printing and swallowing any exception.
fn call(hook: &Option<Py<PyAny>>, args: impl FnOnce(Python<'_>) -> PyResult<Py<PyTuple>>) {
    let Some(cb) = hook else { return };
    Python::attach(|py| {
        let result = args(py).and_then(|a| cb.call1(py, a));
        if let Err(err) = result {
            err.print(py);
        }
    });
}

impl RowObserver for PyObserver {
    fn on_begin_row(&self, row_index: usize) {
        call(&self.on_begin_row, |py| {
            (row_index,).into_pyobject(py).map(|t| t.unbind())
        });
    }

    fn on_put_field(&self, row_index: usize, resolved_name: &str, slot: usize, value: &Value<'_>) {
        call(&self.on_put_field, |py| {
            let v = value_to_pyobject(py, value);
            (row_index, resolved_name, slot, v)
                .into_pyobject(py)
                .map(|t| t.unbind())
        });
    }

    fn on_row_accepted(&self, row_index: usize) {
        call(&self.on_row_accepted, |py| {
            (row_index,).into_pyobject(py).map(|t| t.unbind())
        });
    }

    fn on_row_rejected(&self, row_index: usize) {
        call(&self.on_row_rejected, |py| {
            (row_index,).into_pyobject(py).map(|t| t.unbind())
        });
    }

    fn on_chunk_finished(&self, total: usize, accepted: usize, rejected: usize) {
        call(&self.on_chunk_finished, |py| {
            (total, accepted, rejected)
                .into_pyobject(py)
                .map(|t| t.unbind())
        });
    }
}
