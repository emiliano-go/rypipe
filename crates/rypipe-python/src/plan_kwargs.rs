//! Convert Python-style kwargs into a `rypipe_core::ExecutionPlan`.
//!
//! This mirrors crxml's `build_plan_from_kwargs` and intentionally preserves
//! the same error messages where tests depend on them.
//!
//! Filters may be flat leaf specs or arbitrarily nested boolean trees:
//!
//! * `{"field": "x", "op": "==", "value": "y"}`: constant equality
//! * `{"field_a": "a", "op": ">", "field_b": "b"}`: column comparison
//! * `{"and": [spec, ...]}`: conjunction (short-circuits on first failure)
//! * `{"or": [spec, ...]}`: disjunction (short-circuits on first success)
//! * `{"not": spec}`: negation

use pyo3::prelude::*;
use pyo3::types::PyDict;
use rypipe_core::{CompareOp, ExecutionPlan, FieldType, FilterPredicate};
use std::collections::HashMap;

use crate::PlanError;

/// Build an [`ExecutionPlan`] from the Python-facing keyword arguments shared
/// by all `read_to_columnar*` entry points.
#[allow(clippy::too_many_arguments)]
pub fn execution_plan_from_kwargs(
    field_mapping: Option<HashMap<String, String>>,
    drop_fields: Option<Vec<String>>,
    filter: Option<&Bound<'_, PyAny>>,
    field_types: Option<HashMap<String, String>>,
    dictionary_columns: Option<Vec<String>>,
    schema: Option<Vec<String>>,
    auto_dict: bool,
    auto_dict_threshold: Option<f64>,
    auto_dict_max_size: Option<usize>,
) -> PyResult<ExecutionPlan> {
    let mut plan = ExecutionPlan::new();

    if let Some(map) = field_mapping {
        plan.field_map = map.into_iter().collect();
    }

    if let Some(drop) = drop_fields {
        plan.drop_fields = drop.into_iter().collect();
    }

    if let Some(s) = schema {
        plan.schema_order = s;
    }

    plan.auto_dict = auto_dict;
    plan.dict_threshold = auto_dict_threshold;
    plan.dict_max_size = auto_dict_max_size;

    if let Some(ft) = field_types {
        for (name, type_str) in ft {
            let ft = FieldType::from_str(&type_str).ok_or_else(|| {
                let valid = "string, int64, float64, bool, dictionary, date32, timestamp";
                PlanError::new_err(format!(
                    "unknown field type '{type_str}' for '{name}'; valid types: {valid}"
                ))
            })?;
            plan.field_types.insert(name, ft);
        }
    }

    if let Some(dict) = dictionary_columns {
        plan.dictionary_columns = dict.into_iter().collect();
    }

    if let Some(f) = filter {
        plan.filter = Some(parse_filter_spec(f)?);
    }

    Ok(plan)
}

/// Parse one filter spec (leaf or compound) into a [`FilterPredicate`].
fn parse_filter_spec(spec: &Bound<'_, PyAny>) -> PyResult<FilterPredicate> {
    let dict = spec.cast::<PyDict>().map_err(|_| {
        let ty = spec
            .get_type()
            .name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        PlanError::new_err(format!("filter spec must be a dict, got {ty}"))
    })?;

    // Compound forms take precedence over leaves.
    if let Some(item) = dict.get_item("and")? {
        return combine_list(&item, FilterPredicate::all, "'and'");
    }
    if let Some(item) = dict.get_item("or")? {
        return combine_list(&item, FilterPredicate::any, "'or'");
    }
    if let Some(item) = dict.get_item("not")? {
        let inner = parse_filter_spec(&item)?;
        return Ok(FilterPredicate::not(inner));
    }

    parse_leaf_spec(dict)
}

/// Fold a list of sub-specs into an `And`/`Or` chain.
fn combine_list(
    item: &Bound<'_, PyAny>,
    combiner: fn(FilterPredicate, FilterPredicate) -> FilterPredicate,
    label: &str,
) -> PyResult<FilterPredicate> {
    let specs: Vec<Bound<'_, PyAny>> = item.extract().map_err(|_| {
        PlanError::new_err(format!("{label} filter expects a list of filter specs"))
    })?;
    let mut iter = specs.into_iter();
    let Some(first) = iter.next() else {
        return Err(PlanError::new_err(format!(
            "{label} filter requires at least one sub-filter"
        )));
    };
    let mut acc = parse_filter_spec(&first)?;
    for spec in iter {
        acc = combiner(acc, parse_filter_spec(&spec)?);
    }
    Ok(acc)
}

/// Parse a flat leaf spec: constant (`field`/`op`/`value`) or column
/// comparison (`field_a`/`op`/`field_b`). Error messages match the original
/// flat-kwarg implementation.
fn parse_leaf_spec(f: &Bound<'_, PyDict>) -> PyResult<FilterPredicate> {
    let op = f
        .get_item("op")?
        .ok_or_else(|| PlanError::new_err("filter must include 'op' key"))?
        .extract::<String>()?;

    // Column-to-column filter: field_a + op + field_b
    if f.contains("field_a")? && f.contains("field_b")? {
        let field_a: String = f.get_item("field_a")?.unwrap().extract()?;
        let field_b: String = f.get_item("field_b")?.unwrap().extract()?;
        let cop = CompareOp::from_str(&op).ok_or_else(|| {
            let valid = ">, <, >=, <=, ==, !=";
            PlanError::new_err(format!("unsupported compare op {op:?}; valid: {valid}"))
        })?;
        return Ok(FilterPredicate::Compare {
            field_a,
            op: cop,
            field_b,
        });
    }

    let field = f
        .get_item("field")?
        .ok_or_else(|| PlanError::new_err("filter must include 'field' key"))?
        .extract::<String>()?;
    let value = f
        .get_item("value")?
        .ok_or_else(|| PlanError::new_err("filter must include 'value' key"))?
        .extract::<String>()?;
    Ok(match op.as_str() {
        "!=" | "ne" => FilterPredicate::NotEqual { field, value },
        "==" | "eq" => FilterPredicate::Equal { field, value },
        "starts_with" => FilterPredicate::StartsWith { field, value },
        "ends_with" => FilterPredicate::EndsWith { field, value },
        other => {
            let cop = CompareOp::from_str(other).ok_or_else(|| {
                let valid = "==, eq, !=, ne, >, gt, <, lt, >=, ge, <=, le, starts_with, ends_with";
                PlanError::new_err(format!("unsupported filter op {other:?}; valid: {valid}"))
            })?;
            FilterPredicate::CompareLiteral { field, op: cop, value }
        }
    })
}
