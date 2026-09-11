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
use rypipe_core::{CompareOp, ExecutionPlan, FieldType, FilterPredicate, RegexSpec};
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
    strict_types: bool,
    max_split_chunks: Option<usize>,
    observer: Option<&Bound<'_, PyAny>>,
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

    if let Some(t) = auto_dict_threshold {
        if !t.is_finite() || t < 0.0 || t > 1.0 {
            return Err(crate::PlanError::new_err(format!(
                "auto_dict_threshold must be between 0.0 and 1.0, got {t}"
            )));
        }
    }
    plan.dict_threshold = auto_dict_threshold;

    if let Some(m) = auto_dict_max_size {
        if m == 0 {
            return Err(crate::PlanError::new_err(
                "auto_dict_max_size must be >= 1",
            ));
        }
    }
    plan.dict_max_size = auto_dict_max_size;

    plan.strict_types = strict_types;

    if let Some(m) = max_split_chunks {
        if m == 0 {
            return Err(crate::PlanError::new_err(
                "max_split_chunks must be >= 1",
            ));
        }
    }
    plan.max_split_chunks = max_split_chunks;

    if let Some(obs) = observer {
        let dict = obs.cast::<pyo3::types::PyDict>().map_err(|_| {
            crate::PlanError::new_err("observer must be a dict like {\"on_row_rejected\": fn}")
        })?;
        plan.observer = Some(crate::PyObserver::from_dict(dict)?);
    }

    if let Some(ft) = field_types {
        for (name, type_str) in ft {
            let ft = type_str.parse::<FieldType>().map_err(|e| {
                PlanError::new_err(format!(
                    "unknown field type '{type_str}' for '{name}'; {e}"
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

/// Maximum nesting depth for filter specs to prevent stack overflow.
const MAX_PREDICATE_DEPTH: usize = 128;

/// Parse one filter spec (leaf or compound) into a [`FilterPredicate`].
fn parse_filter_spec(spec: &Bound<'_, PyAny>) -> PyResult<FilterPredicate> {
    parse_filter_spec_depth(spec, 0)
}

fn parse_filter_spec_depth(spec: &Bound<'_, PyAny>, depth: usize) -> PyResult<FilterPredicate> {
    if depth >= MAX_PREDICATE_DEPTH {
        return Err(PlanError::new_err(format!(
            "filter spec nesting exceeds maximum depth of {MAX_PREDICATE_DEPTH}"
        )));
    }

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
        return combine_list(&item, FilterPredicate::all, "'and'", depth);
    }
    if let Some(item) = dict.get_item("or")? {
        return combine_list(&item, FilterPredicate::any, "'or'", depth);
    }
    if let Some(item) = dict.get_item("not")? {
        let inner = parse_filter_spec_depth(&item, depth + 1)?;
        return Ok(FilterPredicate::not(inner));
    }

    parse_leaf_spec(dict)
}

/// Fold a list of sub-specs into an `And`/`Or` chain.
fn combine_list(
    item: &Bound<'_, PyAny>,
    combiner: fn(FilterPredicate, FilterPredicate) -> FilterPredicate,
    label: &str,
    depth: usize,
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
    let mut acc = parse_filter_spec_depth(&first, depth + 1)?;
    for spec in iter {
        acc = combiner(acc, parse_filter_spec_depth(&spec, depth + 1)?);
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

    // Always-true / always-false
    if f.contains("always")? {
        let val: bool = f
            .get_item("always")?
            .ok_or_else(|| PlanError::new_err("filter 'always' key missing"))?
            .extract()?;
        return Ok(FilterPredicate::Always(val));
    }

    // Not-field (truthiness negation)
    if f.contains("not_field")? {
        let field: String = f
            .get_item("not_field")?
            .ok_or_else(|| PlanError::new_err("filter 'not_field' key missing"))?
            .extract()?;
        return Ok(FilterPredicate::NotField { field });
    }

    // Column-to-column filter: field_a + op + field_b
    if f.contains("field_a")? && f.contains("field_b")? {
        let field_a: String = f
            .get_item("field_a")?
            .ok_or_else(|| PlanError::new_err("filter 'field_a' key missing"))?
            .extract()?;
        let field_b: String = f
            .get_item("field_b")?
            .ok_or_else(|| PlanError::new_err("filter 'field_b' key missing"))?
            .extract()?;
        let cop = op.parse::<CompareOp>().map_err(|_| {
            let valid = ">, <, >=, <=, ==, !=";
            PlanError::new_err(format!("unsupported compare op {op:?}; valid: {valid}"))
        })?;
        return Ok(FilterPredicate::Compare {
            field_a,
            op: cop,
            field_b,
        });
    }

    // Replace: field + old + new + cmp_op + value
    if f.contains("old")? && f.contains("new")? {
        let field: String = f
            .get_item("field")?
            .ok_or_else(|| PlanError::new_err("replace filter 'field' key missing"))?
            .extract()?;
        let old: String = f
            .get_item("old")?
            .ok_or_else(|| PlanError::new_err("replace filter 'old' key missing"))?
            .extract()?;
        let new: String = f
            .get_item("new")?
            .ok_or_else(|| PlanError::new_err("replace filter 'new' key missing"))?
            .extract()?;
        let value = f
            .get_item("value")?
            .ok_or_else(|| PlanError::new_err("replace filter must include 'value' key"))?
            .extract::<String>()?;
        let cmp_op_str = if let Some(cmp) = f.get_item("cmp_op")? {
            cmp.extract::<String>()?
        } else {
            op.clone()
        };
        let cop = cmp_op_str.parse::<CompareOp>().map_err(|_| {
            let valid = "==, eq, !=, ne, >, gt, <, lt, >=, ge, <=, le";
            PlanError::new_err(format!(
                "unsupported compare op {cmp_op_str:?}; valid: {valid}"
            ))
        })?;
        return Ok(FilterPredicate::Replace {
            field,
            old,
            new,
            op: cop,
            value,
        });
    }

    // Collection membership: field + op + values
    if f.contains("values")? {
        let field: String = f
            .get_item("field")?
            .ok_or_else(|| PlanError::new_err("filter 'field' key missing"))?
            .extract()?;
        let values_py = f
            .get_item("values")?
            .ok_or_else(|| PlanError::new_err("filter 'values' key missing"))?;
        let values: Vec<String> = values_py.extract()?;
        if values.len() > 100_000 {
            return Err(PlanError::new_err(format!(
                "values list too large ({} elements, max 100,000)",
                values.len()
            )));
        }
        return Ok(match op.as_str() {
            "in" => FilterPredicate::In { field, values },
            "not_in" => FilterPredicate::NotIn { field, values },
            _ => {
                let valid = "in, not_in";
                return Err(PlanError::new_err(format!(
                    "unsupported collection op {op:?}; valid: {valid}"
                )));
            }
        });
    }

    // Null check: field + op="is_null"
    if op == "is_null" {
        let field = f
            .get_item("field")?
            .ok_or_else(|| PlanError::new_err("is_null filter must include 'field' key"))?
            .extract::<String>()?;
        return Ok(FilterPredicate::IsNull { field });
    }

    // Regex match: field + op="regex" + value (pattern)
    if op == "regex" {
        let field = f
            .get_item("field")?
            .ok_or_else(|| PlanError::new_err("regex filter must include 'field' key"))?
            .extract::<String>()?;
        let pattern = f
            .get_item("value")?
            .ok_or_else(|| PlanError::new_err("regex filter must include 'value' key"))?
            .extract::<String>()?;
        let re = RegexSpec::new(pattern)
            .map_err(|e| PlanError::new_err(format!("invalid regex in filter: {e}")))?;
        return Ok(FilterPredicate::Regex { field, re });
    }

    // Type check: field + op="is_type" + value (type name)
    if op == "is_type" {
        let field = f
            .get_item("field")?
            .ok_or_else(|| PlanError::new_err("is_type filter must include 'field' key"))?
            .extract::<String>()?;
        let type_str = f
            .get_item("value")?
            .ok_or_else(|| PlanError::new_err("is_type filter must include 'value' key"))?
            .extract::<String>()?;
        let field_type = type_str.parse::<FieldType>().map_err(|e| {
            PlanError::new_err(format!(
                "unknown field type '{type_str}' in is_type filter; {e}"
            ))
        })?;
        return Ok(FilterPredicate::IsType { field, field_type });
    }

    // Constant filter: field + op + value
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
        "contains" => FilterPredicate::Contains { field, value },
        "strip" | "lstrip" | "rstrip" | "lower" | "upper" => {
            let cmp_op_str = if let Some(cmp) = f.get_item("cmp_op")? {
                cmp.extract::<String>()?
            } else {
                "=".to_string()
            };
            let cop = cmp_op_str.parse::<CompareOp>().map_err(|_| {
                let valid = "==, eq, !=, ne, >, gt, <, lt, >=, ge, <=, le";
                PlanError::new_err(format!(
                    "unsupported compare op {cmp_op_str:?}; valid: {valid}"
                ))
            })?;
            match op.as_str() {
                "strip" | "lstrip" | "rstrip" => FilterPredicate::Strip {
                    field,
                    op: cop,
                    value,
                },
                "lower" => FilterPredicate::Lower {
                    field,
                    op: cop,
                    value,
                },
                "upper" => FilterPredicate::Upper {
                    field,
                    op: cop,
                    value,
                },
                _ => {
                    return Err(PlanError::new_err(format!(
                        "unsupported string transform op {op:?}"
                    )));
                }
            }
        }
        "length" => {
            let cmp_op_str = if let Some(cmp) = f.get_item("cmp_op")? {
                cmp.extract::<String>()?
            } else {
                ">".to_string()
            };
            let cop = cmp_op_str.parse::<CompareOp>().map_err(|_| {
                let valid = "==, eq, !=, ne, >, gt, <, lt, >=, ge, <=, le";
                PlanError::new_err(format!(
                    "unsupported compare op {cmp_op_str:?}; valid: {valid}"
                ))
            })?;
            FilterPredicate::Length {
                field,
                op: cop,
                value,
            }
        }
        other => {
            let cop = other.parse::<CompareOp>().map_err(|_| {
                let valid = "==, eq, !=, ne, >, gt, <, lt, >=, ge, <=, le, starts_with, ends_with, contains, strip, lower, upper, length, is_null, is_type, regex";
                PlanError::new_err(format!("unsupported filter op {other:?}; valid: {valid}"))
            })?;
            FilterPredicate::CompareLiteral {
                field,
                op: cop,
                value,
            }
        }
    })
}
