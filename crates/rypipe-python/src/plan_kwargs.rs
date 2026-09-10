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
    plan.dict_threshold = auto_dict_threshold;
    plan.dict_max_size = auto_dict_max_size;
    plan.strict_types = strict_types;
    plan.max_split_chunks = max_split_chunks;

    if let Some(obs) = observer {
        let dict = obs.cast::<pyo3::types::PyDict>().map_err(|_| {
            crate::PlanError::new_err("observer must be a dict like {\"on_row_rejected\": fn}")
        })?;
        plan.observer = Some(crate::PyObserver::from_dict(dict)?);
    }

    if let Some(ft) = field_types {
        for (name, type_str) in ft {
            let ft = FieldType::from_str(&type_str).ok_or_else(|| {
                let valid = "string, int64, float64, bool, dictionary, date32, \
                             timestamp, timestamp[s|ms|us|ns], decimal128, decimal128(scale)";
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

    // Always-true / always-false
    if f.contains("always")? {
        let val: bool = f.get_item("always")?.unwrap().extract()?;
        return Ok(FilterPredicate::Always(val));
    }

    // Not-field (truthiness negation)
    if f.contains("not_field")? {
        let field: String = f.get_item("not_field")?.unwrap().extract()?;
        return Ok(FilterPredicate::NotField { field });
    }

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

    // Replace: field + old + new + cmp_op + value
    if f.contains("old")? && f.contains("new")? {
        let field: String = f.get_item("field")?.unwrap().extract()?;
        let old: String = f.get_item("old")?.unwrap().extract()?;
        let new: String = f.get_item("new")?.unwrap().extract()?;
        let value = f
            .get_item("value")?
            .ok_or_else(|| PlanError::new_err("replace filter must include 'value' key"))?
            .extract::<String>()?;
        let cmp_op_str = if let Some(cmp) = f.get_item("cmp_op")? {
            cmp.extract::<String>()?
        } else {
            op.clone()
        };
        let cop = CompareOp::from_str(&cmp_op_str).ok_or_else(|| {
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
        let field: String = f.get_item("field")?.unwrap().extract()?;
        let values_py = f.get_item("values")?.unwrap();
        let values: Vec<String> = values_py.extract()?;
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
        let field_type = FieldType::from_str(&type_str).ok_or_else(|| {
            let valid = "string, int64, float64, bool, dictionary, date32, \
                         timestamp, timestamp[s|ms|us|ns], decimal128, decimal128(scale)";
            PlanError::new_err(format!(
                "unknown field type '{type_str}' in is_type filter; valid types: {valid}"
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
            let cop = CompareOp::from_str(&cmp_op_str).ok_or_else(|| {
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
                _ => unreachable!(),
            }
        }
        "length" => {
            let cmp_op_str = if let Some(cmp) = f.get_item("cmp_op")? {
                cmp.extract::<String>()?
            } else {
                ">".to_string()
            };
            let cop = CompareOp::from_str(&cmp_op_str).ok_or_else(|| {
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
            let cop = CompareOp::from_str(other).ok_or_else(|| {
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
