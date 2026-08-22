//! Convert Python-style kwargs into a `rypipe_core::ExecutionPlan`.
//!
//! This mirrors crxml's `build_plan_from_kwargs` and intentionally preserves
//! the same error messages where tests depend on them.

use pyo3::prelude::*;
use rypipe_core::{CompareOp, ExecutionPlan, FieldType, FilterPredicate};
use std::collections::HashMap;

use crate::PlanError;

/// Build an [`ExecutionPlan`] from the Python-facing keyword arguments shared
/// by all `read_to_columnar*` entry points.
pub fn execution_plan_from_kwargs(
    field_mapping: Option<HashMap<String, String>>,
    drop_fields: Option<Vec<String>>,
    filter: Option<HashMap<String, String>>,
    field_types: Option<HashMap<String, String>>,
    dictionary_columns: Option<Vec<String>>,
    schema: Option<Vec<String>>,
    auto_dict: bool,
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

    if let Some(ft) = field_types {
        for (name, type_str) in ft {
            let ft = FieldType::from_str(&type_str).ok_or_else(|| {
                let valid = "string, int64, float64, bool";
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
        let op = f
            .get("op")
            .ok_or_else(|| PlanError::new_err("filter must include 'op' key"))?
            .to_owned();

        // Column-to-column filter: field_a + op + field_b
        if f.contains_key("field_a") && f.contains_key("field_b") {
            let field_a = f.get("field_a").unwrap().to_owned();
            let field_b = f.get("field_b").unwrap().to_owned();
            let cop = CompareOp::from_str(&op).ok_or_else(|| {
                let valid = ">, <, >=, <=, ==, !=";
                PlanError::new_err(format!(
                    "unsupported compare op {op:?}; valid: {valid}"
                ))
            })?;
            plan.filter = Some(FilterPredicate::Compare {
                field_a,
                op: cop,
                field_b,
            });
        } else {
            let field = f
                .get("field")
                .ok_or_else(|| PlanError::new_err("filter must include 'field' key"))?
                .to_owned();
            let value = f
                .get("value")
                .ok_or_else(|| PlanError::new_err("filter must include 'value' key"))?
                .to_owned();
            plan.filter = Some(match op.as_str() {
                "!=" | "ne" => FilterPredicate::NotEqual { field, value },
                "==" | "eq" => FilterPredicate::Equal { field, value },
                other => {
                    let msg = format!("unsupported filter op {other:?}; use '!=' or '=='");
                    return Err(PlanError::new_err(msg));
                }
            });
        }
    }

    Ok(plan)
}
