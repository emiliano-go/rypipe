use super::*;

impl TableBuilder {
    // Predicate-first helpers
    /// Build the predicate bitmask from field_index. Called lazily on first
    /// `is_predicate_slot` call when the mask is still empty.
    pub(super) fn build_predicate_mask(&mut self) {
        let buf = match self.row_buf {
            Some(ref mut b) => b,
            None => return,
        };
        if !buf.pred_names.is_empty() {
            return; // already built (pred_names populated)
        }
        // Collect predicate field names (one-time clone of the filter).
        let mut names: SmallVec<[String; 4]> = SmallVec::new();
        if let Some(ref f) = self.plan.filter.clone() {
            Self::collect_predicate_field_names(f, &self.plan, &mut names);
        }
        // Cache the names so future calls can check without cloning.
        buf.pred_names.clone_from(&names);
        // Grow mask to cover all columns.
        let ncols = self.columns.len();
        let words = ncols.div_ceil(64);
        buf.predicate_mask.resize(words, 0);
        // Mark slots for predicate fields that already exist.
        for name in &names {
            if let Some(&slot) = self.field_index.get(name.as_str()) {
                let word = slot / 64;
                let bit = slot % 64;
                if word < buf.predicate_mask.len() {
                    buf.predicate_mask[word] |= 1u64 << bit;
                }
            }
        }
    }

    /// Mark a slot as a predicate column by checking cached predicate names.
    /// Called once per new column to handle fields created after the initial
    /// mask build (e.g., the predicate column appears in the data after
    /// `build_predicate_mask` already ran for earlier columns).
    #[inline]
    pub(super) fn mark_predicate_slot(&mut self, slot: u32) {
        if self.plan.filter.is_none() {
            return;
        }
        // Phase 1: check fast path (already marked) via immutable borrow,
        // then check if pred_names are populated.  Avoid holding &mut buf
        // across build_predicate_mask (which needs &mut self).
        let needs_build = self.row_buf.as_ref().is_some_and(|buf| {
            let word = slot as usize / 64;
            let bit = slot as usize % 64;
            if word < buf.predicate_mask.len() && (buf.predicate_mask[word] >> bit) & 1 == 1 {
                return false; // already marked; no-op
            }
            buf.pred_names.is_empty() // need to build the mask
        });
        if needs_build {
            self.build_predicate_mask();
        }
        // Phase 2: check if this slot is now marked after build.
        if let Some(ref buf) = self.row_buf {
            let word = slot as usize / 64;
            let bit = slot as usize % 64;
            if word < buf.predicate_mask.len() && (buf.predicate_mask[word] >> bit) & 1 == 1 {
                return; // marked during build or was already marked
            }
        }
        // Phase 3: mask is built but this slot isn't in it. Check if the
        // column name matches any cached predicate name (no filter clone).
        if let Some(col_name) = self.column_order.get(slot as usize) {
            let is_pred = self
                .row_buf
                .as_ref()
                .is_some_and(|buf| buf.pred_names.iter().any(|n| n == col_name));
            if is_pred {
                let buf = self.row_buf.as_mut().unwrap();
                let word = slot as usize / 64;
                let bit = slot as usize % 64;
                if word >= buf.predicate_mask.len() {
                    buf.predicate_mask.resize(word + 1, 0);
                }
                buf.predicate_mask[word] |= 1u64 << bit;
            }
        }
    }

    pub(super) fn collect_predicate_field_names(
        pred: &FilterPredicate,
        plan: &ExecutionPlan,
        names: &mut SmallVec<[String; 4]>,
    ) {
        match pred {
            FilterPredicate::Equal { field, .. }
            | FilterPredicate::NotEqual { field, .. }
            | FilterPredicate::CompareLiteral { field, .. }
            | FilterPredicate::StartsWith { field, .. }
            | FilterPredicate::EndsWith { field, .. }
            | FilterPredicate::In { field, .. }
            | FilterPredicate::NotIn { field, .. }
            | FilterPredicate::NotField { field, .. }
            | FilterPredicate::ArithmeticCompare { field, .. }
            | FilterPredicate::Strip { field, .. }
            | FilterPredicate::Lower { field, .. }
            | FilterPredicate::Upper { field, .. }
            | FilterPredicate::Replace { field, .. }
            | FilterPredicate::Length { field, .. }
            | FilterPredicate::Contains { field, .. }
            | FilterPredicate::IsNull { field, .. }
            | FilterPredicate::IsType { field, .. }
            | FilterPredicate::Regex { field, .. } => {
                let resolved = plan.resolve_field(field).unwrap_or(field);
                names.push(resolved.to_string());
            }
            FilterPredicate::Always(_) => {}
            FilterPredicate::Compare {
                field_a, field_b, ..
            } => {
                for f in [field_a, field_b] {
                    let resolved = plan.resolve_field(f).unwrap_or(f);
                    names.push(resolved.to_string());
                }
            }
            FilterPredicate::And(a, b) | FilterPredicate::Or(a, b) => {
                Self::collect_predicate_field_names(a, plan, names);
                Self::collect_predicate_field_names(b, plan, names);
            }
            FilterPredicate::Not(inner) => Self::collect_predicate_field_names(inner, plan, names),
        }
    }

    #[inline]
    pub(super) fn is_predicate_slot(&self, slot: u32) -> bool {
        self.row_buf.as_ref().is_some_and(|b| {
            let word = slot as usize / 64;
            let bit = slot as usize % 64;
            word < b.predicate_mask.len() && (b.predicate_mask[word] >> bit) & 1 == 1
        })
    }

    pub(super) fn get_buffered_value(&self, field: &str) -> Option<&Value<'static>> {
        let buf = self.row_buf.as_ref()?;
        let resolved = self.plan.resolve_field(field)?;
        let slot = *self.field_index.get(resolved)? as u32;
        for (s, v) in buf.fields.iter().rev() {
            if *s == slot {
                return Some(v);
            }
        }
        None
    }

    pub(super) fn get_buffered_str(&self, field: &str) -> Option<String> {
        let value = self.get_buffered_value(field)?;
        let resolved = self.plan.resolve_field(field)?;
        match self.plan.column_type(resolved) {
            FieldType::Int64 => match value {
                Value::Str(v) => v.parse::<i64>().ok().map(|v| v.to_string()),
                Value::Int64(v) => Some(v.to_string()),
                _ => None,
            },
            FieldType::Float64 => match value {
                Value::Str(v) => v.parse::<f64>().ok().map(|v| v.to_string()),
                Value::Int64(v) => Some(v.to_string()),
                Value::Float64(v) => Some(v.to_string()),
                _ => None,
            },
            FieldType::Boolean => match value {
                Value::Str(v) => v.parse::<bool>().ok().map(|v| v.to_string()),
                Value::Bool(v) => Some(v.to_string()),
                _ => None,
            },
            FieldType::Decimal128(scale) => match value {
                Value::Str(v) => crate::columnar::parse_decimal128(v, scale),
                _ => None,
            }
            .map(|v| crate::columnar::format_decimal128(v, scale)),
            FieldType::Date32 => match value {
                Value::Str(v) => crate::columnar::parse_date32(v),
                Value::Date32(v) => Some(*v),
                _ => None,
            }
            .map(crate::columnar::format_date32),
            FieldType::Timestamp(unit, format) => match value {
                Value::Str(v) => crate::columnar::parse_timestamp(v, unit, format.as_deref()),
                Value::Timestamp(v) => Some(*v),
                _ => None,
            }
            .map(|v| crate::columnar::format_timestamp(v, unit)),
            FieldType::String | FieldType::Dictionary => match value {
                Value::Str(s) => Some(s.to_string()),
                Value::Int64(i) => Some(i.to_string()),
                Value::Float64(f) => Some(f.to_string()),
                Value::Bool(b) => Some(b.to_string()),
                v => Some(format!("{v:?}")),
            },
        }
    }

    pub(super) fn evaluate_predicate_state(&self) -> PredicateState {
        let Some(ref pred) = self.plan.filter else {
            return PredicateState::Pass;
        };
        #[cfg(feature = "profile")]
        {
            crate::engine::PREDICATE_EVALUATIONS.fetch_add(1, Ordering::Relaxed);
        }
        let result = Self::eval_predicate(pred, self);
        #[cfg(feature = "profile")]
        match result {
            PredicateState::Fail => {
                crate::engine::PREDICATE_FAILS.fetch_add(1, Ordering::Relaxed);
            }
            PredicateState::Undecided => {
                crate::engine::PREDICATE_UNDECIDED.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        result
    }

    pub(super) fn eval_predicate(pred: &FilterPredicate, tb: &TableBuilder) -> PredicateState {
        match pred {
            FilterPredicate::Equal { field, value } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    if actual == *value {
                        PredicateState::Pass
                    } else {
                        PredicateState::Fail
                    }
                }
                None => PredicateState::Undecided,
            },
            FilterPredicate::NotEqual { field, value } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    if actual != *value {
                        PredicateState::Pass
                    } else {
                        PredicateState::Fail
                    }
                }
                None => PredicateState::Undecided,
            },
            FilterPredicate::Compare {
                field_a,
                op,
                field_b,
            } => {
                let type_a = tb
                    .plan
                    .column_type(tb.plan.resolve_field(field_a).unwrap_or(field_a.as_str()));
                let type_b = tb
                    .plan
                    .column_type(tb.plan.resolve_field(field_b).unwrap_or(field_b.as_str()));
                let compatible = match (&type_a, &type_b) {
                    (
                        FieldType::Int64 | FieldType::Float64,
                        FieldType::Int64 | FieldType::Float64,
                    )
                    | (FieldType::Decimal128(_), FieldType::Decimal128(_)) => true,
                    (FieldType::Timestamp(a, _), FieldType::Timestamp(b, _)) => a == b,
                    _ => type_a == type_b,
                };
                if !compatible {
                    return PredicateState::Fail;
                }
                let decimal_scale = |field: &str| {
                    let field = tb.plan.resolve_field(field).unwrap_or(field);
                    match tb.plan.column_type(field) {
                        FieldType::Decimal128(scale) => Some(scale),
                        _ => None,
                    }
                };
                if let (Some(a_scale), Some(b_scale)) =
                    (decimal_scale(field_a), decimal_scale(field_b))
                {
                    return match (tb.get_buffered_str(field_a), tb.get_buffered_str(field_b)) {
                        (Some(a), Some(b)) => {
                            let order = crate::columnar::parse_decimal128(&a, a_scale)
                                .zip(crate::columnar::parse_decimal128(&b, b_scale))
                                .map(|(a, b)| {
                                    crate::plan::compare_decimal128(a, a_scale, b, b_scale)
                                });
                            if order.is_some_and(|order| match op {
                                crate::plan::CompareOp::Gt => order == std::cmp::Ordering::Greater,
                                crate::plan::CompareOp::Lt => order == std::cmp::Ordering::Less,
                                crate::plan::CompareOp::Ge => order != std::cmp::Ordering::Less,
                                crate::plan::CompareOp::Le => order != std::cmp::Ordering::Greater,
                                crate::plan::CompareOp::Eq => order == std::cmp::Ordering::Equal,
                                crate::plan::CompareOp::Ne => order != std::cmp::Ordering::Equal,
                            }) {
                                PredicateState::Pass
                            } else {
                                PredicateState::Fail
                            }
                        }
                        _ => PredicateState::Undecided,
                    };
                }
                let normalize = |field: &str, value: &Value<'static>| {
                    let field = tb.plan.resolve_field(field).unwrap_or(field);
                    match tb.plan.column_type(field) {
                        FieldType::Int64 => match value.as_str() {
                            Some(s) => lexical::parse::<i64, _>(s.as_bytes())
                                .ok()
                                .map(Value::Int64),
                            _ => Some(value.clone()),
                        },
                        FieldType::Float64 => match value.as_str() {
                            Some(s) => lexical::parse::<f64, _>(s.as_bytes())
                                .ok()
                                .map(Value::Float64),
                            _ => Some(value.clone()),
                        },
                        FieldType::Boolean => match value.as_str() {
                            Some(s) => s.parse::<bool>().ok().map(Value::Bool),
                            _ => Some(value.clone()),
                        },
                        FieldType::Date32 => match value.as_str() {
                            Some(s) => crate::columnar::parse_date32(s).map(Value::Date32),
                            _ => Some(value.clone()),
                        },
                        FieldType::Timestamp(unit, fmt) => match value.as_str() {
                            Some(s) => crate::columnar::parse_timestamp(s, unit, fmt.as_deref())
                                .map(Value::Timestamp),
                            _ => Some(value.clone()),
                        },
                        FieldType::String | FieldType::Dictionary => Some(value.clone()),
                        FieldType::Decimal128(_) => None,
                    }
                };
                let va = tb
                    .get_buffered_value(field_a)
                    .and_then(|value| normalize(field_a, value));
                let vb = tb
                    .get_buffered_value(field_b)
                    .and_then(|value| normalize(field_b, value));
                match (va, vb) {
                    (Some(ref a), Some(ref b)) => {
                        let ord = match (a, b) {
                            (crate::value::Value::Int64(ai), crate::value::Value::Int64(bi)) => {
                                Some(ai.cmp(bi))
                            }
                            (
                                crate::value::Value::Float64(af),
                                crate::value::Value::Float64(bf),
                            ) => af.partial_cmp(bf),
                            (crate::value::Value::Int64(ai), crate::value::Value::Float64(bf)) => {
                                (*ai as f64).partial_cmp(bf)
                            }
                            (crate::value::Value::Float64(af), crate::value::Value::Int64(bi)) => {
                                af.partial_cmp(&(*bi as f64))
                            }
                            (crate::value::Value::Str(a), crate::value::Value::Str(b)) => {
                                let resolved_a =
                                    tb.plan.resolve_field(field_a).unwrap_or(field_a.as_str());
                                let resolved_b =
                                    tb.plan.resolve_field(field_b).unwrap_or(field_b.as_str());
                                let type_a = tb.plan.field_types.get(resolved_a);
                                let type_b = tb.plan.field_types.get(resolved_b);
                                match (type_a, type_b) {
                                    (
                                        Some(crate::plan::FieldType::Int64),
                                        Some(crate::plan::FieldType::Int64),
                                    ) => {
                                        let Some(ai) = lexical::parse::<i64, _>(a.as_bytes()).ok()
                                        else {
                                            return PredicateState::Fail;
                                        };
                                        let Some(bi) = lexical::parse(b.as_bytes()).ok() else {
                                            return PredicateState::Fail;
                                        };
                                        Some(ai.cmp(&bi))
                                    }
                                    (
                                        Some(crate::plan::FieldType::Float64),
                                        Some(crate::plan::FieldType::Float64),
                                    ) => {
                                        let Some(af) = lexical::parse::<f64, _>(a.as_bytes()).ok()
                                        else {
                                            return PredicateState::Fail;
                                        };
                                        let Some(bf) = lexical::parse::<f64, _>(b.as_bytes()).ok()
                                        else {
                                            return PredicateState::Fail;
                                        };
                                        af.partial_cmp(&bf)
                                    }
                                    (
                                        Some(crate::plan::FieldType::Int64),
                                        Some(crate::plan::FieldType::Float64),
                                    ) => {
                                        let Some(ai) = lexical::parse::<i64, _>(a.as_bytes()).ok()
                                        else {
                                            return PredicateState::Fail;
                                        };
                                        let Some(bf) = lexical::parse::<f64, _>(b.as_bytes()).ok()
                                        else {
                                            return PredicateState::Fail;
                                        };
                                        (ai as f64).partial_cmp(&bf)
                                    }
                                    (
                                        Some(crate::plan::FieldType::Float64),
                                        Some(crate::plan::FieldType::Int64),
                                    ) => {
                                        let Some(af) = lexical::parse::<f64, _>(a.as_bytes()).ok()
                                        else {
                                            return PredicateState::Fail;
                                        };
                                        let Some(bi) = lexical::parse::<i64, _>(b.as_bytes()).ok()
                                        else {
                                            return PredicateState::Fail;
                                        };
                                        af.partial_cmp(&(bi as f64))
                                    }
                                    // Mixed typed/untyped or String vs non-numeric type:
                                    // type mismatch; fail the comparison.
                                    (Some(_), None) | (None, Some(_)) => None,
                                    // Both untyped (String): fall back to lexicographic.
                                    (None, None) => Some(a.cmp(b)),
                                    // Both Timestamp: parse and compare as i64.
                                    (
                                        Some(crate::plan::FieldType::Timestamp(ua, fa)),
                                        Some(crate::plan::FieldType::Timestamp(ub, fb)),
                                    ) => {
                                        let ta = crate::columnar::parse_timestamp(
                                            a.as_ref(),
                                            *ua,
                                            fa.as_deref(),
                                        );
                                        let tb_val = crate::columnar::parse_timestamp(
                                            b.as_ref(),
                                            *ub,
                                            fb.as_deref(),
                                        );
                                        match (ta, tb_val) {
                                            (Some(ai), Some(bi)) => Some(ai.cmp(&bi)),
                                            _ => None,
                                        }
                                    }
                                    // Both Date32: parse and compare as i32.
                                    (
                                        Some(crate::plan::FieldType::Date32),
                                        Some(crate::plan::FieldType::Date32),
                                    ) => {
                                        let da = crate::columnar::parse_date32(a.as_ref());
                                        let db = crate::columnar::parse_date32(b.as_ref());
                                        match (da, db) {
                                            (Some(ai), Some(bi)) => Some(ai.cmp(&bi)),
                                            _ => None,
                                        }
                                    }
                                    // Different non-numeric types (e.g. String vs Bool): fail.
                                    _ => None,
                                }
                            }
                            (crate::value::Value::Bool(a), crate::value::Value::Bool(b)) => {
                                Some(a.cmp(b))
                            }
                            (crate::value::Value::Date32(a), crate::value::Value::Date32(b)) => {
                                Some(a.cmp(b))
                            }
                            (
                                crate::value::Value::Timestamp(a),
                                crate::value::Value::Timestamp(b),
                            ) => Some(a.cmp(b)),
                            _ => None,
                        };
                        let pass = ord.is_some_and(|ord| match op {
                            crate::plan::CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                            crate::plan::CompareOp::Lt => ord == std::cmp::Ordering::Less,
                            crate::plan::CompareOp::Ge => ord != std::cmp::Ordering::Less,
                            crate::plan::CompareOp::Le => ord != std::cmp::Ordering::Greater,
                            crate::plan::CompareOp::Eq => ord == std::cmp::Ordering::Equal,
                            crate::plan::CompareOp::Ne => ord != std::cmp::Ordering::Equal,
                        });
                        if pass {
                            PredicateState::Pass
                        } else {
                            PredicateState::Fail
                        }
                    }
                    _ => PredicateState::Undecided,
                }
            }
            FilterPredicate::CompareLiteral { field, op, value } => {
                let va = tb.get_buffered_value(field).and_then(|v| {
                    let resolved = tb.plan.resolve_field(field).unwrap_or(field);
                    match tb.plan.column_type(resolved) {
                        crate::plan::FieldType::Int64 => match v.as_str() {
                            Some(s) => lexical::parse::<i64, _>(s.as_bytes())
                                .ok()
                                .map(crate::value::Value::Int64),
                            _ => Some(v.clone()),
                        },
                        crate::plan::FieldType::Float64 => match v.as_str() {
                            Some(s) => lexical::parse::<f64, _>(s.as_bytes())
                                .ok()
                                .map(crate::value::Value::Float64),
                            _ => Some(v.clone()),
                        },
                        crate::plan::FieldType::Boolean => match v.as_str() {
                            Some(s) => s.parse::<bool>().ok().map(crate::value::Value::Bool),
                            _ => Some(v.clone()),
                        },
                        _ => Some(v.clone()),
                    }
                });
                let vb = match tb
                    .plan
                    .column_type(tb.plan.resolve_field(field).unwrap_or(field))
                {
                    crate::plan::FieldType::Int64 => lexical::parse::<i64, _>(value.as_bytes())
                        .ok()
                        .map(crate::value::Value::Int64),
                    crate::plan::FieldType::Float64 => lexical::parse::<f64, _>(value.as_bytes())
                        .ok()
                        .map(crate::value::Value::Float64),
                    crate::plan::FieldType::Boolean => {
                        value.parse::<bool>().ok().map(crate::value::Value::Bool)
                    }
                    _ => Some(crate::value::Value::Str(std::borrow::Cow::Borrowed(value))),
                };
                match (va, vb) {
                    (Some(ref a), Some(ref b)) => {
                        let ord = match (a, b) {
                            (crate::value::Value::Int64(ai), crate::value::Value::Int64(bi)) => {
                                Some(ai.cmp(bi))
                            }
                            (
                                crate::value::Value::Float64(af),
                                crate::value::Value::Float64(bf),
                            ) => af.partial_cmp(bf),
                            (crate::value::Value::Int64(ai), crate::value::Value::Float64(bf)) => {
                                (*ai as f64).partial_cmp(bf)
                            }
                            (crate::value::Value::Float64(af), crate::value::Value::Int64(bi)) => {
                                af.partial_cmp(&(*bi as f64))
                            }
                            (crate::value::Value::Str(a), crate::value::Value::Str(b)) => {
                                Some(a.cmp(b))
                            }
                            (crate::value::Value::Bool(a), crate::value::Value::Bool(b)) => {
                                Some(a.cmp(b))
                            }
                            _ => None,
                        };
                        let pass = ord.is_some_and(|ord| match op {
                            crate::plan::CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                            crate::plan::CompareOp::Lt => ord == std::cmp::Ordering::Less,
                            crate::plan::CompareOp::Ge => ord != std::cmp::Ordering::Less,
                            crate::plan::CompareOp::Le => ord != std::cmp::Ordering::Greater,
                            crate::plan::CompareOp::Eq => ord == std::cmp::Ordering::Equal,
                            crate::plan::CompareOp::Ne => ord != std::cmp::Ordering::Equal,
                        });
                        if pass {
                            PredicateState::Pass
                        } else {
                            PredicateState::Fail
                        }
                    }
                    _ => PredicateState::Undecided,
                }
            }
            FilterPredicate::StartsWith { field, value } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    if actual.starts_with(value.as_str()) {
                        PredicateState::Pass
                    } else {
                        PredicateState::Fail
                    }
                }
                None => PredicateState::Undecided,
            },
            FilterPredicate::EndsWith { field, value } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    if actual.ends_with(value.as_str()) {
                        PredicateState::Pass
                    } else {
                        PredicateState::Fail
                    }
                }
                None => PredicateState::Undecided,
            },
            FilterPredicate::In { field, values } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    if values.contains(&actual) {
                        PredicateState::Pass
                    } else {
                        PredicateState::Fail
                    }
                }
                None => PredicateState::Fail,
            },
            FilterPredicate::NotIn { field, values } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    if !values.contains(&actual) {
                        PredicateState::Pass
                    } else {
                        PredicateState::Fail
                    }
                }
                None => PredicateState::Pass,
            },
            FilterPredicate::Always(keep) => {
                if *keep {
                    PredicateState::Pass
                } else {
                    PredicateState::Fail
                }
            }
            FilterPredicate::NotField { field } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    if actual.is_empty() {
                        PredicateState::Pass
                    } else {
                        PredicateState::Fail
                    }
                }
                None => PredicateState::Pass,
            },
            FilterPredicate::ArithmeticCompare {
                field,
                arith_op,
                arith_value,
                cmp_op,
                cmp_value,
            } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    let field_f64 = match actual.parse::<f64>() {
                        Ok(v) => v,
                        Err(_) => return PredicateState::Fail,
                    };
                    let result = match arith_op {
                        crate::plan::ArithOp::Add => field_f64 + arith_value,
                        crate::plan::ArithOp::Sub => field_f64 - arith_value,
                        crate::plan::ArithOp::Mul => field_f64 * arith_value,
                        crate::plan::ArithOp::Div => {
                            if *arith_value == 0.0 {
                                return PredicateState::Fail;
                            }
                            field_f64 / arith_value
                        }
                    };
                    let cmp_f64 = match cmp_value.parse::<f64>() {
                        Ok(v) => v,
                        Err(_) => return PredicateState::Fail,
                    };
                    match result.partial_cmp(&cmp_f64) {
                        Some(ord) => {
                            let pass = match cmp_op {
                                crate::plan::CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                                crate::plan::CompareOp::Lt => ord == std::cmp::Ordering::Less,
                                crate::plan::CompareOp::Ge => ord != std::cmp::Ordering::Less,
                                crate::plan::CompareOp::Le => ord != std::cmp::Ordering::Greater,
                                crate::plan::CompareOp::Eq => ord == std::cmp::Ordering::Equal,
                                crate::plan::CompareOp::Ne => ord != std::cmp::Ordering::Equal,
                            };
                            if pass {
                                PredicateState::Pass
                            } else {
                                PredicateState::Fail
                            }
                        }
                        None => PredicateState::Fail,
                    }
                }
                None => PredicateState::Undecided,
            },
            FilterPredicate::Strip {
                field,
                op,
                value,
                mode,
            } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    let transformed = match mode {
                        crate::plan::TrimMode::Both => actual.trim(),
                        crate::plan::TrimMode::Start => actual.trim_start(),
                        crate::plan::TrimMode::End => actual.trim_end(),
                    };
                    match transformed.partial_cmp(value.as_str()) {
                        Some(ord) => {
                            let pass = match op {
                                crate::plan::CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                                crate::plan::CompareOp::Lt => ord == std::cmp::Ordering::Less,
                                crate::plan::CompareOp::Ge => ord != std::cmp::Ordering::Less,
                                crate::plan::CompareOp::Le => ord != std::cmp::Ordering::Greater,
                                crate::plan::CompareOp::Eq => ord == std::cmp::Ordering::Equal,
                                crate::plan::CompareOp::Ne => ord != std::cmp::Ordering::Equal,
                            };
                            if pass {
                                PredicateState::Pass
                            } else {
                                PredicateState::Fail
                            }
                        }
                        None => PredicateState::Fail,
                    }
                }
                None => PredicateState::Undecided,
            },
            FilterPredicate::Lower { field, op, value } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    let transformed = actual.to_lowercase();
                    match transformed.as_str().partial_cmp(value.as_str()) {
                        Some(ord) => {
                            let pass = match op {
                                crate::plan::CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                                crate::plan::CompareOp::Lt => ord == std::cmp::Ordering::Less,
                                crate::plan::CompareOp::Ge => ord != std::cmp::Ordering::Less,
                                crate::plan::CompareOp::Le => ord != std::cmp::Ordering::Greater,
                                crate::plan::CompareOp::Eq => ord == std::cmp::Ordering::Equal,
                                crate::plan::CompareOp::Ne => ord != std::cmp::Ordering::Equal,
                            };
                            if pass {
                                PredicateState::Pass
                            } else {
                                PredicateState::Fail
                            }
                        }
                        None => PredicateState::Fail,
                    }
                }
                None => PredicateState::Undecided,
            },
            FilterPredicate::Upper { field, op, value } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    let transformed = actual.to_uppercase();
                    match transformed.as_str().partial_cmp(value.as_str()) {
                        Some(ord) => {
                            let pass = match op {
                                crate::plan::CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                                crate::plan::CompareOp::Lt => ord == std::cmp::Ordering::Less,
                                crate::plan::CompareOp::Ge => ord != std::cmp::Ordering::Less,
                                crate::plan::CompareOp::Le => ord != std::cmp::Ordering::Greater,
                                crate::plan::CompareOp::Eq => ord == std::cmp::Ordering::Equal,
                                crate::plan::CompareOp::Ne => ord != std::cmp::Ordering::Equal,
                            };
                            if pass {
                                PredicateState::Pass
                            } else {
                                PredicateState::Fail
                            }
                        }
                        None => PredicateState::Fail,
                    }
                }
                None => PredicateState::Undecided,
            },
            FilterPredicate::Replace {
                field,
                old,
                new,
                op,
                value,
            } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    let transformed = actual.replace(old.as_str(), new.as_str());
                    match transformed.as_str().partial_cmp(value.as_str()) {
                        Some(ord) => {
                            let pass = match op {
                                crate::plan::CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                                crate::plan::CompareOp::Lt => ord == std::cmp::Ordering::Less,
                                crate::plan::CompareOp::Ge => ord != std::cmp::Ordering::Less,
                                crate::plan::CompareOp::Le => ord != std::cmp::Ordering::Greater,
                                crate::plan::CompareOp::Eq => ord == std::cmp::Ordering::Equal,
                                crate::plan::CompareOp::Ne => ord != std::cmp::Ordering::Equal,
                            };
                            if pass {
                                PredicateState::Pass
                            } else {
                                PredicateState::Fail
                            }
                        }
                        None => PredicateState::Fail,
                    }
                }
                None => PredicateState::Undecided,
            },
            FilterPredicate::Length { field, op, value } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    let len = actual.chars().count() as f64;
                    let cmp_val = match value.parse::<f64>() {
                        Ok(v) => v,
                        Err(_) => return PredicateState::Fail,
                    };
                    match len.partial_cmp(&cmp_val) {
                        Some(ord) => {
                            let pass = match op {
                                crate::plan::CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                                crate::plan::CompareOp::Lt => ord == std::cmp::Ordering::Less,
                                crate::plan::CompareOp::Ge => ord != std::cmp::Ordering::Less,
                                crate::plan::CompareOp::Le => ord != std::cmp::Ordering::Greater,
                                crate::plan::CompareOp::Eq => ord == std::cmp::Ordering::Equal,
                                crate::plan::CompareOp::Ne => ord != std::cmp::Ordering::Equal,
                            };
                            if pass {
                                PredicateState::Pass
                            } else {
                                PredicateState::Fail
                            }
                        }
                        None => PredicateState::Fail,
                    }
                }
                None => PredicateState::Undecided,
            },
            FilterPredicate::Contains { field, value } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    if actual.contains(value.as_str()) {
                        PredicateState::Pass
                    } else {
                        PredicateState::Fail
                    }
                }
                None => PredicateState::Undecided,
            },
            FilterPredicate::Regex { field, re } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    if re.compiled.is_match(actual.as_ref()) {
                        PredicateState::Pass
                    } else {
                        PredicateState::Fail
                    }
                }
                None => PredicateState::Undecided,
            },
            FilterPredicate::IsNull { field } => match tb.get_buffered_str(field) {
                Some(_) => PredicateState::Fail,
                None => PredicateState::Pass,
            },
            FilterPredicate::IsType { field, field_type } => {
                // Check the plan's declared type first (fast path)
                let resolved = tb.plan.resolve_field(field).unwrap_or(field);
                let declared = tb.plan.column_type(resolved);
                if declared == *field_type {
                    // Declared type matches: pass if value exists
                    if tb.get_buffered_value(field).is_some() {
                        return PredicateState::Pass;
                    }
                }
                // For String columns, check if the buffered value can be parsed
                if let Some(val) = tb.get_buffered_value(field) {
                    match field_type {
                        crate::plan::FieldType::Int64 => {
                            if let Some(s) = val.as_str() {
                                if s.parse::<i64>().is_ok() {
                                    return PredicateState::Pass;
                                }
                            }
                        }
                        crate::plan::FieldType::Float64 => {
                            if let Some(s) = val.as_str() {
                                if s.parse::<f64>().is_ok() {
                                    return PredicateState::Pass;
                                }
                            }
                        }
                        crate::plan::FieldType::Boolean => {
                            if let Some(s) = val.as_str() {
                                if matches!(
                                    s.to_lowercase().as_str(),
                                    "true" | "false" | "1" | "0" | "yes" | "no"
                                ) {
                                    return PredicateState::Pass;
                                }
                            }
                        }
                        crate::plan::FieldType::String | crate::plan::FieldType::Dictionary => {
                            return PredicateState::Pass;
                        }
                        _ => {}
                    }
                }
                PredicateState::Fail
            }
            FilterPredicate::And(a, b) => {
                let sa = Self::eval_predicate(a, tb);
                let sb = Self::eval_predicate(b, tb);
                match (sa, sb) {
                    (PredicateState::Fail, _) | (_, PredicateState::Fail) => PredicateState::Fail,
                    (PredicateState::Pass, PredicateState::Pass) => PredicateState::Pass,
                    _ => PredicateState::Undecided,
                }
            }
            FilterPredicate::Or(a, b) => {
                let sa = Self::eval_predicate(a, tb);
                let sb = Self::eval_predicate(b, tb);
                match (sa, sb) {
                    (PredicateState::Pass, _) | (_, PredicateState::Pass) => PredicateState::Pass,
                    (PredicateState::Fail, PredicateState::Fail) => PredicateState::Fail,
                    _ => PredicateState::Undecided,
                }
            }
            FilterPredicate::Not(inner) => match Self::eval_predicate(inner, tb) {
                PredicateState::Pass => PredicateState::Fail,
                PredicateState::Fail => PredicateState::Pass,
                PredicateState::Undecided => PredicateState::Undecided,
            },
        }
    }

    pub(super) fn drain_buffered(&mut self, pass: bool) {
        let observer = self.plan.observer.clone();
        let buf = match self.row_buf {
            Some(ref mut b) => b,
            None => return,
        };
        if pass {
            // Deduplicate last-write-wins by slot index (u32).
            // O(n) reverse scan with a u64 bitmask; first hit in reverse is
            // the last write in forward order (last-write-wins).
            let ncols = self.columns.len();
            let words = ncols.div_ceil(64);
            if self.row_dirty.len() < words {
                self.row_dirty.resize(words, 0);
            }
            let mut seen: u64 = 0;
            let mut seen_extra: SmallVec<[u64; 1]> = SmallVec::new();
            if words > 1 {
                seen_extra.resize(words - 1, 0);
            }
            for &(slot, ref val) in buf.fields.iter().rev() {
                let word = slot as usize / 64;
                let bit = slot as usize % 64;
                let mask = 1u64 << bit;
                let already = if word == 0 {
                    seen & mask != 0
                } else {
                    seen_extra.get(word - 1).is_some_and(|w| w & mask != 0)
                };
                if already {
                    continue;
                }
                if word == 0 {
                    seen |= mask;
                } else if let Some(w) = seen_extra.get_mut(word - 1) {
                    *w |= mask;
                }
                self.row_dirty[word] |= mask;
                let b = &mut self.columns[slot as usize];
                if b.len() > self.row_count {
                    b.pop();
                }
                b.push_value(val.clone());
                // Buffered values are observable only once the row is
                // accepted (drained), never while merely buffered.
                if let Some(ref obs) = observer {
                    if let Some(name) = self.column_order.get(slot as usize) {
                        obs.on_put_field(self.row_count, name, slot as usize, val);
                    }
                }
            }
            // Null-fill missing columns and handle filter/dirty, then increment row_count.
            // Skip null-fill when called mid-row (buf.direct = true): the remaining
            // fields will be pushed via the direct path, and end_row's
            // null_fill_missing handles any columns that don't appear.
            if !buf.direct {
                let ncols = self.columns.len();
                let full_words = ncols / 64;
                let rem_bits = ncols % 64;
                let is_full = (0..full_words).all(|w| self.row_dirty[w] == u64::MAX)
                    && (rem_bits == 0
                        || self.row_dirty.get(full_words).copied().unwrap_or(0)
                            == (1u64 << rem_bits) - 1);
                if is_full {
                    self.row_dirty.fill(0);
                } else {
                    for (i, b) in self.columns.iter_mut().enumerate() {
                        let word = i / 64;
                        let bit = i % 64;
                        let is_set = (self.row_dirty[word] >> bit) & 1 == 1;
                        if !is_set {
                            b.push(None);
                        }
                    }
                    self.row_dirty.fill(0);
                }
            } else {
                // Mid-row: keep dirty bits set so null_fill_missing in end_row
                // can take the is_full fast path when all columns were pushed.
            }
            self.row_count += 1;
            self.rows_accepted += 1;
            if let Some(ref obs) = observer {
                obs.on_row_accepted(self.row_count - 1);
            }
        } else {
            // Fail: discard buffered fields, no row increment, clear dirty
            buf.fields.clear();
            self.row_dirty.fill(0);
            self.rows_rejected += 1;
            if let Some(ref obs) = observer {
                obs.on_row_rejected(self.row_count);
            }
        }
        // Ensure fields are cleared for next row
        if !buf.fields.is_empty() {
            buf.fields.clear();
        }
        buf.state = PredicateState::Undecided;
    }

    pub(super) fn evaluate_against_null(&self) -> PredicateState {
        // At end_row, any predicate field still missing is NULL.
        // For Equal with missing => Fail, NotEqual with missing => Pass (as per old finish_row check where get_value returns None)
        // For Compare with missing => Fail.
        // We can reuse eval_predicate which returns Undecided for missing, then map Undecided to Pass/Fail per leaf semantics.
        let Some(ref pred) = self.plan.filter else {
            return PredicateState::Pass;
        };
        Self::eval_predicate_with_null(pred, self)
    }

    pub(super) fn eval_predicate_with_null(
        pred: &FilterPredicate,
        tb: &TableBuilder,
    ) -> PredicateState {
        match pred {
            FilterPredicate::Equal { field, value } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    if actual == *value {
                        PredicateState::Pass
                    } else {
                        PredicateState::Fail
                    }
                }
                None => PredicateState::Fail, // missing => None != Some(value) => Fail for Equal
            },
            FilterPredicate::NotEqual { field, value } => match tb.get_buffered_str(field) {
                Some(actual) => {
                    if actual != *value {
                        PredicateState::Pass
                    } else {
                        PredicateState::Fail
                    }
                }
                None => PredicateState::Pass, // missing => None != Some(value) => Pass
            },
            FilterPredicate::Compare { .. }
            | FilterPredicate::CompareLiteral { .. }
            | FilterPredicate::StartsWith { .. }
            | FilterPredicate::EndsWith { .. }
            | FilterPredicate::In { .. }
            | FilterPredicate::NotIn { .. }
            | FilterPredicate::NotField { .. }
            | FilterPredicate::ArithmeticCompare { .. }
            | FilterPredicate::Strip { .. }
            | FilterPredicate::Lower { .. }
            | FilterPredicate::Upper { .. }
            | FilterPredicate::Replace { .. }
            | FilterPredicate::Length { .. }
            | FilterPredicate::Contains { .. }
            | FilterPredicate::IsNull { .. }
            | FilterPredicate::IsType { .. }
            | FilterPredicate::Regex { .. } => match Self::eval_predicate(pred, tb) {
                PredicateState::Undecided => PredicateState::Fail,
                other => other,
            },
            FilterPredicate::Always(keep) => {
                if *keep {
                    PredicateState::Pass
                } else {
                    PredicateState::Fail
                }
            }
            FilterPredicate::And(a, b) => {
                let sa = Self::eval_predicate_with_null(a, tb);
                let sb = Self::eval_predicate_with_null(b, tb);
                match (sa, sb) {
                    (PredicateState::Fail, _) | (_, PredicateState::Fail) => PredicateState::Fail,
                    (PredicateState::Pass, PredicateState::Pass) => PredicateState::Pass,
                    _ => PredicateState::Undecided, // Should not happen after null mapping, but treat as Fail?
                }
            }
            FilterPredicate::Or(a, b) => {
                let sa = Self::eval_predicate_with_null(a, tb);
                let sb = Self::eval_predicate_with_null(b, tb);
                match (sa, sb) {
                    (PredicateState::Pass, _) | (_, PredicateState::Pass) => PredicateState::Pass,
                    (PredicateState::Fail, PredicateState::Fail) => PredicateState::Fail,
                    _ => PredicateState::Fail,
                }
            }
            FilterPredicate::Not(inner) => match Self::eval_predicate_with_null(inner, tb) {
                PredicateState::Pass => PredicateState::Fail,
                PredicateState::Fail => PredicateState::Pass,
                PredicateState::Undecided => PredicateState::Fail, // Not Undecided -> Fail?
            },
        }
    }
}
