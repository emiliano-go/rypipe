use super::{ExecutionPlan, FieldType};
use rustc_hash::FxHashMap as HashMap;

/// Comparison operator for column-to-column filters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompareOp {
    Gt,
    Lt,
    Ge,
    Le,
    Eq,
    Ne,
}

/// Arithmetic operator for field expressions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrimMode {
    Both,
    Start,
    End,
}

impl std::str::FromStr for ArithOp {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "+" | "add" => Ok(ArithOp::Add),
            "-" | "sub" => Ok(ArithOp::Sub),
            "*" | "mul" => Ok(ArithOp::Mul),
            "/" | "div" => Ok(ArithOp::Div),
            _ => Err(()),
        }
    }
}

impl std::str::FromStr for CompareOp {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            ">" | "gt" => Ok(CompareOp::Gt),
            "<" | "lt" => Ok(CompareOp::Lt),
            ">=" | "ge" => Ok(CompareOp::Ge),
            "<=" | "le" => Ok(CompareOp::Le),
            "==" | "eq" => Ok(CompareOp::Eq),
            "!=" | "ne" => Ok(CompareOp::Ne),
            _ => Err(()),
        }
    }
}

/// A filter predicate evaluated per-row during parsing.
///
// Leaf predicates (`Equal`, `NotEqual`, `Compare`, `CompareLiteral`) can be
// composed into a tree with `And`, `Or`, and `Not`. Evaluation is recursive
// with short-circuiting; a row is kept only if the whole tree passes.
#[derive(Clone, Debug, PartialEq)]
pub enum FilterPredicate {
    /// Keep row if `field_value != value` (string comparison, per-row).
    NotEqual { field: String, value: String },
    /// Keep row if `field_value == value` (string comparison, per-row).
    Equal { field: String, value: String },
    /// Column-to-column comparison, evaluated natively per-row during
    /// parsing with numeric promotion (Int64 vs Float64 widens).
    Compare {
        field_a: String,
        op: CompareOp,
        field_b: String,
    },
    /// Column-to-literal comparison with ordering. Uses typed comparison
    /// with numeric promotion (e.g. Int64 field > Float64 literal).
    CompareLiteral {
        field: String,
        op: CompareOp,
        value: String,
    },
    /// Keep row if `field_value.starts_with(value)`.
    StartsWith { field: String, value: String },
    /// Keep row if `field_value.ends_with(value)`.
    EndsWith { field: String, value: String },
    /// Keep row if `field_value` is in the given set.
    In { field: String, values: Vec<String> },
    /// Keep row if `field_value` is not in the given set.
    NotIn { field: String, values: Vec<String> },
    /// Always keep or reject the row.
    Always(bool),
    /// Keep row if the field is falsy (None, empty string, or missing).
    NotField { field: String },
    /// Arithmetic compare: field <arith_op> <arith_value> <cmp_op> <cmp_value>
    ArithmeticCompare {
        field: String,
        arith_op: ArithOp,
        arith_value: f64,
        cmp_op: CompareOp,
        cmp_value: String,
    },
    /// Keep row if `field_value.strip()` satisfies the comparison.
    Strip {
        field: String,
        op: CompareOp,
        value: String,
        mode: TrimMode,
    },
    /// Keep row if `field_value.to_lowercase()` satisfies the comparison.
    Lower {
        field: String,
        op: CompareOp,
        value: String,
    },
    /// Keep row if `field_value.to_uppercase()` satisfies the comparison.
    Upper {
        field: String,
        op: CompareOp,
        value: String,
    },
    /// Keep row if `field_value.replace(old, new)` satisfies the comparison.
    Replace {
        field: String,
        old: String,
        new: String,
        op: CompareOp,
        value: String,
    },
    /// Keep row if `field_value.len()` satisfies the comparison (numeric).
    Length {
        field: String,
        op: CompareOp,
        value: String,
    },
    /// Keep row if `field_value.contains(value)`.
    Contains { field: String, value: String },
    /// Keep row if the field is null or missing.
    IsNull { field: String },
    /// Keep row if the field is of the given type (e.g., Int64, Float64, String).
    IsType {
        field: String,
        field_type: FieldType,
    },
    /// Keep row if `field_value` matches the regular expression.
    Regex { field: String, re: RegexSpec },
    /// Keep row if both sub-predicates pass. Short-circuits on the first
    /// failure.
    And(Box<FilterPredicate>, Box<FilterPredicate>),
    /// Keep row if either sub-predicate passes. Short-circuits on the first
    /// success.
    Or(Box<FilterPredicate>, Box<FilterPredicate>),
    /// Keep row if the sub-predicate fails.
    Not(Box<FilterPredicate>),
}

/// A compiled regex plus its pattern. Equality and debug output use the
/// pattern string; the compiled form is the execution representation.
#[derive(Clone)]
pub struct RegexSpec {
    pub pattern: String,
    pub compiled: regex::Regex,
}

impl RegexSpec {
    /// Maximum compiled regex size in bytes (1 MiB). Patterns that compile
    /// to larger automata are rejected to limit memory use and reduce ReDoS
    /// risk.
    const SIZE_LIMIT: usize = 1024 * 1024;

    pub fn new(pattern: impl Into<String>) -> std::result::Result<Self, regex::Error> {
        let pattern = pattern.into();
        let compiled = regex::RegexBuilder::new(&pattern)
            .size_limit(Self::SIZE_LIMIT)
            .build()?;
        Ok(Self { pattern, compiled })
    }
}

impl std::fmt::Debug for RegexSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegexSpec")
            .field("pattern", &self.pattern)
            .finish()
    }
}

impl PartialEq for RegexSpec {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern
    }
}

impl FilterPredicate {
    /// Combine two predicates with logical AND.
    pub fn all(a: FilterPredicate, b: FilterPredicate) -> Self {
        FilterPredicate::And(Box::new(a), Box::new(b))
    }

    /// Combine two predicates with logical OR.
    pub fn any(a: FilterPredicate, b: FilterPredicate) -> Self {
        FilterPredicate::Or(Box::new(a), Box::new(b))
    }

    /// Negate a predicate.
    #[allow(clippy::should_implement_trait)]
    pub fn not(inner: FilterPredicate) -> Self {
        FilterPredicate::Not(Box::new(inner))
    }
}

impl FilterPredicate {
    /// Check whether a partial row passes the filter.
    /// `columns`/`field_index` contain all builders; `row_index` is the current
    /// row number. Returns true to keep the row.
    ///
    /// * `Equal`/`NotEqual` use string comparison on the stored value.
    /// * `Compare` is evaluated natively per-row against typed values with
    ///   numeric promotion (`Int64` vs `Float64` widens to f64); any other
    ///   type mismatch or a null operand fails the row.
    /// * `And`/`Or` short-circuit; `Not` negates. A missing field fails an
    ///   inner leaf, which `Not` can flip back to a keep.
    pub(crate) fn check(
        &self,
        columns: &[crate::columnar::ColumnBuilder],
        field_index: &HashMap<String, usize>,
        row_index: usize,
        plan: &ExecutionPlan,
    ) -> bool {
        match self {
            FilterPredicate::NotEqual { field, value } => {
                let actual = get_value(columns, field_index, field, plan, row_index);
                actual.as_deref() != Some(value.as_str())
            }
            FilterPredicate::Equal { field, value } => {
                let actual = get_value(columns, field_index, field, plan, row_index);
                actual.as_deref() == Some(value.as_str())
            }
            FilterPredicate::Compare {
                field_a,
                op,
                field_b,
            } => {
                let va = get_column(columns, field_index, resolve(field_a, plan))
                    .and_then(|b| b.get_typed_value(row_index));
                let vb = get_column(columns, field_index, resolve(field_b, plan))
                    .and_then(|b| b.get_typed_value(row_index));
                match (va, vb) {
                    (Some(a), Some(b)) => compare_typed(&a, *op, &b),
                    _ => false,
                }
            }
            FilterPredicate::CompareLiteral { field, op, value } => {
                let va = get_column(columns, field_index, resolve(field, plan))
                    .and_then(|b| b.get_typed_value(row_index));
                if let Some(a) = va {
                    if let Some(b) = typed_from_literal(&a, value) {
                        return compare_typed(&a, *op, &b);
                    }
                }
                false
            }
            FilterPredicate::StartsWith { field, value } => {
                let actual = get_value(columns, field_index, field, plan, row_index);
                match actual {
                    Some(s) => s.starts_with(value.as_str()),
                    None => false,
                }
            }
            FilterPredicate::EndsWith { field, value } => {
                let actual = get_value(columns, field_index, field, plan, row_index);
                match actual {
                    Some(s) => s.ends_with(value.as_str()),
                    None => false,
                }
            }
            FilterPredicate::In { field, values } => {
                let actual = get_value(columns, field_index, field, plan, row_index);
                match actual {
                    Some(s) => values.contains(&s),
                    None => false,
                }
            }
            FilterPredicate::NotIn { field, values } => {
                let actual = get_value(columns, field_index, field, plan, row_index);
                match actual {
                    Some(s) => !values.contains(&s),
                    None => true,
                }
            }
            FilterPredicate::Always(keep) => *keep,
            FilterPredicate::NotField { field } => {
                let actual = get_value(columns, field_index, field, plan, row_index);
                match actual {
                    Some(s) => s.is_empty(),
                    None => true,
                }
            }
            FilterPredicate::ArithmeticCompare {
                field,
                arith_op,
                arith_value,
                cmp_op,
                cmp_value,
            } => {
                let actual = get_column(columns, field_index, resolve(field, plan))
                    .and_then(|b| b.get_typed_value(row_index));
                if let Some(a) = actual {
                    let field_f64 = match &a {
                        crate::columnar::TypedValue::Int64(v) => *v as f64,
                        crate::columnar::TypedValue::Float64(v) => *v,
                        crate::columnar::TypedValue::Decimal128(v, scale) => {
                            *v as f64 / 10_f64.powi(*scale as i32)
                        }
                        crate::columnar::TypedValue::Str(s) => match s.parse::<f64>() {
                            Ok(v) => v,
                            Err(_) => return false,
                        },
                        _ => return false,
                    };
                    let result = match arith_op {
                        ArithOp::Add => field_f64 + arith_value,
                        ArithOp::Sub => field_f64 - arith_value,
                        ArithOp::Mul => field_f64 * arith_value,
                        ArithOp::Div => {
                            if *arith_value == 0.0 {
                                return false;
                            }
                            field_f64 / arith_value
                        }
                    };
                    let cmp_f64 = match cmp_value.parse::<f64>() {
                        Ok(v) => v,
                        Err(_) => return false,
                    };
                    return apply_op(*cmp_op, result.partial_cmp(&cmp_f64));
                }
                false
            }
            FilterPredicate::Strip {
                field,
                op,
                value,
                mode,
            } => {
                let actual = get_value(columns, field_index, field, plan, row_index);
                match actual {
                    Some(s) => {
                        let transformed = match mode {
                            TrimMode::Both => s.trim(),
                            TrimMode::Start => s.trim_start(),
                            TrimMode::End => s.trim_end(),
                        };
                        apply_op(*op, transformed.partial_cmp(value.as_str()))
                    }
                    None => false,
                }
            }
            FilterPredicate::Lower { field, op, value } => {
                let actual = get_value(columns, field_index, field, plan, row_index);
                match actual {
                    Some(s) => {
                        let transformed = s.to_lowercase();
                        apply_op(*op, transformed.as_str().partial_cmp(value.as_str()))
                    }
                    None => false,
                }
            }
            FilterPredicate::Upper { field, op, value } => {
                let actual = get_value(columns, field_index, field, plan, row_index);
                match actual {
                    Some(s) => {
                        let transformed = s.to_uppercase();
                        apply_op(*op, transformed.as_str().partial_cmp(value.as_str()))
                    }
                    None => false,
                }
            }
            FilterPredicate::Replace {
                field,
                old,
                new,
                op,
                value,
            } => {
                let actual = get_value(columns, field_index, field, plan, row_index);
                match actual {
                    Some(s) => {
                        let transformed = s.replace(old.as_str(), new.as_str());
                        apply_op(*op, transformed.as_str().partial_cmp(value.as_str()))
                    }
                    None => false,
                }
            }
            FilterPredicate::Length { field, op, value } => {
                let actual = get_value(columns, field_index, field, plan, row_index);
                match actual {
                    Some(s) => {
                        let len = s.chars().count() as f64;
                        let cmp_val = match value.parse::<f64>() {
                            Ok(v) => v,
                            Err(_) => return false,
                        };
                        apply_op(*op, len.partial_cmp(&cmp_val))
                    }
                    None => false,
                }
            }
            FilterPredicate::Contains { field, value } => {
                let actual = get_value(columns, field_index, field, plan, row_index);
                match actual {
                    Some(s) => s.contains(value.as_str()),
                    None => false,
                }
            }
            FilterPredicate::IsNull { field } => {
                let actual = get_value(columns, field_index, field, plan, row_index);
                actual.is_none()
            }
            FilterPredicate::Regex { field, re } => {
                let actual = get_value(columns, field_index, field, plan, row_index);
                match actual {
                    Some(s) => re.compiled.is_match(s.as_ref()),
                    None => false,
                }
            }
            FilterPredicate::IsType { field, field_type } => {
                let resolved = resolve(field, plan);
                let col = get_column(columns, field_index, resolved);
                match col {
                    None => false,
                    Some(c) => c.is_type_at(row_index, field_type),
                }
            }
            FilterPredicate::And(a, b) => {
                // Evaluate the operand with the earlier field first for
                // short-circuit benefit (C2: reorder by document position).
                let (first, second) =
                    if pred_ordinal(a, field_index, plan) <= pred_ordinal(b, field_index, plan) {
                        (a.as_ref(), b.as_ref())
                    } else {
                        (b.as_ref(), a.as_ref())
                    };
                first.check(columns, field_index, row_index, plan)
                    && second.check(columns, field_index, row_index, plan)
            }
            FilterPredicate::Or(a, b) => {
                let (first, second) =
                    if pred_ordinal(a, field_index, plan) <= pred_ordinal(b, field_index, plan) {
                        (a.as_ref(), b.as_ref())
                    } else {
                        (b.as_ref(), a.as_ref())
                    };
                first.check(columns, field_index, row_index, plan)
                    || second.check(columns, field_index, row_index, plan)
            }
            FilterPredicate::Not(inner) => !inner.check(columns, field_index, row_index, plan),
        }
    }
}

/// Resolve a filter field name to its output column name.
fn resolve<'a>(field: &'a str, plan: &'a ExecutionPlan) -> &'a str {
    plan.field_map.get(field).map_or(field, |s| s.as_str())
}

fn get_column<'a>(
    columns: &'a [crate::columnar::ColumnBuilder],
    field_index: &HashMap<String, usize>,
    name: &str,
) -> Option<&'a crate::columnar::ColumnBuilder> {
    field_index.get(name).map(|&i| &columns[i])
}

/// Fetch a stored value formatted as a string for Equal/NotEqual checks.
/// Tries zero-allocation `get_filter_view` first (String/Dict columns);
/// falls back to `get_filter_value` for typed columns that need formatting.
fn get_value(
    columns: &[crate::columnar::ColumnBuilder],
    field_index: &HashMap<String, usize>,
    field: &str,
    plan: &ExecutionPlan,
    row_index: usize,
) -> Option<String> {
    let col = get_column(columns, field_index, resolve(field, plan))?;
    // Fast path: borrowed &str for String/Dictionary columns (no allocation).
    if let Some(s) = col.get_filter_view(row_index) {
        return Some(s.to_owned());
    }
    // Slow path: typed columns need formatting (allocates).
    col.get_filter_value(row_index)
}

/// Native-typed comparison with numeric promotion. Mixed Int64/Float64
/// operands are widened to f64; equal integral types stay integral.
/// Nulls never reach here (checked by the caller).
fn compare_typed(
    a: &crate::columnar::TypedValue<'_>,
    op: CompareOp,
    b: &crate::columnar::TypedValue<'_>,
) -> bool {
    use crate::columnar::TypedValue as T;
    match (a, b) {
        (T::Int64(x), T::Int64(y)) => apply_op(op, Some(x.cmp(y))),
        (T::Float64(x), T::Float64(y)) => apply_op(op, x.partial_cmp(y)),
        (T::Int64(x), T::Float64(y)) => apply_op(op, (*x as f64).partial_cmp(y)),
        (T::Float64(x), T::Int64(y)) => apply_op(op, x.partial_cmp(&(*y as f64))),
        (T::Decimal128(x, xs), T::Decimal128(y, ys)) => {
            apply_op(op, Some(compare_decimal128(*x, *xs, *y, *ys)))
        }
        (T::Str(x), T::Str(y)) => apply_op(op, x.cmp(y).into()),
        (T::Bool(x), T::Bool(y)) => apply_op(op, x.cmp(y).into()),
        // Temporal types compare by their raw integer; mixed Date32/Timestamp
        // or differing timestamp units are not comparable.
        (T::Date32(x), T::Date32(y)) => apply_op(op, x.cmp(y).into()),
        (T::Timestamp(x, xu), T::Timestamp(y, yu)) if xu == yu => apply_op(op, x.cmp(y).into()),
        _ => false,
    }
}

pub(crate) fn compare_decimal128(a: i128, a_scale: u8, b: i128, b_scale: u8) -> std::cmp::Ordering {
    if a == 0 || b == 0 || a_scale == b_scale {
        return a.cmp(&b);
    }
    let negative = a.is_negative();
    match (negative, b.is_negative()) {
        (true, false) => return std::cmp::Ordering::Less,
        (false, true) => return std::cmp::Ordering::Greater,
        _ => {}
    }
    let scale = a_scale.max(b_scale) as usize;
    let a = a.unsigned_abs().to_string();
    let b = b.unsigned_abs().to_string();
    let a_len = a.len() + scale - a_scale as usize;
    let b_len = b.len() + scale - b_scale as usize;
    let order = a_len.cmp(&b_len).then_with(|| {
        let width = a_len.max(b_len);
        (0..width)
            .map(|i| a.as_bytes().get(i).copied().unwrap_or(b'0'))
            .cmp((0..width).map(|i| b.as_bytes().get(i).copied().unwrap_or(b'0')))
    });
    if negative {
        order.reverse()
    } else {
        order
    }
}

/// Apply an operator to an `Ordering`, treating `None` (NaN) as no-match.
fn apply_op(op: CompareOp, ord: Option<std::cmp::Ordering>) -> bool {
    use std::cmp::Ordering::*;
    let Some(ord) = ord else { return false };
    match op {
        CompareOp::Gt => ord == Greater,
        CompareOp::Lt => ord == Less,
        CompareOp::Ge => ord != Less,
        CompareOp::Le => ord != Greater,
        CompareOp::Eq => ord == Equal,
        CompareOp::Ne => ord != Equal,
    }
}

/// Parse a string literal into a `TypedValue` matching the operand's type.
/// Used by `CompareLiteral` to coerce the literal to the field's type.
fn typed_from_literal<'a>(
    sample: &crate::columnar::TypedValue<'a>,
    literal: &'a str,
) -> Option<crate::columnar::TypedValue<'a>> {
    use crate::columnar::TypedValue as T;
    match sample {
        T::Str(_) => Some(T::Str(literal)),
        T::Int64(_) => literal.parse::<i64>().ok().map(T::Int64),
        T::Float64(_) => literal.parse::<f64>().ok().map(T::Float64),
        T::Bool(_) => match literal {
            "true" | "True" | "TRUE" => Some(T::Bool(true)),
            "false" | "False" | "FALSE" => Some(T::Bool(false)),
            _ => None,
        },
        T::Date32(_) => literal.parse::<i32>().ok().map(T::Date32),
        T::Timestamp(_, unit) => literal
            .parse::<i64>()
            .ok()
            .map(|value| T::Timestamp(value, *unit)),
        T::Decimal128(_, scale) => crate::columnar::parse_decimal128(literal, *scale)
            .map(|value| T::Decimal128(value, *scale)),
    }
}

/// Return the minimum field ordinal for a predicate, used to order
/// `And`/`Or` operands by document position (C2). Higher ordinal = later
/// in the document. Returns `usize::MAX` for predicates without a
/// resolvable field (should evaluate last).
fn pred_ordinal(
    pred: &FilterPredicate,
    field_index: &HashMap<String, usize>,
    plan: &ExecutionPlan,
) -> usize {
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
        | FilterPredicate::Regex { field, .. } => field_index
            .get(resolve(field, plan))
            .copied()
            .unwrap_or(usize::MAX),
        FilterPredicate::Always(_) => usize::MAX,
        FilterPredicate::Compare {
            field_a, field_b, ..
        } => {
            let a = field_index
                .get(resolve(field_a, plan))
                .copied()
                .unwrap_or(usize::MAX);
            let b = field_index
                .get(resolve(field_b, plan))
                .copied()
                .unwrap_or(usize::MAX);
            a.min(b)
        }
        FilterPredicate::And(a, b) | FilterPredicate::Or(a, b) => {
            pred_ordinal(a, field_index, plan).min(pred_ordinal(b, field_index, plan))
        }
        FilterPredicate::Not(inner) => pred_ordinal(inner, field_index, plan),
    }
}
