use arrow::datatypes::TimeUnit;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

/// The storage type for a column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldType {
    String,
    Int64,
    Float64,
    Boolean,
    Dictionary,
    /// Calendar date: days since the Unix epoch.
    Date32,
    /// Point in time with an explicit unit and an optional chrono format
    /// string for parsing non-ISO input (e.g. `timestamp[ms,format=%Y%m%d]`).
    Timestamp(TimeUnit, Option<Box<str>>),
    /// Fixed-precision decimal with given scale (default 18).
    Decimal128(u8),
}

impl std::str::FromStr for FieldType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "string" => Ok(FieldType::String),
            "int64" => Ok(FieldType::Int64),
            "float64" => Ok(FieldType::Float64),
            "bool" | "boolean" => Ok(FieldType::Boolean),
            "dictionary" => Ok(FieldType::Dictionary),
            "date32" => Ok(FieldType::Date32),
            s if s.starts_with("timestamp") => {
                parse_timestamp_spec(s).ok_or_else(|| format!("invalid timestamp spec: {s:?}"))
            }
            "decimal128" => Ok(FieldType::Decimal128(18)),
            s if s.starts_with("decimal128(") => {
                let scale = s
                    .trim_start_matches("decimal128(")
                    .trim_end_matches(')')
                    .parse::<u8>()
                    .map_err(|e| format!("invalid decimal128 scale: {e}"))?;
                Ok(FieldType::Decimal128(scale))
            }
            _ => Err(format!("unknown field type: {s:?}")),
        }
    }
}

/// Parse a timestamp type spec: `timestamp`, `timestamp[unit]`, or
/// `timestamp[unit,format=…]` (unit defaults to `us`; format is a chrono
/// format string tried before the built-in ISO layouts).
fn parse_timestamp_spec(s: &str) -> Option<FieldType> {
    let rest = s.strip_prefix("timestamp")?;
    if rest.is_empty() {
        return Some(FieldType::Timestamp(TimeUnit::Microsecond, None));
    }
    let inner = rest.strip_prefix('[')?.strip_suffix(']')?;
    let (unit_str, format) = match inner.split_once(',') {
        Some((unit, opt)) => {
            let fmt = opt.strip_prefix("format=")?;
            if fmt.is_empty() {
                return None;
            }
            (unit, Some(fmt))
        }
        None => {
            if let Some(fmt) = inner.strip_prefix("format=") {
                if fmt.is_empty() {
                    return None;
                }
                ("us", Some(fmt))
            } else {
                (inner, None)
            }
        }
    };
    let unit = match unit_str {
        "" | "us" | "µs" => TimeUnit::Microsecond,
        "s" => TimeUnit::Second,
        "ms" => TimeUnit::Millisecond,
        "ns" => TimeUnit::Nanosecond,
        _ => return None,
    };
    let format: Option<Box<str>> =
        format.and_then(|f| if f.is_empty() { None } else { Some(f.into()) });
    Some(FieldType::Timestamp(unit, format))
}

/// A compiled execution plan that controls field renaming, dropping,
/// type assignment, dictionary encoding, row filtering, and column ordering.
/// Default (empty) is a no-op.
#[derive(Clone, Default)]
pub struct ExecutionPlan {
    /// Map from raw field name to output column name.
    pub field_map: HashMap<String, String>,
    /// Set of raw field names to drop entirely.
    pub drop_fields: HashSet<String>,
    /// Explicit type overrides per output column name.
    pub field_types: HashMap<String, FieldType>,
    /// Set of output column names to dict-encode.
    pub dictionary_columns: HashSet<String>,
    /// Optional row filter predicate.
    pub filter: Option<FilterPredicate>,
    /// Desired output column order (names in order).  Columns not listed here
    /// appear after all listed columns in first-appearance order.  If empty,
    /// first-appearance order is used.
    pub schema_order: Vec<String>,
    /// When true, string columns with low cardinality are automatically
    /// upgraded to dictionary encoding during parse.
    pub auto_dict: bool,
    /// Auto-dict tuning: maximum fraction of rows allowed as distinct values
    /// for an upgrade. Defaults to `0.05` when `None`.
    pub dict_threshold: Option<f64>,
    /// Auto-dict tuning: maximum dictionary size (distinct values) allowed for
    /// an upgrade. Defaults to `256` when `None`.
    pub dict_max_size: Option<usize>,
    /// When true, a non-null value that fails to parse into its declared
    /// `field_types` type aborts the parse with an error instead of silently
    /// becoming null. Genuine nulls (missing fields) stay null regardless.
    pub strict_types: bool,
    /// Cap on the number of bounded-streaming batches/chunks. Defaults to
    /// `MAX_SPLIT_CHUNKS` (100,000) when `None`.
    pub max_split_chunks: Option<usize>,
    /// Optional row observer; hooks fire from parse threads during the read.
    pub observer: Option<std::sync::Arc<dyn crate::RowObserver>>,
}

impl std::fmt::Debug for ExecutionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionPlan")
            .field("field_map", &self.field_map)
            .field("drop_fields", &self.drop_fields)
            .field("field_types", &self.field_types)
            .field("dictionary_columns", &self.dictionary_columns)
            .field("filter", &self.filter)
            .field("schema_order", &self.schema_order)
            .field("auto_dict", &self.auto_dict)
            .field("dict_threshold", &self.dict_threshold)
            .field("dict_max_size", &self.dict_max_size)
            .field("strict_types", &self.strict_types)
            .field("max_split_chunks", &self.max_split_chunks)
            .field("observer", &self.observer.is_some())
            .finish()
    }
}

impl ExecutionPlan {
    pub fn new() -> Self {
        Self::default()
    }

    /// Rename a raw field to an output column name.
    pub fn rename(mut self, raw: impl Into<String>, output: impl Into<String>) -> Self {
        self.field_map.insert(raw.into(), output.into());
        self
    }

    /// Drop a single raw/output field.
    pub fn drop(mut self, field: impl Into<String>) -> Self {
        self.drop_fields.insert(field.into());
        self
    }

    /// Drop many raw/output fields.
    pub fn drop_many<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.drop_fields.extend(fields.into_iter().map(Into::into));
        self
    }

    /// Set the storage type for an output column.
    pub fn type_as(mut self, field: impl Into<String>, field_type: FieldType) -> Self {
        self.field_types.insert(field.into(), field_type);
        self
    }

    /// Dict-encode an output column.
    pub fn dictionary(mut self, field: impl Into<String>) -> Self {
        self.dictionary_columns.insert(field.into());
        self
    }

    /// Keep only rows where `field == value` (string comparison, per-row).
    pub fn filter_eq(mut self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.filter = Some(FilterPredicate::Equal {
            field: field.into(),
            value: value.into(),
        });
        self
    }

    /// Keep only rows where `field != value` (string comparison, per-row).
    pub fn filter_ne(mut self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.filter = Some(FilterPredicate::NotEqual {
            field: field.into(),
            value: value.into(),
        });
        self
    }

    /// Keep only rows where `field_a op field_b` (native-typed, per-row).
    pub fn filter_compare(
        mut self,
        field_a: impl Into<String>,
        op: CompareOp,
        field_b: impl Into<String>,
    ) -> Self {
        self.filter = Some(FilterPredicate::Compare {
            field_a: field_a.into(),
            op,
            field_b: field_b.into(),
        });
        self
    }

    /// Declare the output schema as a **projection + order**: the output
    /// contains exactly the listed columns, in the listed order.  Fields in
    /// the data but not listed are skipped during parsing (never
    /// materialized); listed columns absent from the data come out
    /// null-filled.  `field_map` renames apply before matching.
    pub fn schema_order<I, S>(mut self, order: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.schema_order = order.into_iter().map(Into::into).collect();
        self
    }

    /// Enable or disable automatic dictionary upgrade for low-cardinality
    /// string columns.
    pub fn with_auto_dict(mut self, yes: bool) -> Self {
        self.auto_dict = yes;
        self
    }

    /// Override the auto-dict distinct-ratio threshold (default `0.05`).
    pub fn with_dict_threshold(mut self, threshold: f64) -> Self {
        self.dict_threshold = Some(threshold);
        self
    }

    /// Override the auto-dict maximum dictionary size (default `256`).
    pub fn with_dict_max_size(mut self, max_size: usize) -> Self {
        self.dict_max_size = Some(max_size);
        self
    }

    /// Enable strict typing: a non-null value that cannot be parsed into its
    /// `field_types`-declared type becomes an error at `finish()` instead of
    /// a silent null.
    pub fn with_strict_types(mut self, yes: bool) -> Self {
        self.strict_types = yes;
        self
    }

    /// Override the bounded-streaming split cap (default 100,000 chunks).
    pub fn with_max_split_chunks(mut self, cap: usize) -> Self {
        self.max_split_chunks = Some(cap);
        self
    }

    /// Attach a row observer; hooks fire from parse threads during the read.
    pub fn with_observer(mut self, observer: std::sync::Arc<dyn crate::RowObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Determine the storage type for an output column name.
    pub fn column_type(&self, name: &str) -> FieldType {
        if let Some(ft) = self.field_types.get(name) {
            return ft.clone();
        }
        if self.dictionary_columns.contains(name) {
            return FieldType::Dictionary;
        }
        FieldType::String
    }

    /// Resolve a raw field name to its output column name.
    /// Returns `None` if the field should be dropped.
    ///
    /// Application order: rename first, then drop, then schema projection,
    /// matching left-to-right pipeline semantics.
    ///
    /// When `schema_order` is non-empty it acts as a **projection**: only the
    /// listed columns are materialized, so any other resolved name returns
    /// `None` here (which makes `ColumnarSink::wants`/`resolve` reject the
    /// field and lets adapters skip extracting it).  Fields referenced by the
    /// row filter are kept even when absent from `schema_order` so predicates
    /// still evaluate correctly; they are projected out of the final batch.
    pub fn resolve_field<'a>(&'a self, raw: &'a str) -> Option<&'a str> {
        let resolved = self.field_map.get(raw).map_or(raw, |s| s.as_str());
        if self.drop_fields.contains(resolved) {
            return None;
        }
        if !self.schema_order.is_empty()
            && !self.schema_order.iter().any(|n| n == resolved)
            && !self.filter_references(resolved)
        {
            return None;
        }
        Some(resolved)
    }

    /// Whether the row filter references `resolved` (a post-rename output
    /// name).  Used by [`Self::resolve_field`] to keep predicate fields out
    /// of the schema projection.
    fn filter_references(&self, resolved: &str) -> bool {
        fn matches(pred: &FilterPredicate, plan: &ExecutionPlan, resolved: &str) -> bool {
            // Rename only: drop/projection must not apply here (recursion).
            fn ren<'x>(plan: &'x ExecutionPlan, f: &'x String) -> &'x str {
                plan.field_map.get(f).map_or(f.as_str(), |s| s.as_str())
            }
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
                | FilterPredicate::Regex { field, .. } => ren(plan, field) == resolved,
                FilterPredicate::Always(_) => false,
                FilterPredicate::Compare {
                    field_a, field_b, ..
                } => ren(plan, field_a) == resolved || ren(plan, field_b) == resolved,
                FilterPredicate::And(a, b) | FilterPredicate::Or(a, b) => {
                    matches(a, plan, resolved) || matches(b, plan, resolved)
                }
                FilterPredicate::Not(inner) => matches(inner, plan, resolved),
            }
        }
        self.filter
            .as_ref()
            .is_some_and(|f| matches(f, self, resolved))
    }
}

/// Comparison operator for column-to-column filters (evaluated post-reduce).
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
            FilterPredicate::Strip { field, op, value } => {
                let actual = get_value(columns, field_index, field, plan, row_index);
                match actual {
                    Some(s) => {
                        let transformed = s.trim().to_string();
                        apply_op(*op, transformed.as_str().partial_cmp(value.as_str()))
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
                        let len = s.len() as f64;
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
/// operands are widened to f64; any other type mismatch yields false.
/// Nulls never reach here (checked by the caller).
fn compare_typed(
    a: &crate::columnar::TypedValue<'_>,
    op: CompareOp,
    b: &crate::columnar::TypedValue<'_>,
) -> bool {
    use crate::columnar::TypedValue as T;
    // Numeric promotion: Int64 <-> Float64.
    let num_pair: Option<(f64, f64)> = match (a, b) {
        (T::Int64(x), T::Int64(y)) => Some((*x as f64, *y as f64)),
        (T::Float64(x), T::Float64(y)) => Some((*x, *y)),
        (T::Int64(x), T::Float64(y)) | (T::Float64(y), T::Int64(x)) => Some((*x as f64, *y)),
        _ => None,
    };
    if let Some((x, y)) = num_pair {
        return apply_op(op, x.partial_cmp(&y));
    }
    match (a, b) {
        (T::Str(x), T::Str(y)) => apply_op(op, x.cmp(y).into()),
        (T::Bool(x), T::Bool(y)) => apply_op(op, x.cmp(y).into()),
        // Temporal types compare by their raw integer; mixed Date32/Timestamp
        // or differing timestamp units are not comparable.
        (T::Date32(x), T::Date32(y)) => apply_op(op, x.cmp(y).into()),
        (T::Timestamp(x), T::Timestamp(y)) => apply_op(op, x.cmp(y).into()),
        _ => false,
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
        T::Timestamp(_) => literal.parse::<i64>().ok().map(T::Timestamp),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_spec_plain() {
        assert_eq!(
            "timestamp".parse::<FieldType>(),
            Ok(FieldType::Timestamp(TimeUnit::Microsecond, None))
        );
        assert_eq!(
            "timestamp[ns]".parse::<FieldType>(),
            Ok(FieldType::Timestamp(TimeUnit::Nanosecond, None))
        );
    }

    #[test]
    fn test_timestamp_spec_format() {
        assert_eq!(
            "timestamp[ms,format=%Y%m%d %H:%M]".parse::<FieldType>(),
            Ok(FieldType::Timestamp(
                TimeUnit::Millisecond,
                Some("%Y%m%d %H:%M".into())
            ))
        );
        // Unit omitted: defaults to microseconds.
        assert_eq!(
            "timestamp[format=%d/%m/%Y]".parse::<FieldType>(),
            Ok(FieldType::Timestamp(
                TimeUnit::Microsecond,
                Some("%d/%m/%Y".into())
            ))
        );
        // Unknown unit or unknown option key: rejected.
        assert!("timestamp[fortnights]".parse::<FieldType>().is_err());
        assert!("timestamp[us,color=red]".parse::<FieldType>().is_err());
        assert!("timestamp[us,format=]".parse::<FieldType>().is_err());
    }
}
