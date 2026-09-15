use arrow::datatypes::TimeUnit;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

mod predicate;
pub(crate) use predicate::compare_decimal128;
pub use predicate::{ArithOp, CompareOp, FilterPredicate, RegexSpec, TrimMode};

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
                let inner = s
                    .strip_prefix("decimal128(")
                    .and_then(|v| v.strip_suffix(')'))
                    .ok_or_else(|| format!("invalid decimal128 spec: {s:?}"))?;
                let scale = inner
                    .parse::<u8>()
                    .map_err(|e| format!("invalid decimal128 scale: {e}"))?;
                if scale > 38 {
                    return Err(format!("decimal128 scale exceeds 38: {scale}"));
                }
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
        assert_eq!(
            "timestamp[format=%d/%m/%Y]".parse::<FieldType>(),
            Ok(FieldType::Timestamp(
                TimeUnit::Microsecond,
                Some("%d/%m/%Y".into())
            ))
        );
        assert!("timestamp[fortnights]".parse::<FieldType>().is_err());
        assert!("timestamp[us,color=red]".parse::<FieldType>().is_err());
        assert!("timestamp[us,format=]".parse::<FieldType>().is_err());
    }

    #[test]
    fn decimal128_spec_requires_closing_delimiter_and_valid_scale() {
        assert_eq!(
            "decimal128(38)".parse::<FieldType>(),
            Ok(FieldType::Decimal128(38))
        );
        assert!("decimal128(38".parse::<FieldType>().is_err());
        assert!("decimal128(39)".parse::<FieldType>().is_err());
    }
}
