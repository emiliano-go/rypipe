use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

/// The storage type for a column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldType {
    String,
    Int64,
    Float64,
    Boolean,
    Dictionary,
}

impl FieldType {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "string" => Some(FieldType::String),
            "int64" => Some(FieldType::Int64),
            "float64" => Some(FieldType::Float64),
            "bool" | "boolean" => Some(FieldType::Boolean),
            "dictionary" => Some(FieldType::Dictionary),
            _ => None,
        }
    }
}

/// A compiled execution plan that controls field renaming, dropping,
/// type assignment, dictionary encoding, row filtering, and column ordering.
/// Default (empty) is a no-op.
#[derive(Clone, Debug, Default)]
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

    /// Keep only rows where `field_a op field_b` (post-reduce via Arrow compute).
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

    /// Set the output column order. Columns not listed appear after the listed
    /// ones in first-appearance order.
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
    /// Application order: rename first, then drop, matching left-to-right
    /// pipeline semantics.
    pub fn resolve_field<'a>(&'a self, raw: &'a str) -> Option<&'a str> {
        let resolved = self.field_map.get(raw).map_or(raw, |s| s.as_str());
        if self.drop_fields.contains(resolved) {
            return None;
        }
        Some(resolved)
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

impl CompareOp {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            ">" | "gt" => Some(CompareOp::Gt),
            "<" | "lt" => Some(CompareOp::Lt),
            ">=" | "ge" => Some(CompareOp::Ge),
            "<=" | "le" => Some(CompareOp::Le),
            "==" | "eq" => Some(CompareOp::Eq),
            "!=" | "ne" => Some(CompareOp::Ne),
            _ => None,
        }
    }
}

/// A filter predicate evaluated per-row during parsing.
#[derive(Clone, Debug, PartialEq)]
pub enum FilterPredicate {
    /// Keep row if `field_value != value` (string comparison, per-row).
    NotEqual { field: String, value: String },
    /// Keep row if `field_value == value` (string comparison, per-row).
    Equal { field: String, value: String },
    /// Column-to-column comparison evaluated post-reduce via Arrow compute.
    Compare {
        field_a: String,
        op: CompareOp,
        field_b: String,
    },
}

impl FilterPredicate {
    /// Check whether a partial row passes the per-row part of the filter.
    /// `columns` contains all builders; `row_index` is the current row number.
    /// Returns true to keep the row.  Compare filters always return true here
    /// because they are evaluated after all rows have been assembled.
    pub(crate) fn check(
        &self,
        columns: &HashMap<String, crate::columnar::ColumnBuilder>,
        row_index: usize,
        plan: &ExecutionPlan,
    ) -> bool {
        let (field, expected) = match self {
            FilterPredicate::NotEqual { field, value } => (field, value),
            FilterPredicate::Equal { field, value } => (field, value),
            FilterPredicate::Compare { .. } => return true,
        };
        // Resolve the filter field name: if renamed, use the new name.
        let resolved = plan.field_map.get(field).map_or(field, |s| s);

        let actual = columns
            .get(resolved)
            .and_then(|b| b.get_filter_value(row_index));
        let actual = actual.as_deref();
        match self {
            FilterPredicate::NotEqual { .. } => actual != Some(expected),
            FilterPredicate::Equal { .. } => actual == Some(expected),
            FilterPredicate::Compare { .. } => true,
        }
    }
}
