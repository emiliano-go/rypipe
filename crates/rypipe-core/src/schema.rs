//! Frozen schema for parallel streaming.
//!
//! A `FrozenSchema` captures the output column layout after discovery or
//! explicit declaration.  Once yielded to the consumer it cannot change,
//! which is the correctness guarantee that makes parallel streaming safe:
//! chunk 1 cannot discover a column that chunk 7 later introduces.

use std::sync::Arc;

use rustc_hash::FxHashMap;

use crate::plan::{ExecutionPlan, FieldType};

/// Immutable, shared across all workers.  No lock, read-only after
/// construction.
#[derive(Clone, Debug)]
pub struct FrozenSchema {
    /// Output column names in final order.
    names: Vec<Arc<str>>,
    /// Raw input field name → output slot index.  `None` = dropped by plan.
    /// Collapses `field_map` (rename) + `drop_fields` + `field_index` into
    /// ONE lookup.
    index: FxHashMap<Box<str>, Option<u32>>,
    /// Output column types, parallel to `names`.
    types: Vec<FieldType>,
    /// `true` = full scan, `false` = sampled.  Affects the unknown-field
    /// message.
    exact: bool,
}

/// Options controlling schema discovery.
pub struct DiscoveryOpts {
    /// Files smaller than this are fully scanned (cheap, exact).
    pub full_scan_threshold: u64,
    /// Number of strided windows for large files.
    pub windows: usize,
    /// Bytes per window.
    pub window_bytes: usize,
}

impl Default for DiscoveryOpts {
    fn default() -> Self {
        Self {
            full_scan_threshold: 128 * 1024 * 1024, // 128 MiB
            windows: 16,
            window_bytes: 2 * 1024 * 1024, // 2 MiB
        }
    }
}

/// What to do when a field appears in the file but not in the schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnknownFieldPolicy {
    /// Return an error (default).
    Error,
    /// Ignore the field, count occurrences, report at end.
    Skip,
}

impl FrozenSchema {
    /// Build from explicit column names and an execution plan.
    ///
    /// Declares the exact output columns.  Fields in the file not listed
    /// here are governed by `UnknownFieldPolicy`.  Columns declared here
    /// but absent from the file become all-null.
    pub fn from_plan(names: &[&str], plan: &ExecutionPlan) -> Self {
        let mut index = FxHashMap::default();
        let mut types = Vec::with_capacity(names.len());

        for (slot, &name) in names.iter().enumerate() {
            // Collapse rename: if the plan renames `name` to something
            // else, we still map the *raw* name to the slot.  The caller
            // should pass the *output* names; renames are applied by the
            // plan already.
            let ty = plan.column_type(name);
            types.push(ty);
            index.insert(Box::from(name), Some(slot as u32));
        }

        FrozenSchema {
            names: names.iter().map(|n| Arc::from(*n)).collect(),
            index,
            types,
            exact: true,
        }
    }

    /// Build from discovered names (file order, then document order within
    /// a window).  Applies the execution plan's renames and drops.
    pub fn from_discovered(
        names_in_order: &[String],
        plan: &ExecutionPlan,
    ) -> Self {
        let mut index = FxHashMap::default();
        let mut out_names = Vec::new();
        let mut types = Vec::new();

        for name in names_in_order {
            // Apply rename
            let resolved = plan.resolve_field(name);
            match resolved {
                Some(resolved_name) => {
                    // Check if renamed name is dropped
                    if plan.drop_fields.contains(resolved_name) {
                        index.insert(Box::from(name.as_str()), None);
                        continue;
                    }
                    let slot = out_names.len() as u32;
                    let ty = plan.column_type(resolved_name);
                    index.insert(Box::from(name.as_str()), Some(slot));
                    out_names.push(Arc::from(resolved_name));
                    types.push(ty);
                }
                None => {
                    // Field is dropped by plan
                    index.insert(Box::from(name.as_str()), None);
                }
            }
        }

        FrozenSchema {
            names: out_names,
            index,
            types,
            exact: false,
        }
    }

    /// Number of output columns.
    pub fn num_columns(&self) -> usize {
        self.names.len()
    }

    /// Output column names in order.
    pub fn column_names(&self) -> &[Arc<str>] {
        &self.names
    }

    /// Output column types, parallel to `column_names`.
    pub fn column_types(&self) -> &[FieldType] {
        &self.types
    }

    /// Resolve a raw field name to its output slot, or `None` if dropped.
    #[inline]
    pub fn resolve(&self, raw_name: &str) -> Option<u32> {
        self.index.get(raw_name).copied().flatten()
    }

    /// Whether the schema was derived from a full scan (exact) or sampling.
    pub fn is_exact(&self) -> bool {
        self.exact
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_plan_explicit() {
        let plan = ExecutionPlan::new();
        let schema = FrozenSchema::from_plan(&["A", "B", "C"], &plan);
        assert_eq!(schema.num_columns(), 3);
        assert_eq!(schema.resolve("A"), Some(0));
        assert_eq!(schema.resolve("B"), Some(1));
        assert_eq!(schema.resolve("C"), Some(2));
        assert_eq!(schema.resolve("D"), None);
        assert!(schema.is_exact());
    }

    #[test]
    fn test_from_plan_with_drop() {
        let mut plan = ExecutionPlan::new();
        plan.drop_fields.insert("B".to_string());
        let schema = FrozenSchema::from_plan(&["A", "B", "C"], &plan);
        // from_plan does NOT drop — it declares the schema as-is.
        // Drops are handled at parse time via resolve() → None.
        assert_eq!(schema.num_columns(), 3);
    }

    #[test]
    fn test_from_discovered_with_rename() {
        let mut plan = ExecutionPlan::new();
        plan.field_map.insert("old".to_string(), "new".to_string());
        let names = vec!["old".to_string(), "keep".to_string()];
        let schema = FrozenSchema::from_discovered(&names, &plan);
        assert_eq!(schema.num_columns(), 2);
        assert_eq!(schema.resolve("old"), Some(0)); // slot 0 = "new"
        assert_eq!(schema.resolve("keep"), Some(1));
        assert_eq!(schema.column_names()[0].as_ref(), "new");
    }

    #[test]
    fn test_from_discovered_with_drop() {
        let mut plan = ExecutionPlan::new();
        plan.drop_fields.insert("drop_me".to_string());
        let names = vec!["keep".to_string(), "drop_me".to_string()];
        let schema = FrozenSchema::from_discovered(&names, &plan);
        assert_eq!(schema.num_columns(), 1);
        assert_eq!(schema.resolve("keep"), Some(0));
        assert_eq!(schema.resolve("drop_me"), None);
    }

    #[test]
    fn test_deterministic_order() {
        let plan = ExecutionPlan::new();
        let names = vec!["C".to_string(), "A".to_string(), "B".to_string()];
        let s1 = FrozenSchema::from_discovered(&names, &plan);
        let s2 = FrozenSchema::from_discovered(&names, &plan);
        assert_eq!(s1.column_names(), s2.column_names());
    }

    #[test]
    fn test_ensure_schema_preserves_existing_columns() {
        use crate::decoder::ColumnarSink;
        use crate::engine::TableBuilder;
        let plan = ExecutionPlan::new();
        let schema = FrozenSchema::from_plan(&["X", "Y", "Z"], &plan);
        let mut builder = TableBuilder::with_plan(100, Arc::new(plan));
        // Pre-add column "X" with a push.
        builder.begin_row();
        builder.put_field("X", crate::value::Value::Str("hello"));
        builder.end_row();
        let rows_before = builder.num_rows();
        let cols_before = builder.num_columns();
        // ensure_schema should add Y and Z, but not duplicate X.
        builder.ensure_schema(&schema).unwrap();
        assert_eq!(builder.num_rows(), rows_before);
        assert_eq!(builder.num_columns(), cols_before + 2);
        assert!(builder.column_names().contains(&"X".to_string()));
        assert!(builder.column_names().contains(&"Y".to_string()));
        assert!(builder.column_names().contains(&"Z".to_string()));
    }
}
