use std::panic::{catch_unwind, AssertUnwindSafe};

use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use rustc_hash::FxHashMap as HashMap;

use crate::arrow_export::apply_compare_filter;
use crate::decoder::{split_points_to_ranges, RecordParser, Splitter};
use crate::engine::TableBuilder;
use crate::merge::engines_to_record_batches;
use crate::plan::ExecutionPlan;
use crate::Result;

/// Parallel executor that splits an input byte slice into chunks, parses each
/// chunk independently, and returns the resulting Arrow record batches.
pub struct ParallelExecutor;

impl ParallelExecutor {
    /// Parse `bytes` in parallel using `splitter` and `parser`.
    ///
    /// * Fast path (no `auto_dict`): each chunk is exported as its own
    ///   `RecordBatch` and a vector of batches is returned. Row filters
    ///   (`Equal`/`NotEqual`/`Compare`) are applied during parse per chunk.
    /// * Merge path: chunk builders are merged sequentially so that
    ///   auto-dictionary upgrades see the full cardinality, then a single
    ///   `RecordBatch` is returned inside the vector.
    pub fn parse<P>(
        bytes: &[u8],
        splitter: &dyn Splitter,
        parser: P,
        plan: ExecutionPlan,
        num_chunks: usize,
    ) -> Result<Vec<RecordBatch>>
    where
        P: RecordParser + Clone + Send + Sync,
    {
        let split_points = splitter.find_split_points(bytes, num_chunks);
        let ranges = split_points_to_ranges(&split_points, bytes.len());

        let results: Vec<Result<TableBuilder>> = ranges
            .into_par_iter()
            .map(|range| {
                catch_unwind(AssertUnwindSafe(|| {
                    let est = if !range.is_empty() {
                        (range.len() / 512).max(64)
                    } else {
                        64
                    };
                    let mut sink = TableBuilder::with_plan(est, plan.clone());
                    parser.validate(&bytes[range.clone()])?;
                    parser.parse_chunk(&bytes[range.clone()], &mut sink)?;
                    Ok(sink)
                }))
                .unwrap_or_else(|payload| {
                    let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                        (*s).to_string()
                    } else if let Some(s) = payload.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "unknown panic".to_string()
                    };
                    Err(crate::Error::Merge(format!(
                        "worker panicked during parallel parse: {msg}"
                    )))
                })
            })
            .collect();

        let engines: Vec<TableBuilder> = results.into_iter().collect::<Result<_>>()?;

        if engines.is_empty() {
            return Ok(Vec::new());
        }

        let plan = engines[0].plan.clone();

        // Fast path: no auto_dict and all chunks agree on column types.
        // Compare filters are evaluated per-row during parse, so they do not
        // force the merge path. Schema-inconsistent chunks fall through to the
        // merge path, which surfaces a precise `Error::Merge` for irreconcilable
        // type mismatches instead of an opaque Arrow schema error.
        if !plan.auto_dict && schemas_consistent(&engines) {
            return engines_to_record_batches(engines, &plan);
        }

        // Merge path.
        let mut merged = TableBuilder::with_plan(engines.len().max(64) * 512, plan.clone());
        for engine in engines {
            merged.extend(engine)?;
        }
        let batch = merged.finish()?;
        if let Some(ref filter) = plan.filter {
            return Ok(vec![apply_compare_filter(batch, filter)?]);
        }
        Ok(vec![batch])
    }
}

/// True when every chunk builder agrees on each column's storage variant.
/// Chunks missing a column entirely are fine (the export path null-fills).
fn schemas_consistent(engines: &[TableBuilder]) -> bool {
    let Some(first) = engines.first() else {
        return true;
    };
    let base: HashMap<&str, &str> = first
        .field_index
        .iter()
        .map(|(name, &idx)| (name.as_str(), first.columns[idx].variant_key()))
        .collect();
    engines[1..].iter().all(|e| {
        e.field_index.iter().all(|(name, &idx)| {
            let b = &e.columns[idx];
            match base.get(name.as_str()) {
                Some(key) => *key == b.variant_key(),
                None => true,
            }
        })
    })
}
