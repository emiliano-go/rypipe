use std::panic::{catch_unwind, AssertUnwindSafe};

use arrow::record_batch::RecordBatch;
use rayon::prelude::*;

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
    /// * Fast path (no `auto_dict` and no `Compare` filter): each chunk is
    ///   exported as its own `RecordBatch` and a vector of batches is returned.
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
                .unwrap_or_else(|_| {
                    Err(crate::Error::Merge(
                        "worker panicked during parallel parse".to_string(),
                    ))
                })
            })
            .collect();

        let engines: Vec<TableBuilder> = results.into_iter().collect::<Result<_>>()?;

        if engines.is_empty() {
            return Ok(Vec::new());
        }

        let plan = engines[0].plan.clone();

        // Fast path: no auto_dict and no post-reduce Compare filter.
        if !plan.auto_dict
            && !matches!(
                plan.filter,
                Some(crate::plan::FilterPredicate::Compare { .. })
            )
        {
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
