use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use rustc_hash::FxHashMap as HashMap;

use crate::arrow_export::apply_compare_filter;
use crate::decoder::{split_points_to_ranges, RecordParser, Splitter};
use crate::engine::TableBuilder;
use crate::merge::engines_to_record_batches;
use crate::plan::ExecutionPlan;
use crate::Result;

// --- Chunk-time profiling ---
// Reset before each parallel run; read via `chunk_profile()`.
static CHUNK_TIME_SUM_NS: AtomicU64 = AtomicU64::new(0);
static CHUNK_TIME_MAX_NS: AtomicU64 = AtomicU64::new(0);
static CHUNK_COUNT: AtomicU64 = AtomicU64::new(0);
static SPLIT_SCAN_NS: AtomicU64 = AtomicU64::new(0);

/// Reset profiling counters (call before a parallel run).
pub fn reset_chunk_profile() {
    CHUNK_TIME_SUM_NS.store(0, Ordering::Relaxed);
    CHUNK_TIME_MAX_NS.store(0, Ordering::Relaxed);
    CHUNK_COUNT.store(0, Ordering::Relaxed);
    SPLIT_SCAN_NS.store(0, Ordering::Relaxed);
}

/// Read the chunk-time profile as (split_scan_ns, sum_ns, max_ns, count).
pub fn chunk_profile() -> (u64, u64, u64, u64) {
    (
        SPLIT_SCAN_NS.load(Ordering::Relaxed),
        CHUNK_TIME_SUM_NS.load(Ordering::Relaxed),
        CHUNK_TIME_MAX_NS.load(Ordering::Relaxed),
        CHUNK_COUNT.load(Ordering::Relaxed),
    )
}

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
        plan: Arc<ExecutionPlan>,
        num_chunks: usize,
    ) -> Result<Vec<RecordBatch>>
    where
        P: RecordParser + Clone + Send + Sync,
    {
        reset_chunk_profile();
        let t_split = Instant::now();
        let split_points = splitter.find_split_points(bytes, num_chunks);
        SPLIT_SCAN_NS.store(t_split.elapsed().as_nanos() as u64, Ordering::Relaxed);
        let ranges = split_points_to_ranges(&split_points, bytes.len());

        let est_row = splitter
            .estimate_bytes_per_row(&bytes[..bytes.len().min(65536)])
            .max(512);
        let results: Vec<Result<TableBuilder>> = ranges
            .into_par_iter()
            .map(|range| {
                let t_chunk = Instant::now();
                let result = catch_unwind(AssertUnwindSafe(|| {
                    let est = if !range.is_empty() {
                        (range.len() / est_row).max(64)
                    } else {
                        64
                    };
                    let mut sink = TableBuilder::with_plan(est, Arc::clone(&plan));
                    parser.validate(&bytes[range.clone()])?;
                    parser.parse_chunk_generic(&bytes[range.clone()], &mut sink)?;
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
                });
                // Record chunk timing for get_par_profile()
                let elapsed_ns = t_chunk.elapsed().as_nanos() as u64;
                CHUNK_TIME_SUM_NS.fetch_add(elapsed_ns, Ordering::Relaxed);
                CHUNK_COUNT.fetch_add(1, Ordering::Relaxed);
                // Atomic max: CAS loop
                loop {
                    let prev = CHUNK_TIME_MAX_NS.load(Ordering::Relaxed);
                    if elapsed_ns <= prev {
                        break;
                    }
                    if CHUNK_TIME_MAX_NS
                        .compare_exchange_weak(
                            prev,
                            elapsed_ns,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
                result
            })
            .collect();

        let mut engines: Vec<TableBuilder> = results.into_iter().collect::<Result<_>>()?;

        if engines.is_empty() {
            return Ok(Vec::new());
        }

        let plan = Arc::clone(&engines[0].plan);

        // Fast path: no auto_dict and all chunks agree on column types.
        // Compare filters are evaluated per-row during parse, so they do not
        // force the merge path. Schema-inconsistent chunks fall through to the
        // merge path, which surfaces a precise `Error::Merge` for irreconcilable
        // type mismatches instead of an opaque Arrow schema error.
        if !plan.auto_dict && schemas_consistent(&engines) {
            return engines_to_record_batches(engines, &plan);
        }

        // Incremental dict path for auto_dict: per-chunk upgrade in parallel,
        // then unify dictionaries (tiny serial) and remap, staying on fast path.
        if plan.auto_dict {
            // Per-chunk upgrade in parallel (was serial after merge)
            engines.par_iter_mut().for_each(|e| e.auto_dict_upgrade());
            if schemas_consistent(&engines) {
                // Find first dict column that needs unification across chunks.
                let mut unify_col: Option<String> = None;
                for col_name in &engines[0].column_order.clone() {
                    let first_idx = engines[0].field_index.get(col_name).copied();
                    if let Some(idx) = first_idx {
                        if engines[0].columns[idx].variant_key() == "dictionary" {
                            let first_data =
                                if let crate::columnar::ColumnBuilder::Dictionary { data, offsets, .. } =
                                    &engines[0].columns[idx]
                                {
                                    (data, offsets)
                                } else {
                                    continue;
                                };
                            for e in &engines[1..] {
                                if let Some(&j) = e.field_index.get(col_name) {
                                    if let crate::columnar::ColumnBuilder::Dictionary {
                                        data, offsets, ..
                                    } = &e.columns[j]
                                    {
                                        if data != first_data.0 || offsets != first_data.1 {
                                            unify_col = Some(col_name.clone());
                                            break;
                                        }
                                    }
                                }
                            }
                            if unify_col.is_some() {
                                break;
                            }
                        }
                    }
                }
                if unify_col.is_none() {
                    return engines_to_record_batches(engines, &plan);
                }
                let col_name = unify_col.unwrap();
                // Build seed from first chunk, unify, remap.
                let first_data =
                    if let crate::columnar::ColumnBuilder::Dictionary { data, offsets, .. } =
                        &engines[0].columns[engines[0].field_index[&col_name]]
                    {
                        (data.clone(), offsets.clone())
                    } else {
                        return engines_to_record_batches(engines, &plan);
                    };
                // Build SeedDict from the contiguous buffer
                let mut seed_values = Vec::new();
                let mut seed_offsets = Vec::with_capacity(first_data.1.len());
                seed_offsets.push(0);
                for i in 0..(first_data.1.len() - 1) {
                    let start = first_data.1[i] as usize;
                    let end = first_data.1[i + 1] as usize;
                    seed_values.extend_from_slice(&first_data.0[start..end]);
                    seed_offsets.push(seed_values.len() as i32);
                }
                let seed = crate::dict::SeedDict {
                    values: seed_values,
                    offsets: seed_offsets,
                    index: rustc_hash::FxHashMap::default(), // Will be populated by unify
                };
                let col_refs: Vec<&crate::columnar::ColumnBuilder> = engines
                    .iter()
                    .filter_map(|e| e.field_index.get(&col_name).map(|&idx| &e.columns[idx]))
                    .collect();
                let (unified_dict, remaps) = crate::dict::unify_dictionaries(&seed, &col_refs);
                // Build unified data+offsets+index for replace_dict.
                let mut new_index: rustc_hash::FxHashMap<Box<str>, i32> = rustc_hash::FxHashMap::default();
                for i in 0..(unified_dict.offsets.len() - 1) {
                    let start = unified_dict.offsets[i] as usize;
                    let end = unified_dict.offsets[i + 1] as usize;
                    let s = std::str::from_utf8(&unified_dict.data[start..end]).unwrap_or("");
                    let k: Box<str> = s.into();
                    new_index.insert(k, i as i32);
                }
                // Apply remap to each chunk, then replace dict.
                for (e, remap) in engines.iter_mut().zip(remaps.iter()) {
                    if let Some(idx) = e.field_index.get(&col_name).copied() {
                        if let Some(codes) = e.columns[idx].dict_codes_mut() {
                            codes.remap_codes(&remap.map);
                        }
                        e.columns[idx].replace_dict(
                            unified_dict.data.clone(),
                            unified_dict.offsets.clone(),
                            new_index.clone(),
                        );
                    }
                }
                return engines_to_record_batches(engines, &plan);
            }
        }

        // Merge path.
        let mut merged = TableBuilder::with_plan(engines.len().max(64) * 512, Arc::clone(&plan));
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
