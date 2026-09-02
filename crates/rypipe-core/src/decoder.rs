use std::ops::Range;

use arrow::record_batch::RecordBatch;

use crate::value::Value;
use crate::Result;

// ---------------------------------------------------------------------------
// S4: Chunk-size floor
// ---------------------------------------------------------------------------

/// Minimum chunk size in bytes. Sub-MB chunks collapse throughput due to
/// per-chunk fixed cost (thread dispatch, cache cold start). Measured:
/// 100 MB at par128 (0.78 MB chunks) = 2,265 MB/s vs par16 (6.25 MB) = 3,735.
pub const MIN_CHUNK_BYTES: usize = 2 << 20; // 2 MiB

/// Maximum number of split chunks. Above this, scheduling overhead dominates.
pub const MAX_SPLIT_CHUNKS: usize = 1024;

/// Mode for chunk planning — parallel and streaming have different optima.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SplitMode {
    /// Parallel: peak at 4 MB chunks (measured on 533 MB, 16 cores).
    Parallel,
    /// Streaming: peak at 2 MB chunks (measured with bounded memory).
    Streaming,
}

/// Plan the number of chunks given input size, thread count, and mode.
/// Returns a value in `[threads, MAX_SPLIT_CHUNKS]` with a 2 MiB floor.
pub fn plan_chunk_count(bytes: usize, threads: usize, mode: SplitMode) -> usize {
    let by_size = bytes / MIN_CHUNK_BYTES.max(1);
    let cap = match mode {
        SplitMode::Parallel => 16 * threads,
        SplitMode::Streaming => 8 * threads,
    };
    by_size.min(cap).max(threads).min(MAX_SPLIT_CHUNKS)
}

// ---------------------------------------------------------------------------
// S3: SkipRegionFinder — byte ranges where a candidate boundary is invalid
// ---------------------------------------------------------------------------

/// Finds byte ranges (comments, CDATA, quoted fields, string literals) where
/// a candidate record boundary must be rejected.  The engine calls this
/// during split-point discovery to avoid splitting inside comments or strings.
pub trait SkipRegionFinder: Send + Sync {
    /// Openers that start a skip region (e.g. `b"<!--"`, `b"<![CDATA["`).
    fn openers(&self) -> &[&'static [u8]];

    /// The closer for a given opener (e.g. `"-->"` for `"<!--"`).
    fn closer_for(&self, opener: &[u8]) -> &'static [u8];

    /// Maximum backward scan window in bytes.  Default 64 KiB — sufficient
    /// for most XML/CSV comments and JSON string literals.
    fn window(&self) -> usize {
        64 * 1024
    }
}

/// Check whether byte position `at` falls inside a skip region by scanning
/// backward up to `finder.window()` bytes, looking for an unclosed opener.
///
/// Returns `false` immediately when the file contains none of the openers
/// — check that once per chunk, not per candidate.
pub fn in_skip_region(bytes: &[u8], at: usize, finder: &dyn SkipRegionFinder) -> bool {
    let openers = finder.openers();
    if openers.is_empty() {
        return false;
    }
    let scan_start = at.saturating_sub(finder.window());
    let scan = &bytes[scan_start..at];
    for opener in openers {
        let closer = finder.closer_for(opener);
        // Use memchr::memmem::find directly (no Finder::new preprocessing).
        let mut search = 0;
        while let Some(rel) = memchr::memmem::find(&scan[search..], opener) {
            let open_pos = scan_start + search + rel;
            // Check if there's a closer between the opener and `at`.
            let between = &bytes[open_pos + opener.len()..at];
            if memchr::memmem::find(between, closer).is_none() {
                // Unclosed opener — `at` is inside this skip region.
                return true;
            }
            search += rel + 1;
        }
    }
    false
}

// ---------------------------------------------------------------------------
// S1: Splitter trait with default
// ---------------------------------------------------------------------------

/// Format-specific splitter: decides where it is safe to divide an input byte
/// stream into independent chunks.
///
/// The only required method is `next_record_start` — adapters provide the
/// record boundary logic, and the engine handles chunk planning, skip-region
/// rejection, and deduplication.
pub trait Splitter: Send + Sync {
    /// Return the next record boundary at or after `from`.  Must return a
    /// position where a record starts, or `None` if no more records exist.
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize>;

    /// Estimate the average bytes per record from a sample of the input.
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize;

    /// Optional: byte ranges where a candidate boundary must be rejected.
    /// See `in_skip_region`.
    fn skip_regions(&self) -> Option<&dyn SkipRegionFinder> {
        None
    }

    /// Find split points.  **Do not override** without a measured reason.
    ///
    /// Default implementation:
    /// 1. Nominal offsets at `bytes.len() * i / n` for `i in 1..n`.
    /// 2. `par_iter` over nominals (rayon), each calling `next_record_start`.
    /// 3. Reject candidates inside skip regions via `in_skip_region`.
    /// 4. Dedup, sort, prepend 0.
    /// 5. Apply chunk floor via `plan_chunk_count`.
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize> {
        if max_chunks <= 1 || bytes.is_empty() {
            return vec![0, bytes.len()];
        }
        let n = plan_chunk_count(bytes.len(), 1, SplitMode::Parallel).min(max_chunks);
        let nominals: Vec<usize> = (1..n).map(|i| bytes.len() * i / n).collect();

        use rayon::prelude::*;

        let skip = self.skip_regions();
        let has_skip = skip.is_some();

        let mut points: Vec<usize> = nominals
            .par_iter()
            .filter_map(|&approx| {
                let pos = self.next_record_start(bytes, approx)?;
                // Reject candidates inside skip regions.
                if has_skip && in_skip_region(bytes, pos, skip.unwrap()) {
                    return None;
                }
                Some(pos)
            })
            .collect();

        points.sort_unstable();
        points.dedup();
        points.insert(0, 0);
        if *points.last().unwrap_or(&0) != bytes.len() {
            points.push(bytes.len());
        }
        points
    }
}

/// Format-specific parser: turns a byte chunk into field/value events sent to
/// a `ColumnarSink`.
pub trait RecordParser: Send + Sync {
    /// Validate that the whole byte slice is well-formed for this format.
    fn validate(&self, bytes: &[u8]) -> Result<()>;

    /// Parse one chunk and feed all row events into `sink` (object-safe, virtual).
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()>;

    /// Parse one chunk generically (monomorphized, inlinable).
    ///
    /// Default impl delegates to `parse_chunk` (object-safe) for adapters that
    /// have not yet migrated. Adapters that implement this directly get
    /// devirtualized `sink` calls and cross-crate inlining.
    #[inline]
    fn parse_chunk_generic<S: ColumnarSink>(&self, bytes: &[u8], sink: &mut S) -> Result<()>
    where
        Self: Sized,
    {
        // Default impl delegates to the object-safe path. Adapters that override
        // this method get devirtualized sink calls and cross-crate inlining.
        self.parse_chunk(bytes, sink as &mut dyn ColumnarSink)
    }
}

/// Sink for decoder events.  The decoder calls `begin_row` / `put_field` /
/// `end_row` for each record.
pub trait ColumnarSink {
    fn begin_row(&mut self);
    fn put_field(&mut self, name: &str, value: Value<'_>);

    /// Push a complete row.
    ///
    /// The default preserves event-oriented semantics. Sinks may override this
    /// to optimize row handling while preserving filtering and duplicate-field
    /// behavior.
    #[inline]
    fn put_row(&mut self, fields: &[(&str, Value<'_>)]) {
        for (name, value) in fields {
            self.put_field(name, value.clone());
        }
    }

    /// Resolve a valid raw name without allocating a UTF-8 `String`.
    #[inline]
    fn resolve_raw<'a>(&'a self, raw_name: &'a [u8]) -> Option<&'a str> {
        std::str::from_utf8(raw_name)
            .ok()
            .and_then(|name| self.resolve(name))
    }

    /// Push a value for a field whose name is still in its raw byte form.
    #[inline]
    fn resolve_and_put_raw(&mut self, raw_name: &[u8], value: Value<'_>) {
        if let Ok(name) = std::str::from_utf8(raw_name) {
            self.resolve_and_put(name, value);
        }
    }

    fn end_row(&mut self);

    /// Return `false` to signal that the engine will drop this field.
    #[inline]
    fn wants(&self, _name: &str) -> bool {
        true
    }

    /// Resolve a raw field name to its output column name, or `None` if dropped.
    /// Default keeps the name as-is. Adapters that do expensive extraction can
    /// call this once and then `put_field_resolved` to avoid a second
    /// `resolve_field` inside `put_field`.
    #[inline]
    fn resolve<'a>(&'a self, name: &'a str) -> Option<&'a str> {
        Some(name)
    }

    /// Push a field that has already been resolved via `resolve`.
    /// Default delegates to `put_field` (which will resolve again). Overrides
    /// should bypass the rename/drop lookup.
    #[inline]
    fn put_field_resolved(&mut self, resolved_name: &str, value: Value<'_>) {
        self.put_field(resolved_name, value);
    }

    /// Push a value directly to a known slot index, bypassing all name
    /// resolution, hash lookups, and attribute scanning.  Used by adapters
    /// that have verified the field identity via `expect_slot` + memcmp.
    ///
    /// Default: delegates to `put_field` with a synthetic name (slow path).
    /// Engines that track slot indices should override this.
    #[inline]
    fn put_field_at(&mut self, _slot: u32, value: Value<'_>) {
        // Fallback: can't push without a name; discard.
        // Engines override this to push directly to columns[slot].
        let _ = value;
    }

    /// Resolve a field name and push it in one call, avoiding the double
    /// `resolve_field` that `wants` + `put_field` would perform.
    /// Default: resolve then put_field_resolved (with owned clone to satisfy
    /// the borrow checker). Overrides should avoid the allocation.
    #[inline]
    fn resolve_and_put(&mut self, name: &str, value: Value<'_>) {
        if let Some(resolved) = self.resolve(name) {
            let owned = resolved.to_owned();
            self.put_field_resolved(&owned, value);
        }
    }

    /// Whether this sink needs decoded values from the parser.
    ///
    /// Return `false` for locate-only sinks that walk rows and resolve field
    /// names but skip value extraction entirely.  The parser can use this to
    /// bypass expensive text extraction (e.g. `raw_text_until` in XML scanners).
    #[inline]
    fn needs_value(&self) -> bool {
        true
    }

    /// Whether this sink needs field-name resolution (`wants` + `resolve`).
    ///
    /// Return `false` for traversal-only sinks that walk rows and find field
    /// extents but don't need to resolve names or check `wants`.  When false,
    /// the parser skips `wants()` and `resolve()` calls entirely — it only
    /// locates the byte extents of each field within a row.
    ///
    /// **Note:** this method is primarily for benchmarking/profiling harnesses.
    /// A sink returning `false` may receive `put_field` calls with unresolved
    /// names, or none at all, depending on the adapter.  Consider this
    /// experimental until a non-benchmark consumer exists.
    #[doc(hidden)]
    #[inline]
    fn needs_resolve(&self) -> bool {
        true
    }

    /// Whether the current row has been rejected by the filter and the scanner
    /// should skip the remainder of the row. Used for predicate-first
    /// deferred materialization: once `Fail`, the scanner can byte-jump to
    /// `</Details>` without further field scanning. Default false.
    #[inline]
    fn row_rejected(&self) -> bool {
        false
    }

    /// Whether every wanted column for the current row already has a value.
    /// The adapter can byte-jump to the row close tag when this returns true,
    /// skipping remaining fields.  Composes with `row_rejected` (a row can
    /// be satisfied OR rejected, both short-circuit).
    ///
    /// Default: false (never short-circuit).
    #[inline]
    fn row_satisfied(&self) -> bool {
        false
    }

    /// Bitmask of columns that the projection wants.  Bit `i` is set iff
    /// column `i` is in the output schema.  The adapter can test membership
    /// with `(wanted_mask >> slot) & 1 == 1` instead of a virtual call per
    /// field.  Returns empty mask (all zeros) when no projection is active.
    #[inline]
    fn wanted_mask(&self) -> u64 {
        0
    }

    /// After the layout is learned, the expected (slot, raw name bytes) at
    /// this ordinal position in the record.  The adapter can compare the
    /// raw bytes in-place (one memcmp) instead of running the full
    /// attribute scan → UTF-8 decode → hash → lookup path.
    ///
    /// Return `None` to fall back to the generic path.  Default: `None`.
    #[inline]
    fn expect_slot(&self, _ordinal: u32) -> Option<(u32, &[u8])> {
        None
    }

    /// Adapter reports the slot it resolved for `ordinal` so the engine
    /// can cache it for subsequent rows.  Called after the adapter resolves
    /// a field via the generic path (wants + resolve + put_field_resolved).
    #[inline]
    fn record_slot(&mut self, _ordinal: u32, _slot: u32, _raw_name: &[u8]) {}

    /// Adapter reports that the layout expected at `ordinal` did not match.
    /// The engine should invalidate its cached expectation and fall back to
    /// generic resolution for subsequent rows.
    #[inline]
    fn layout_broken(&mut self, _ordinal: u32) {}

    /// Reset the ordinal counter for child-element tracking.  Called after
    /// row-tag attributes are emitted so child ordinals start at 0.
    #[inline]
    fn reset_child_ordinal(&mut self) {}

    /// Finalize the sink into an Arrow `RecordBatch`.
    fn finish(&mut self) -> Result<RecordBatch>;
}

/// Convert split-point offsets returned by a `Splitter` into non-empty
/// `Range<usize>` chunks.
pub fn split_points_to_ranges(points: &[usize], len: usize) -> Vec<Range<usize>> {
    if points.len() < 2 {
        return std::iter::once(0..len).collect();
    }
    points
        .windows(2)
        .filter_map(|w| {
            let (start, end) = (w[0], w[1]);
            if start < end {
                Some(start..end)
            } else {
                None
            }
        })
        .collect()
}
