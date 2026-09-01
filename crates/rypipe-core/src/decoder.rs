use std::ops::Range;

use arrow::record_batch::RecordBatch;

use crate::value::Value;
use crate::Result;

/// Format-specific splitter: decides where it is safe to divide an input byte
/// stream into independent chunks.
pub trait Splitter: Send + Sync {
    /// Return sorted byte offsets where the input may be split.  The first
    /// offset should normally be `0` and the last should be `bytes.len()`.
    /// Adjacent offsets produce one chunk range.
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize>;

    /// Estimate the average bytes per record from a sample of the input.
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize;
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
