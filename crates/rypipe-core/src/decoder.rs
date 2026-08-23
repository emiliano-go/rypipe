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

    /// Parse one chunk and feed all row events into `sink`.
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()>;
}

/// Sink for decoder events.  The decoder calls `begin_row` / `put_field` /
/// `end_row` for each record.
pub trait ColumnarSink {
    fn begin_row(&mut self);
    fn put_field(&mut self, name: &str, value: Value<'_>);
    fn end_row(&mut self);

    /// Return `false` to signal that the engine will drop this field.
    fn wants(&self, _name: &str) -> bool {
        true
    }

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
