//! Per-row observer hooks fired by `TableBuilder` during parsing.
//!
//! Attach via [`crate::ExecutionPlan::observer`]. Every executor (serial,
//! parallel, bounded, streaming) builds on `TableBuilder`, so hooks fire on
//! all of them, called from whichever thread parses the chunk: implementors
//! must be `Send + Sync` and cheap, since hooks run on the parse hot path.
//!
//! Rejected rows are never observed field-by-field: when a row filter
//! buffers values before the predicate resolves, `on_put_field` fires only
//! when the buffered row is drained (accepted). One exception: when the
//! predicate column arrives late in the row, values are pushed directly and
//! popped on reject, so `on_put_field` may fire for a row that is later
//! rejected; `on_row_rejected` still reports the rejection.

use crate::Value;

/// Hook points invoked by `TableBuilder` while building a batch.
///
/// All methods default to no-ops; implement only the ones you need.
pub trait RowObserver: Send + Sync {
    /// A new row started. `row_index` is the number of rows accepted so far.
    fn on_begin_row(&self, _row_index: usize) {}

    /// A field value landed in a column of an accepted (or not-yet-rejected)
    /// row. `slot` is the column index in first-appearance order.
    fn on_put_field(
        &self,
        _row_index: usize,
        _resolved_name: &str,
        _slot: usize,
        _value: &Value<'_>,
    ) {
    }

    /// The row passed the filter (or no filter) and was committed.
    fn on_row_accepted(&self, _row_index: usize) {}

    /// The row was rejected by the filter and discarded.
    fn on_row_rejected(&self, _row_index: usize) {}

    /// A batch/chunk finished. Counts cover the rows since the previous
    /// finished batch (in streaming mode each emitted batch reports its own
    /// delta).
    fn on_chunk_finished(&self, _total: usize, _accepted: usize, _rejected: usize) {}
}
