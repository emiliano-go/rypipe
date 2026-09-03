use crate::value::Value;
use smallvec::SmallVec;

/// Outcome of a predicate evaluation for one row.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum PredicateState {
    #[default]
    Undecided,
    Pass,
    Fail,
}

/// Heap-allocated row buffer for predicate-first deferred materialization.
/// Boxed so that unfiltered parses (the common case) don't carry 1 KB of
/// inline SmallVec in every `TableBuilder`.
pub(crate) struct RowBuffer {
    /// Per-row field buffer: `(slot_index, value)`. u32 is the column index
    /// from `field_index`, eliminating per-field String allocation.
    pub(crate) fields: SmallVec<[(u32, Value<'static>); 32]>,
    pub(crate) state: PredicateState,
    /// Bitmask of predicate column slots. Bit `i` is set when slot `i` appears
    /// in the filter predicate. For ≤64 columns this is a single u64; above
    /// 64 it grows like `row_dirty`.
    pub(crate) predicate_mask: SmallVec<[u64; 1]>,
    /// When true, the predicate has already resolved to Pass and remaining
    /// fields are pushed directly to columns (no buffering).
    pub(crate) direct: bool,
    /// Learned ordinal of the predicate column (0-based), set after the first
    /// row. When the predicate is late (> 4/5 of columns), buffering is a net
    /// loss; we switch to direct push + pop-on-reject instead.
    pub(crate) predicate_ordinal: Option<u32>,
    /// Whether the adaptive strategy has decided buffering is worthwhile.
    /// True by default; set to false on row 2 when predicate_ordinal is late.
    pub(crate) buffer_worthwhile: bool,
    /// Resolved predicate field names, cached once by `build_predicate_mask`.
    /// Used to check newly-created columns against the filter without cloning
    /// the filter tree on every non-predicate field.
    pub(crate) pred_names: SmallVec<[String; 4]>,
}

impl RowBuffer {
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self {
            fields: SmallVec::new(),
            state: PredicateState::Undecided,
            predicate_mask: SmallVec::new(),
            direct: false,
            predicate_ordinal: None,
            buffer_worthwhile: true,
            pred_names: SmallVec::new(),
        }
    }
}
