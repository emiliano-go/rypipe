//! Non-fatal parse diagnostics.
//!
//! Batch parsers return `Err` to abort a chunk, but streaming formats need to
//! report recoverable damage (a malformed record, an unterminated section, an
//! item dropped at EOF) without failing the whole feed. Adapters receive a
//! `ParseDiagnostics` and report structured events; the caller decides what to
//! log or count.

/// Sink for parser warnings and counters.
///
/// Implementations are shared across parse threads in the parallel engines, so
/// they must be `Send + Sync` and cheap; a no-op default keeps the common case
/// free.
pub trait ParseDiagnostics: Send + Sync {
    /// A recoverable event the caller should surface. `code` is a stable,
    /// adapter-defined tag (e.g. `"malformed_item"`); `message` is a
    /// human-readable description.
    fn warning(&self, _code: &str, _message: &str) {}

    /// Increment a named counter (e.g. `"discarded_items"`) by `delta`.
    fn counter(&self, _name: &str, _delta: usize) {}
}

/// Diagnostics sink that drops everything. Used when the caller does not care
/// about per-record damage.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopDiagnostics;

impl ParseDiagnostics for NoopDiagnostics {}
