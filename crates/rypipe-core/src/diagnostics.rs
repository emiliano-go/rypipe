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

    /// Whether any event is pending. Streaming drivers poll this cheaply
    /// instead of draining (and allocating) per record.
    fn has_events(&self) -> bool {
        false
    }
}

/// Diagnostics sink that drops everything. Used when the caller does not care
/// about per-record damage.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopDiagnostics;

impl ParseDiagnostics for NoopDiagnostics {}

/// A diagnostics sink that collects warnings and counters for the caller to
/// drain. Shareable across parse threads.
#[derive(Default)]
pub struct CollectingDiagnostics {
    events: std::sync::Mutex<Vec<(String, String)>>,
    counters: std::sync::Mutex<std::collections::HashMap<String, usize>>,
}

impl CollectingDiagnostics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Take the warnings accumulated since the last call.
    pub fn take_events(&self) -> Vec<(String, String)> {
        std::mem::take(&mut *self.events.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// Snapshot the counters.
    pub fn counters(&self) -> std::collections::HashMap<String, usize> {
        self.counters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

impl ParseDiagnostics for CollectingDiagnostics {
    fn warning(&self, code: &str, message: &str) {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((code.to_string(), message.to_string()));
    }

    fn counter(&self, name: &str, delta: usize) {
        *self
            .counters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(name.to_string())
            .or_insert(0) += delta;
    }

    fn has_events(&self) -> bool {
        !self
            .events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collecting_diagnostics_reports_and_drains() {
        let d = CollectingDiagnostics::new();
        assert!(!d.has_events());
        d.warning("bad_item", "dropped");
        d.counter("discarded", 2);
        assert!(d.has_events());
        assert_eq!(d.take_events().len(), 1);
        assert!(!d.has_events());
        assert_eq!(d.counters().get("discarded"), Some(&2));
    }
}
