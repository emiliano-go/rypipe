//! Auto engine selection: heuristic for picking the best execution mode.

/// The execution mode selected by the auto heuristic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineMode {
    /// Single-threaded, full table in RAM.
    Columnar,
    /// Multi-core parallel, full table in RAM.
    Parallel,
    /// Single-threaded streaming, bounded memory.
    Stream,
    /// Multi-core parallel streaming, bounded memory.
    ParallelStreaming,
}

impl EngineMode {
    /// Return the string representation used by Python adapters.
    pub fn as_str(&self) -> &'static str {
        match self {
            EngineMode::Columnar => "columnar",
            EngineMode::Parallel => "parallel",
            EngineMode::Stream => "stream",
            EngineMode::ParallelStreaming => "parallel_streaming",
        }
    }
}

/// Configuration for the auto engine selection heuristic.
pub struct AutoConfig {
    /// Size of the input file in bytes.
    pub file_size: u64,
    /// Memory budget (e.g. 64 MiB). `None` = no budget (full table).
    pub memory: Option<u64>,
    /// Number of threads for parallel processing. `None` = 1 (single-threaded).
    pub threads: Option<usize>,
    /// Explicit schema (column names). `Some` = schema provided, `None` = discover.
    pub schema: Option<Vec<String>>,
    /// Whether the adapter has parallel support.
    pub has_parallel: bool,
    /// Whether the adapter has columnar support.
    pub has_columnar: bool,
}

/// Resolve the best engine mode based on configuration.
///
/// The heuristic considers:
/// - `memory`: if provided, user wants streaming
/// - `threads`: if > 1, prefer parallel modes
/// - `schema`: if provided, streaming is 11% faster (no discovery overhead)
/// - `file_size`: small files use columnar, large files use parallel
pub fn resolve_engine(config: &AutoConfig) -> EngineMode {
    let threads = config.threads.unwrap_or(1);

    // 1. User explicitly requested streaming via memory=
    if config.memory.is_some() {
        if threads > 1 {
            return EngineMode::ParallelStreaming;
        }
        return EngineMode::Stream;
    }

    // 2. User explicitly requested parallel via threads=
    if threads > 1 {
        // For parallel streaming, we need a memory budget.
        // Without memory=, prefer parallel (full table) if it fits.
        // If we can't determine available memory, use parallel streaming
        // as the safer choice for large files.
        if config.file_size >= 100 * 1024 * 1024 {
            // Large file: prefer parallel streaming for bounded memory
            return EngineMode::ParallelStreaming;
        }
        return EngineMode::Parallel;
    }

    // 3. Schema provided: prefer streaming for 11% boost on large files
    if config.schema.is_some() && config.file_size >= 100 * 1024 * 1024 {
        return EngineMode::Stream;
    }

    // 4. Default: file size based
    if config.file_size < 8 * 1024 * 1024 {
        // Small file: prefer columnar (no chunk overhead)
        if config.has_columnar {
            return EngineMode::Columnar;
        }
        return EngineMode::Stream;
    }

    // Large file: prefer parallel if available
    if config.has_parallel {
        return EngineMode::Parallel;
    }

    // Fallback
    EngineMode::Stream
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_only() {
        let config = AutoConfig {
            file_size: 1_000_000_000,
            memory: Some(64 * 1024 * 1024),
            threads: None,
            schema: None,
            has_parallel: true,
            has_columnar: true,
        };
        assert_eq!(resolve_engine(&config), EngineMode::Stream);
    }

    #[test]
    fn test_memory_and_threads() {
        let config = AutoConfig {
            file_size: 1_000_000_000,
            memory: Some(64 * 1024 * 1024),
            threads: Some(16),
            schema: None,
            has_parallel: true,
            has_columnar: true,
        };
        assert_eq!(resolve_engine(&config), EngineMode::ParallelStreaming);
    }

    #[test]
    fn test_threads_only_large_file() {
        let config = AutoConfig {
            file_size: 1_000_000_000,
            memory: None,
            threads: Some(16),
            schema: None,
            has_parallel: true,
            has_columnar: true,
        };
        assert_eq!(resolve_engine(&config), EngineMode::ParallelStreaming);
    }

    #[test]
    fn test_threads_only_small_file() {
        let config = AutoConfig {
            file_size: 50 * 1024 * 1024,
            memory: None,
            threads: Some(16),
            schema: None,
            has_parallel: true,
            has_columnar: true,
        };
        assert_eq!(resolve_engine(&config), EngineMode::Parallel);
    }

    #[test]
    fn test_schema_large_file() {
        let config = AutoConfig {
            file_size: 500 * 1024 * 1024,
            memory: None,
            threads: None,
            schema: Some(vec!["a".into(), "b".into()]),
            has_parallel: true,
            has_columnar: true,
        };
        assert_eq!(resolve_engine(&config), EngineMode::Stream);
    }

    #[test]
    fn test_small_file_columnar() {
        let config = AutoConfig {
            file_size: 5 * 1024 * 1024,
            memory: None,
            threads: None,
            schema: None,
            has_parallel: true,
            has_columnar: true,
        };
        assert_eq!(resolve_engine(&config), EngineMode::Columnar);
    }

    #[test]
    fn test_small_file_no_columnar() {
        let config = AutoConfig {
            file_size: 5 * 1024 * 1024,
            memory: None,
            threads: None,
            schema: None,
            has_parallel: true,
            has_columnar: false,
        };
        assert_eq!(resolve_engine(&config), EngineMode::Stream);
    }

    #[test]
    fn test_medium_file_parallel() {
        let config = AutoConfig {
            file_size: 100 * 1024 * 1024,
            memory: None,
            threads: None,
            schema: None,
            has_parallel: true,
            has_columnar: true,
        };
        assert_eq!(resolve_engine(&config), EngineMode::Parallel);
    }

    #[test]
    fn test_large_file_no_parallel() {
        let config = AutoConfig {
            file_size: 100 * 1024 * 1024,
            memory: None,
            threads: None,
            schema: None,
            has_parallel: false,
            has_columnar: true,
        };
        assert_eq!(resolve_engine(&config), EngineMode::Stream);
    }
}
