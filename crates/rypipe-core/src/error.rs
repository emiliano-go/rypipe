use thiserror::Error;

/// Crate-wide error type.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid UTF-8 in input bytes.
    #[error("invalid UTF-8: {0}")]
    Utf8(#[from] simdutf8::basic::Utf8Error),

    /// I/O failure (read, seek, mmap, etc.).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Invalid or inconsistent execution plan.
    #[error("plan error: {0}")]
    Plan(String),

    /// Merge conflict between chunks (e.g. column type mismatch).
    #[error("merge error: {0}")]
    Merge(String),

    /// Arrow array/batch construction failure.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

/// Shorthand result type used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;
