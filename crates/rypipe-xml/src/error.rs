//! Adapter-specific error type and conversions to `rypipe_core::Error`.

use thiserror::Error;

/// Errors that can occur while parsing Crystal Reports XML.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid UTF-8 in the input byte slice.
    #[error("invalid UTF-8: {0}")]
    Utf8(#[from] simdutf8::basic::Utf8Error),

    /// Malformed XML or an unsupported construct.
    #[error("XML parse error at byte {0}: {1}")]
    XmlParse(usize, String),
}

impl From<Error> for rypipe_core::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Utf8(e) => rypipe_core::Error::Utf8(e),
            Error::XmlParse(pos, msg) => {
                rypipe_core::Error::Plan(format!("XML parse error at byte {pos}: {msg}"))
            }
        }
    }
}
