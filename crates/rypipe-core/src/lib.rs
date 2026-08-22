//! `rypipe-core` — format-agnostic columnar engine.
//!
//! The crate parses byte streams into typed Arrow record batches through a
//! small decoder API (`Splitter` + `RecordParser` + `ColumnarSink`).  It has
//! no format-specific logic and no Python/FFI dependencies.

pub mod arrow_export;
pub mod bounded;
pub mod columnar;
pub mod decoder;
pub mod engine;
pub mod error;
pub mod input;
pub mod merge;
pub mod parallel;
pub mod plan;
pub mod value;

pub use arrow_export::apply_compare_filter;
pub use decoder::{ColumnarSink, RecordParser, Splitter};
pub use engine::TableBuilder;
pub use error::{Error, Result};
pub use input::InputBuffer;
pub use merge::engines_to_record_batches;
pub use plan::{CompareOp, ExecutionPlan, FieldType, FilterPredicate};
pub use value::Value;
