//! `rypipe-core`: format-agnostic columnar engine.
//!
//! The crate parses byte streams into typed Arrow record batches through a
//! small decoder API (`Splitter` + `RecordParser` + `ColumnarSink`).  It has
//! no format-specific logic and no Python/FFI dependencies.

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod arrow_export;
pub mod bounded;
pub mod columnar;
pub mod consumer;
pub mod decoder;
pub mod engine;
pub mod error;
pub mod input;
pub mod merge;
pub mod block_masks;
pub mod parallel;
pub mod parallel_stream;
pub mod pipeline;
pub mod plan;
pub mod schema;
pub mod streaming;
pub mod value;

pub use arrow_export::apply_compare_filter;
pub use bounded::MemoryBudget;
pub use consumer::{BatchConsumer, CollectingConsumer, DiscardingConsumer};
pub use decoder::{ColumnarSink, RecordParser, Splitter};
#[cfg(feature = "profiling")]
pub use engine::RESOLVE_AND_PUT_COUNT;
pub use engine::{LocateOnly, TableBuilder};
pub use error::{Error, Result};
pub use schema::{FrozenSchema, DiscoveryOpts, UnknownFieldPolicy};
pub use input::InputBuffer;
pub use merge::engines_to_record_batches;
pub use parallel_stream::{discover_schema_for_bytes, discovery_profile, reset_discovery_profile, ParallelStreamingBatchIterator, ParallelStreamingExecutor, ParallelStreamOpts};
pub use pipeline::Pipeline;
pub use plan::{CompareOp, ExecutionPlan, FieldType, FilterPredicate};
pub use streaming::StreamingBatchIterator;
pub use value::Value;
