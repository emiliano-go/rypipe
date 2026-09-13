//! `rypipe-core`: format-agnostic ingestion framework.
//!
//! The crate parses byte streams into typed Arrow record batches through a
//! small decoder API (`Splitter` + `RecordParser` + `ColumnarSink`).  It has
//! no format-specific logic and no Python/FFI dependencies.

#![allow(unexpected_cfgs)]
#![allow(unsafe_code)]

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "alloc-stats")]
pub mod alloc_stats;
pub mod arrow_export;
pub mod auto;
#[cfg(feature = "bench")]
pub mod bench;
pub mod block_masks;
pub mod bounded;
pub mod columnar;
pub mod consumer;
pub mod decoder;
pub mod dict;
pub mod engine;
pub mod error;
pub mod input;
pub mod merge;
pub mod observer;
pub mod parallel;
pub mod parallel_stream;
pub mod pipeline;
pub mod plan;
pub mod scan;
pub mod schema;
pub mod streaming;
pub mod value;

pub use arrow_export::apply_compare_filter;
pub use auto::{resolve_engine, AutoConfig, EngineMode};
pub use bounded::{MemoryBudget, MAX_SPLIT_CHUNKS};
pub use consumer::{BatchConsumer, CollectingConsumer, DiscardingConsumer};
pub use decoder::{split_points_to_ranges, ColumnarSink, RecordParser, Splitter};
pub use engine::{LocateOnly, TableBuilder};
#[cfg(feature = "profile")]
pub use engine::{
    IS_PRED_FALSE, IS_PRED_TRUE, PREDICATE_EVALUATIONS, PREDICATE_FAILS, PREDICATE_UNDECIDED,
    RESOLVE_AND_PUT_COUNT,
};
pub use error::{Error, Result};
pub use input::InputBuffer;
pub use merge::engines_to_record_batches;
pub use observer::RowObserver;
pub use parallel_stream::{
    discover_schema_for_bytes, discovery_profile, reset_discovery_profile, ParallelStreamOpts,
    ParallelStreamingBatchIterator, ParallelStreamingExecutor,
};
pub use pipeline::Pipeline;
pub use plan::{CompareOp, ExecutionPlan, FieldType, FilterPredicate, RegexSpec};
pub use schema::{DiscoveryOpts, FrozenSchema, UnknownFieldPolicy};
pub use streaming::StreamingBatchIterator;
pub use value::Value;
