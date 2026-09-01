mod locate;
mod predicate;
pub(crate) mod table_builder;

pub use locate::LocateOnly;
pub(crate) use predicate::RowBuffer;
pub use predicate::PredicateState;
pub use table_builder::TableBuilder;

// Re-export for backward compatibility — existing code does `use crate::engine::*`.
pub use table_builder::*;
