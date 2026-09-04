# Rust API Reference { #rust-api }

This page is a reference for the **rypipe-core** Rust API. For a tutorial,
see [Writing Adapters](../writing-adapters/index.md).

## Crate structure { #crate-structure }

| Crate | Purpose |
|-------|---------|
| `rypipe-core` | Engine, traits, pipeline, Arrow export |
| `rypipe-python` | PyO3 bindings and helpers |

## Core traits { #core-traits }

### Splitter { #splitter }

Finds row boundaries in the byte stream.

```rust
pub trait Splitter: Send + Sync {
    /// Return the byte position of the next record start after `from`,
    /// or None at end of input.
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize>;

    /// Estimate average bytes per row from a sample.
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize;

    /// Optional: regions where split points must be rejected.
    fn skip_regions(&self) -> Option<&dyn SkipRegionFinder> {
        None  // default: no skip regions
    }
}
```

### RecordParser { #recordparser }

Extracts field values from each row.

```rust
pub trait RecordParser: Send + Sync {
    /// Validate that bytes are well-formed. Called once per chunk.
    fn validate(&self, bytes: &[u8]) -> Result<()>;

    /// Parse a chunk into field/value events via the sink.
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()>;

    /// Monomorphized version for inlining (default: delegates to parse_chunk).
    fn parse_chunk_generic<S: ColumnarSink>(&self, bytes: &[u8], sink: &mut S) -> Result<()>
    where
        Self: Sized;
}
```

### ColumnarSink { #columnarsink }

Accumulates values into Arrow columns. The engine provides `TableBuilder`
as the production implementation.

```rust
pub trait ColumnarSink {
    // Required
    fn begin_row(&mut self);
    fn put_field(&mut self, name: &str, value: Value<'_>);
    fn end_row(&mut self);
    fn finish(&mut self) -> Result<RecordBatch>;

    // With defaults
    fn wants(&self, _name: &str) -> bool { true }
    fn resolve<'a>(&'a self, name: &'a str) -> Option<&'a str> { Some(name) }
    fn row_rejected(&self) -> bool { false }
    fn row_satisfied(&self) -> bool { false }
    // ... more methods with defaults
}
```

### SkipRegionFinder { #skipregionfinder }

Defines byte ranges that must not be split on.

```rust
pub trait SkipRegionFinder: Send + Sync {
    fn openers(&self) -> &[&'static [u8]];
    fn closer_for(&self, opener: &[u8]) -> &'static [u8];
    fn window(&self) -> usize { 64 * 1024 }
}
```

## Value enum { #value }

Represents a parsed field value.

```rust
pub enum Value<'a> {
    Str(Cow<'a, str>),      // UTF-8 string (borrowed or owned)
    Int64(i64),              // 64-bit signed integer
    Float64(f64),            // 64-bit float
    Bool(bool),              // Boolean
    Date32(i32),             // Days since Unix epoch
    Timestamp(i64),          // Raw integer (unit from field_types)
    Null,                    // Explicit null
}
```

## ExecutionPlan { #executionplan }

Configuration for the parse loop.

```rust
pub struct ExecutionPlan {
    pub field_map: HashMap<String, String>,       // rename columns
    pub drop_fields: HashSet<String>,             // columns to skip
    pub field_types: HashMap<String, FieldType>,  // type overrides
    pub dictionary_columns: HashSet<String>,      // dict-encode columns
    pub filter: Option<FilterPredicate>,          // row filter
    pub schema_order: Vec<String>,                // output column order
    pub auto_dict: bool,                          // auto-dictionary
    pub dict_threshold: Option<f64>,              // auto-dict threshold
    pub dict_max_size: Option<usize>,             // auto-dict max size
}
```

**Builder methods:**

```rust
ExecutionPlan::new()
    .rename("old", "new")
    .drop("field")
    .drop_many(["a", "b"])
    .type_as("col", FieldType::Float64)
    .dictionary("col")
    .filter_eq("status", "active")
    .filter_compare("price", CompareOp::Gt, "cost")
    .schema_order(["a", "b", "c"])
    .with_auto_dict(true)
```

## FieldType { #fieldtype }

```rust
pub enum FieldType {
    String,
    Int64,
    Float64,
    Boolean,
    Dictionary,
    Date32,
    Timestamp(TimeUnit),
}

impl FieldType {
    pub fn from_str(s: &str) -> Option<Self>;
    // Recognized: "string", "int64", "float64", "bool", "boolean",
    // "dictionary", "date32", "timestamp", "timestamp[s]", etc.
}
```

## CompareOp { #compareop }

```rust
pub enum CompareOp { Gt, Lt, Ge, Le, Eq, Ne }

impl CompareOp {
    pub fn from_str(s: &str) -> Option<Self>;
    // Recognized: ">"|"gt", "<"|"lt", ">="|"ge", "<="|"le", "=="|"eq", "!="|"ne"
}
```

## FilterPredicate { #filterpredicate }

```rust
pub enum FilterPredicate {
    Equal { field: String, value: String },
    NotEqual { field: String, value: String },
    Compare { field_a: String, op: CompareOp, field_b: String },
    And(Box<FilterPredicate>, Box<FilterPredicate>),
    Or(Box<FilterPredicate>, Box<FilterPredicate>),
    Not(Box<FilterPredicate>),
}

impl FilterPredicate {
    pub fn all(a: Self, b: Self) -> Self;
    pub fn any(a: Self, b: Self) -> Self;
    pub fn not(inner: Self) -> Self;
}
```

## Pipeline { #pipeline }

Orchestrates splitting, parsing, and export.

```rust
pub struct Pipeline<S, P> {
    splitter: S,
    parser: P,
    plan: Arc<ExecutionPlan>,
}

impl<S, P> Pipeline<S, P>
where
    S: Splitter + Clone,
    P: RecordParser + Clone,
{
    pub fn new(splitter: S, parser: P) -> Self;
    pub fn with_plan(self, plan: ExecutionPlan) -> Self;

    // Single-threaded
    pub fn read_bytes(&self, bytes: &[u8]) -> Result<RecordBatch>;
    pub fn read_path(&self, path: impl AsRef<Path>, use_mmap: bool, prefault: bool) -> Result<RecordBatch>;

    // Parallel
    pub fn read_bytes_par(&self, bytes: &[u8], num_chunks: usize) -> Result<Vec<RecordBatch>>;
    pub fn read_path_par(&self, path: impl AsRef<Path>, num_chunks: usize, use_mmap: bool, prefault: bool) -> Result<Vec<RecordBatch>>;

    // Streaming
    pub fn read_bytes_stream(&self, bytes: &[u8], budget: MemoryBudget) -> Result<Vec<RecordBatch>>;
    pub fn read_path_stream(&self, path: impl AsRef<Path>, budget: MemoryBudget, prefault: bool) -> Result<Vec<RecordBatch>>;
}
```

## FrozenSchema { #frozenschema}

Resolved schema for a parse run.

```rust
pub struct FrozenSchema { /* fields private */ }

impl FrozenSchema {
    pub fn from_plan(names: &[&str], plan: &ExecutionPlan) -> Self;
    pub fn from_discovered(names_in_order: &[String], plan: &ExecutionPlan) -> Self;
    pub fn num_columns(&self) -> usize;
    pub fn column_names(&self) -> &[Arc<str>];
    pub fn column_types(&self) -> &[FieldType];
    pub fn resolve(&self, raw_name: &str) -> Option<u32>;
}
```

## Python bindings { #python-bindings }

### execution_plan_from_kwargs { #execution-plan-from-kwars }

```rust
pub fn execution_plan_from_kwargs(
    field_mapping: Option<HashMap<String, String>>,
    drop_fields: Option<Vec<String>>,
    filter: Option<&Bound<'_, PyAny>>,
    field_types: Option<HashMap<String, String>>,
    dictionary_columns: Option<Vec<String>>,
    schema: Option<Vec<String>>,
    auto_dict: bool,
    auto_dict_threshold: Option<f64>,
    auto_dict_max_size: Option<usize>,
) -> PyResult<ExecutionPlan>;
```

### Export functions { #export-functions }

```rust
pub fn record_batches_to_pyarrow_table(
    py: Python<'_>,
    batches: &[RecordBatch],
) -> PyResult<PyObject>;

pub fn record_batch_to_pyarrow(
    py: Python<'_>,
    batch: &RecordBatch,
) -> PyResult<PyObject>;
```

### Exceptions { #exceptions }

| Exception | Parent | Meaning |
|-----------|--------|---------|
| `ParseError` | `PyException` | File could not be parsed. |
| `XmlError` | `ParseError` | XML-specific parse error. |
| `PlanError` | `PyException` | Invalid plan kwargs. |
| `MergeError` | `PyException` | Schema mismatch between chunks. |
